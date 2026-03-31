"""
Incremental state update: scope watermarks + per-run cohort rows (WORKFLOW_STATUS=detected).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from cognite.client import data_modeling as dm
from cognite.client.data_classes import Row

from ..cdf_fn_common.incremental_scope import (
    CHANGE_KIND_COLUMN,
    CHANGE_KIND_ADD,
    CHANGE_KIND_UPDATE,
    EXTERNAL_ID_COLUMN,
    HIGH_WATERMARK_MS_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    RAW_ROW_KEY_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RECORD_KIND_WATERMARK,
    RUN_ID_COLUMN,
    SCOPE_KEY_COLUMN,
    WORKFLOW_STATUS_CHECKPOINT,
    WORKFLOW_STATUS_COLUMN,
    WORKFLOW_STATUS_DETECTED,
    WORKFLOW_STATUS_UPDATED_AT_COLUMN,
    cohort_row_key,
    list_all_instances,
    load_prior_node_ids_for_scope,
    node_instance_id_str,
    node_last_updated_time_ms,
    read_watermark_high_ms,
    scope_key_from_view_dict,
    scope_watermark_row_key,
)
from ..cdf_fn_common.raw_upload import create_raw_upload_queue
from ..fn_dm_key_extraction.common.cdf_utils import create_table_if_not_exists

RAW_COL_UPDATED_AT = "UPDATED_AT"


def incremental_state_update(
    client: Any,
    logger: Any,
    data: Dict[str, Any],
    cdf_config: Any,
) -> None:
    """
    Write scope watermarks and cohort rows with WORKFLOW_STATUS=detected.

    Expects ``cdf_config.parameters.incremental_change_processing`` True and
    standard key-extraction ``raw_db`` / ``raw_table_key``.
    """
    params = getattr(cdf_config, "parameters", None)
    if not bool(getattr(params, "incremental_change_processing", False)):
        raise ValueError("incremental_change_processing must be true for this function")

    raw_db = str(getattr(params, "raw_db", "") or "")
    raw_table_key = str(getattr(params, "raw_table_key", "") or "")
    if not raw_db or not raw_table_key:
        raise ValueError("raw_db and raw_table_key are required")

    process_all = bool(data.get("process_all", False))

    source_views = getattr(getattr(cdf_config, "data", None), "source_views", None) or []
    if not source_views:
        sv = getattr(getattr(cdf_config, "data", None), "source_view", None)
        source_views = [sv] if sv is not None else []
    if not source_views:
        raise ValueError("source_views (or source_view) is required")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    create_table_if_not_exists(client, raw_db, raw_table_key, logger)
    raw_uploader = create_raw_upload_queue(client)

    cohort_rows = 0
    batch_updated_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    for entity_view_config in source_views:
        view_id = entity_view_config.as_view_id()
        view_dict = (
            entity_view_config.model_dump()
            if hasattr(entity_view_config, "model_dump")
            else entity_view_config.dict()
        )
        scope_key = scope_key_from_view_dict(view_dict)
        wm_key = scope_watermark_row_key(scope_key)

        if process_all:
            try:
                client.raw.rows.delete(raw_db, raw_table_key, wm_key)
            except Exception as ex:
                logger.warning(f"Could not delete watermark row {wm_key!r}: {ex}")

        high_wm = None if process_all else read_watermark_high_ms(
            client, raw_db, raw_table_key, wm_key
        )

        base_filter = entity_view_config.build_filter()
        if high_wm is not None and not process_all:
            rng = dm.filters.Range(
                ("node", "lastUpdatedTime"),
                gt=int(high_wm),
            )
            filt: dm.filters.Filter = dm.filters.And(base_filter, rng)
        else:
            filt = base_filter

        prior_ids = load_prior_node_ids_for_scope(
            client, raw_db, raw_table_key, scope_key
        )

        max_ts: Optional[int] = None
        space = getattr(entity_view_config, "instance_space", None)

        for instance in list_all_instances(
            client,
            instance_type="node",
            space=space,
            sources=[view_id],
            filter=filt,
            limit_per_page=min(1000, getattr(entity_view_config, "batch_size", 1000) or 1000),
        ):
            nid = node_instance_id_str(instance)
            if not nid:
                logger.warning("Skipping instance without node id")
                continue
            ext_id = getattr(instance, "external_id", None)
            change_kind = CHANGE_KIND_UPDATE if nid in prior_ids else CHANGE_KIND_ADD
            ts = node_last_updated_time_ms(instance)
            if ts is not None:
                max_ts = ts if max_ts is None else max(max_ts, ts)

            rk = cohort_row_key(run_id, nid, scope_key)
            columns: Dict[str, Any] = {
                RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
                WORKFLOW_STATUS_COLUMN: WORKFLOW_STATUS_DETECTED,
                RUN_ID_COLUMN: run_id,
                CHANGE_KIND_COLUMN: change_kind,
                NODE_INSTANCE_ID_COLUMN: nid,
                EXTERNAL_ID_COLUMN: str(ext_id) if ext_id is not None else "",
                SCOPE_KEY_COLUMN: scope_key,
                RAW_ROW_KEY_COLUMN: rk,
                "view_space": view_id.space,
                "view_external_id": view_id.external_id,
                "view_version": view_id.version,
                "instance_space": str(space or getattr(instance, "space", "") or ""),
                "entity_type": (
                    entity_view_config.entity_type.value
                    if hasattr(entity_view_config.entity_type, "value")
                    else str(entity_view_config.entity_type)
                ),
                RAW_COL_UPDATED_AT: batch_updated_at,
                WORKFLOW_STATUS_UPDATED_AT_COLUMN: batch_updated_at,
            }
            raw_uploader.add_to_upload_queue(
                database=raw_db,
                table=raw_table_key,
                raw_row=Row(key=rk, columns=columns),
            )
            cohort_rows += 1

        # Upsert watermark for this scope.
        # If this run found no changed instances, preserve existing high watermark
        # instead of resetting to 0 (which would re-select the full scope next run).
        next_high_wm = max_ts if max_ts is not None else high_wm
        wm_columns: Dict[str, Any] = {
            RECORD_KIND_COLUMN: RECORD_KIND_WATERMARK,
            WORKFLOW_STATUS_COLUMN: WORKFLOW_STATUS_CHECKPOINT,
            SCOPE_KEY_COLUMN: scope_key,
            RAW_COL_UPDATED_AT: batch_updated_at,
            HIGH_WATERMARK_MS_COLUMN: int(next_high_wm if next_high_wm is not None else 0),
        }
        raw_uploader.add_to_upload_queue(
            database=raw_db,
            table=raw_table_key,
            raw_row=Row(key=wm_key, columns=wm_columns),
        )

    raw_uploader.upload()

    data["run_id"] = run_id
    data["status"] = "success"
    data["message"] = json.dumps(
        {
            "cohort_rows_written": cohort_rows,
            "raw_db": raw_db,
            "raw_table_key": raw_table_key,
        }
    )
    logger.info(
        f"Incremental state update wrote {cohort_rows} cohort row(s); run_id={run_id}"
    )
