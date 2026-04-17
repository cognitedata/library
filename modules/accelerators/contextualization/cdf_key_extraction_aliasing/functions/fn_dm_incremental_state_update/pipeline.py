"""
Incremental state update: scope watermarks + per-run cohort rows (WORKFLOW_STATUS=detected).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from cognite.client import data_modeling as dm
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError

from cdf_fn_common.extraction_input_hash import (
    build_field_map_for_hash,
    extraction_inputs_hash,
    iter_wanted_fields,
    resolve_key_discovery_hash_field_paths,
    rules_fingerprint,
)
from cdf_fn_common.key_discovery_state_fdm import (
    is_key_discovery_cdm_deployed,
    key_discovery_view_ids_from_parameters,
    load_key_discovery_scope_state_maps,
    read_key_discovery_high_watermark_ms,
    upsert_scope_checkpoint,
)
from cdf_fn_common.incremental_scope import (
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
    load_latest_hash_by_node_for_scope,
    load_prior_node_ids_for_scope,
    node_instance_id_str,
    node_last_updated_time_ms,
    read_watermark_high_ms,
    scope_key_from_view_dict,
    scope_watermark_row_key,
)
from cdf_fn_common.raw_upload import create_raw_upload_queue
from cdf_fn_common.run_all import resolve_run_all
from cdf_fn_common.cdf_utils import create_table_if_not_exists

RAW_COL_UPDATED_AT = "UPDATED_AT"


def _cfg_get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _as_view_id(entity_view_config: Any) -> dm.ViewId:
    if hasattr(entity_view_config, "as_view_id"):
        return entity_view_config.as_view_id()
    return dm.ViewId(
        space=str(_cfg_get(entity_view_config, "view_space", "")),
        external_id=str(_cfg_get(entity_view_config, "view_external_id", "")),
        version=str(_cfg_get(entity_view_config, "view_version", "")),
    )


def _build_base_filter(entity_view_config: Any, view_id: dm.ViewId) -> dm.filters.Filter:
    from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.source_view_filter_build import (
        build_source_view_query_filter,
    )

    vd = _view_dict(entity_view_config)
    filters = vd.get("filters") or []
    return build_source_view_query_filter(view_id, filters)


def _view_dict(entity_view_config: Any) -> Dict[str, Any]:
    if hasattr(entity_view_config, "model_dump"):
        dump = getattr(entity_view_config, "model_dump")
        try:
            return dump(mode="python")
        except TypeError:
            return dump()
    if isinstance(entity_view_config, dict):
        return dict(entity_view_config)
    if hasattr(entity_view_config, "dict"):
        return entity_view_config.dict()
    return {}


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

    run_all = resolve_run_all(params, data)

    source_views = getattr(getattr(cdf_config, "data", None), "source_views", None) or []
    if not source_views:
        sv = getattr(getattr(cdf_config, "data", None), "source_view", None)
        source_views = [sv] if sv is not None else []
    if not source_views:
        raise ValueError("source_views (or source_view) is required")

    skip_unchanged_inputs = bool(
        getattr(params, "incremental_skip_unchanged_source_inputs", True)
    ) and not run_all
    extraction_rules = getattr(getattr(cdf_config, "data", None), "extraction_rules", None)
    rules_fp: Optional[str] = None
    if skip_unchanged_inputs:
        rules_fp = rules_fingerprint(extraction_rules)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    create_table_if_not_exists(client, raw_db, raw_table_key, logger)
    raw_uploader = create_raw_upload_queue(client)

    cohort_rows = 0
    cohort_rows_skipped_unchanged_hash = 0
    batch_updated_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    kd_space = str(getattr(params, "key_discovery_instance_space", None) or "").strip()
    use_key_discovery = bool(kd_space)
    workflow_scope_param = str(getattr(params, "workflow_scope", None) or "").strip()
    processing_view_id = None
    checkpoint_view_id = None
    if use_key_discovery:
        processing_view_id, checkpoint_view_id = key_discovery_view_ids_from_parameters(
            params
        )
        if not is_key_discovery_cdm_deployed(
            client, processing_view_id, checkpoint_view_id, logger=logger
        ):
            use_key_discovery = False
            processing_view_id = None
            checkpoint_view_id = None
    if use_key_discovery and not workflow_scope_param:
        raise ValueError(
            "workflow_scope must be set (e.g. via scope build) when key_discovery_instance_space is set"
        )

    for entity_view_config in source_views:
        view_id = _as_view_id(entity_view_config)
        view_dict = _view_dict(entity_view_config)
        scope_key = scope_key_from_view_dict(view_dict)
        wm_key = scope_watermark_row_key(scope_key)
        sv_fp = scope_key

        if run_all:
            try:
                client.raw.rows.delete(raw_db, raw_table_key, wm_key)
            except Exception as ex:
                logger.warning(f"Could not delete watermark row {wm_key!r}: {ex}")

        effective_kd = use_key_discovery
        high_wm: Optional[int] = None
        hash_by_node: Dict[str, str] = {}
        prior_ids: Set[str] = set()

        if effective_kd:
            assert checkpoint_view_id is not None and processing_view_id is not None
            try:
                high_wm = (
                    None
                    if run_all
                    else read_key_discovery_high_watermark_ms(
                        client,
                        checkpoint_view_id,
                        kd_space,
                        workflow_scope_param,
                        sv_fp,
                    )
                )
                hash_by_node, prior_ids_set = load_key_discovery_scope_state_maps(
                    client,
                    processing_view_id,
                    kd_space,
                    workflow_scope_param,
                    sv_fp,
                    limit_per_page=min(
                        1000,
                        int(_cfg_get(entity_view_config, "batch_size", 1000) or 1000),
                    ),
                    logger=logger,
                )
                prior_ids = prior_ids_set
            except (CogniteAPIError, CogniteNotFoundError) as ex:
                if hasattr(logger, "warning"):
                    logger.warning(
                        "Key Discovery FDM state read failed; using RAW watermark/hash for this view: %s",
                        ex,
                    )
                effective_kd = False

        if not effective_kd:
            high_wm = None if run_all else read_watermark_high_ms(
                client, raw_db, raw_table_key, wm_key
            )
            prior_ids = load_prior_node_ids_for_scope(
                client, raw_db, raw_table_key, scope_key
            )
            hash_by_node = {}

        base_filter = _build_base_filter(entity_view_config, view_id)
        if high_wm is not None and not run_all:
            # Some backends can include the boundary value for `gt` on lastUpdatedTime.
            # Add +1ms to make the bound unambiguously exclusive.
            rng = dm.filters.Range(
                ("node", "lastUpdatedTime"),
                gt=int(high_wm) + 1,
            )
            filt: dm.filters.Filter = dm.filters.And(base_filter, rng)
        else:
            filt = base_filter

        if not isinstance(hash_by_node, dict):
            hash_by_node = {}
        wanted_fields: Optional[List[Tuple[str, bool, List[str]]]] = None
        if skip_unchanged_inputs and rules_fp is not None:
            if not effective_kd:
                hash_by_node = load_latest_hash_by_node_for_scope(
                    client,
                    raw_db,
                    raw_table_key,
                    scope_key,
                    chunk_size=int(
                        getattr(params, "raw_skip_scan_chunk_size", 2500) or 2500
                    ),
                )
            wanted_fields = (
                resolve_key_discovery_hash_field_paths(
                    extraction_rules, entity_view_config
                )
                if effective_kd
                else iter_wanted_fields(extraction_rules, entity_view_config)
            )

        max_ts: Optional[int] = None
        space = _cfg_get(entity_view_config, "instance_space", None)

        _lpp = min(1000, int(_cfg_get(entity_view_config, "batch_size", 1000) or 1000))
        _ctx = (
            f"scope_key={scope_key!r} "
            f"view={view_id.space}/{view_id.external_id}/{view_id.version} "
            f"limit_per_page={_lpp}"
        )
        for instance in list_all_instances(
            client,
            instance_type="node",
            space=space,
            sources=[view_id],
            filter=filt,
            limit_per_page=_lpp,
            logger=logger,
            progress_context=_ctx,
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

            if (
                skip_unchanged_inputs
                and rules_fp is not None
                and wanted_fields is not None
            ):
                inst_dump = instance.dump() if hasattr(instance, "dump") else {}
                props = (
                    inst_dump.get("properties", {})
                    if isinstance(inst_dump, dict)
                    else {}
                )
                entity_props = (
                    props.get(view_id.space, {}).get(
                        f"{view_id.external_id}/{view_id.version}", {}
                    )
                    if isinstance(props, dict)
                    else {}
                )
                field_map = build_field_map_for_hash(
                    entity_props, wanted_fields, logger=logger
                )
                # With no fields to hash, every instance would share the same digest; never skip.
                if field_map:
                    if effective_kd:
                        new_hash = extraction_inputs_hash(
                            scope_key,
                            rules_fp,
                            field_map,
                            workflow_scope=workflow_scope_param,
                            source_view_fingerprint=sv_fp,
                        )
                    else:
                        new_hash = extraction_inputs_hash(scope_key, rules_fp, field_map)
                    if new_hash == hash_by_node.get(nid):
                        cohort_rows_skipped_unchanged_hash += 1
                        continue

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
                    _cfg_get(entity_view_config, "entity_type").value
                    if hasattr(_cfg_get(entity_view_config, "entity_type"), "value")
                    else str(_cfg_get(entity_view_config, "entity_type") or "")
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

        # Upsert watermark for this scope (RAW legacy, or Key Discovery checkpoint).
        # If this run found no changed instances, preserve existing high watermark
        # instead of resetting to 0 (which would re-select the full scope next run).
        next_high_wm = max_ts if max_ts is not None else high_wm
        if effective_kd:
            assert checkpoint_view_id is not None
            try:
                upsert_scope_checkpoint(
                    client,
                    checkpoint_view_id,
                    kd_space,
                    workflow_scope_param,
                    sv_fp,
                    int(next_high_wm if next_high_wm is not None else 0),
                    logger=logger,
                )
            except (CogniteAPIError, CogniteNotFoundError) as ex:
                if hasattr(logger, "warning"):
                    logger.warning(
                        "Key Discovery checkpoint upsert failed; writing RAW watermark instead: %s",
                        ex,
                    )
                wm_columns_fb: Dict[str, Any] = {
                    RECORD_KIND_COLUMN: RECORD_KIND_WATERMARK,
                    WORKFLOW_STATUS_COLUMN: WORKFLOW_STATUS_CHECKPOINT,
                    SCOPE_KEY_COLUMN: scope_key,
                    RAW_COL_UPDATED_AT: batch_updated_at,
                    HIGH_WATERMARK_MS_COLUMN: int(
                        next_high_wm if next_high_wm is not None else 0
                    ),
                }
                raw_uploader.add_to_upload_queue(
                    database=raw_db,
                    table=raw_table_key,
                    raw_row=Row(key=wm_key, columns=wm_columns_fb),
                )
        else:
            wm_columns: Dict[str, Any] = {
                RECORD_KIND_COLUMN: RECORD_KIND_WATERMARK,
                WORKFLOW_STATUS_COLUMN: WORKFLOW_STATUS_CHECKPOINT,
                SCOPE_KEY_COLUMN: scope_key,
                RAW_COL_UPDATED_AT: batch_updated_at,
                HIGH_WATERMARK_MS_COLUMN: int(
                    next_high_wm if next_high_wm is not None else 0
                ),
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
            "cohort_rows_skipped_unchanged_hash": cohort_rows_skipped_unchanged_hash,
            "raw_db": raw_db,
            "raw_table_key": raw_table_key,
        }
    )
    logger.info(
        f"Incremental state update wrote {cohort_rows} cohort row(s)"
        + (
            f", skipped {cohort_rows_skipped_unchanged_hash} unchanged input hash(es)"
            if cohort_rows_skipped_unchanged_hash
            else ""
        )
        + f"; run_id={run_id}"
    )
