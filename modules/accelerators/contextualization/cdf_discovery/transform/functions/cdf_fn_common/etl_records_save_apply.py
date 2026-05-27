"""Apply cohort rows to CDF Records API (ingest / upsert / delete)."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from cdf_fn_common.etl_common import _first_nonempty
from cdf_fn_common.etl_discovery_query_shared import (
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    cohort_instance_space_and_external_id,
    resolve_task_config,
)
from cdf_fn_common.etl_incremental_scope import RECORD_KIND_RECORD
from cdf_fn_common.etl_save_apply import (
    _iter_entity_rows_for_save,
    _prepare_save_cfg,
    _resolve_dry_run,
    _resolve_save_batch_size,
    score_cohort_row,
)
from cdf_fn_common.etl_save_merge import SAVE_FAN_IN_MERGE, build_merged_props_for_instance, parse_field_policies
from cdf_fn_common.etl_streams_records_api import (
    cohort_row_to_record_item,
    delete_records,
    ingest_records,
    retrieve_stream,
    upsert_records,
)
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data
from cdf_fn_common.query_enumeration import QueryEnumerationStats, enumeration_summary


def _stream_is_mutable(client: Any, stream_external_id: str) -> bool:
    try:
        detail = retrieve_stream(client, stream_external_id)
        return bool(detail.get("mutable"))
    except Exception:
        return False


def etl_apply_records_save(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = _prepare_save_cfg(data)
    stream_external_id = _first_nonempty(cfg.get("stream_external_id"), cfg.get("streamExternalId"))
    if not stream_external_id:
        raise ValueError("save_records requires config.stream_external_id")

    write_mode = _first_nonempty(cfg.get("write_mode"), "ingest").lower()
    if write_mode not in ("ingest", "upsert", "delete"):
        raise ValueError(f"save_records write_mode must be ingest, upsert, or delete; got {write_mode!r}")

    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    dry_run = _resolve_dry_run(data, client, cfg)
    batch_size = _resolve_save_batch_size(cfg)
    fan_mode = str(cfg.get("save_fan_in_mode") or "").strip()

    if client is not None and write_mode in ("upsert", "delete"):
        if not _stream_is_mutable(client, stream_external_id):
            raise ValueError(
                f"save_records write_mode={write_mode!r} requires a mutable stream; "
                f"{stream_external_id!r} is immutable"
            )

    record_type = _first_nonempty(cfg.get("record_type"))
    rows_read = 0
    rows_written = 0
    batch: List[Dict[str, Any]] = []

    def flush_batch() -> None:
        nonlocal rows_written, batch
        if not batch:
            return
        if dry_run or client is None:
            rows_written += len(batch)
            batch = []
            return
        body = {"items": batch}
        if write_mode == "ingest":
            ingest_records(client, stream_external_id, body)
        elif write_mode == "upsert":
            upsert_records(client, stream_external_id, body)
        else:
            delete_records(client, stream_external_id, body)
        rows_written += len(batch)
        batch = []

    all_rows = _iter_entity_rows_for_save(client, data, task_id)
    rows_read = len(all_rows)

    if fan_mode == SAVE_FAN_IN_MERGE and write_mode != "delete":
        policy_map = parse_field_policies(cfg)
        grouped: Dict[tuple, list] = {}
        for pred_index, cols, props in all_rows:
            kind = str(cols.get(RECORD_KIND_COLUMN) or RECORD_KIND_ENTITY)
            if kind not in (RECORD_KIND_ENTITY, RECORD_KIND_RECORD, ""):
                continue
            inst_space, ext_id = cohort_instance_space_and_external_id(cols, cfg=cfg, data=data, props=props)
            if not ext_id:
                continue
            key = (inst_space, ext_id)
            grouped.setdefault(key, []).append((score_cohort_row(cols, pred_index), pred_index, props))
        for (_sp, _ext), scored in grouped.items():
            merged = build_merged_props_for_instance(scored, policy_map)
            item = cohort_row_to_record_item({"EXTERNAL_ID": _ext, "RECORD_SPACE": _sp}, merged, record_type=record_type)
            if item:
                batch.append(item)
            if len(batch) >= batch_size:
                flush_batch()
    else:
        for _pred_index, cols, props in all_rows:
            kind = str(cols.get(RECORD_KIND_COLUMN) or RECORD_KIND_ENTITY)
            if kind not in (RECORD_KIND_ENTITY, RECORD_KIND_RECORD, ""):
                continue
            item = cohort_row_to_record_item(cols, props, record_type=record_type)
            if write_mode == "delete":
                if item.get("space") or item.get("externalId"):
                    batch.append({"space": item.get("space"), "externalId": item.get("externalId")})
            elif item.get("sources") or (item.get("space") and item.get("externalId")):
                batch.append(item)
            if len(batch) >= batch_size:
                flush_batch()

    flush_batch()

    enum_stats = QueryEnumerationStats(rows_read=rows_read, rows_written=rows_written, pages=1)
    return enumeration_summary(
        enum_stats,
        extra={
            "function_external_id": fn_external_id,
            "task_id": task_id,
            "stream_external_id": stream_external_id,
            "write_mode": write_mode,
            "dry_run": dry_run,
            "status": "ok",
        },
    )


def etl_apply_stream_save(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    operation = _first_nonempty(cfg.get("operation"), "create").lower()
    if operation != "create":
        raise ValueError(f"save_stream only supports operation=create; got {operation!r}")

    dry_run = bool(data.get("dry_run") or cfg.get("dry_run") or client is None)
    from cdf_fn_common.etl_streams_records_api import build_stream_create_body, create_stream

    body = build_stream_create_body(cfg)
    stream_external_id = _first_nonempty(body.get("externalId"), body.get("external_id"))

    if not dry_run and client is not None:
        create_stream(client, body)
        if log and hasattr(log, "info"):
            log.info("%s created stream %s", fn_external_id, stream_external_id)

    return {
        "status": "ok",
        "function_external_id": fn_external_id,
        "operation": operation,
        "stream_external_id": stream_external_id,
        "dry_run": dry_run,
    }
