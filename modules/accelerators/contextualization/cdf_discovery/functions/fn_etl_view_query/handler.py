"""CDF handler: ETL DM view query."""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any, Dict, MutableMapping, Optional

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_cohort_handoff import maybe_handoff_predecessor_rows
from cdf_fn_common.etl_dm_query import combine_view_query_filter
from cdf_fn_common.etl_query_predecessor import (
    build_predecessor_instance_dm_filter,
    predecessor_external_ids,
    should_restrict_view_query_to_predecessors,
)
from cdf_fn_common.etl_cohort_storage import predecessor_canvas_node_ids, resolve_incremental_state_sink
from cdf_fn_common.etl_common import (
    _first_nonempty,
    emit_agent_debug_log,
    extract_view_properties,
    node_instance_id_str,
    require_pipeline_run_key,
    resolve_task_config,
)
from cdf_fn_common.etl_dm_query import query_all_view_instances, query_stats_to_enumeration, ViewQueryStats
from cdf_fn_common.etl_incremental_hash import row_content_hash, should_skip_unchanged
from cdf_fn_common.etl_incremental_scope import (
    load_incremental_hashes_for_nodes,
    node_last_updated_time_ms,
    scope_key_from_view_dict,
    scope_watermark_row_key,
    read_watermark_high_ms,
    upsert_incremental_entity_hashes_raw,
    write_incremental_watermark_raw,
)
from cdf_fn_common.etl_query_recovery import (
    load_query_checkpoint_state,
    save_query_checkpoint_state,
)
from cdf_fn_common.etl_run_scope import (
    incremental_change_processing_enabled,
    incremental_listing_narrowed,
    incremental_skip_unchanged,
    is_lookup_full_scan,
    resolve_query_scope_mode,
    resolve_effective_incremental_change_processing,
    resolve_workflow_scope,
)
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data
from cdf_fn_common.query_enumeration import enumeration_summary, mark_truncated, resolve_run_record_cap

def _watermark_filter(high_ms: int) -> Any:
    from cognite.client import data_modeling as dm

    return dm.filters.Range(("node", "lastUpdatedTime"), gt=int(high_ms))


def _can_skip_hash_by_watermark(
    *,
    hash_skip: bool,
    listing_narrowed: bool,
    wm_before: Optional[int],
    lu: Optional[int],
    previous_hash: Optional[str],
) -> bool:
    if not hash_skip or not listing_narrowed or wm_before is None:
        return False
    if not previous_hash:
        return False
    if lu is None:
        return False
    return int(lu) <= int(wm_before)


def etl_handle_view_query(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    wall_t0 = time.perf_counter()
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    lookup_full_scan = is_lookup_full_scan(cfg)
    view_space = _first_nonempty(cfg.get("view_space"), "cdf_cdm")
    view_external_id = _first_nonempty(cfg.get("view_external_id"))
    view_version = _first_nonempty(cfg.get("view_version"), "v1")
    pre_run_id = _first_nonempty(data.get("run_id"), "pre-run")
    # #region agent log
    emit_agent_debug_log(
        run_id=pre_run_id,
        hypothesis_id="H1",
        location="fn_etl_view_query/handler.py:93",
        message="view_query_config_resolved",
        data={
            "task_id": _first_nonempty(data.get("task_id"), fn_external_id),
            "view_space": view_space,
            "view_external_id": view_external_id,
            "view_version": view_version,
            "has_filters": bool(cfg.get("filters")),
        },
    )
    # #endregion
    if not view_external_id:
        # #region agent log
        emit_agent_debug_log(
            run_id=pre_run_id,
            hypothesis_id="H1",
            location="fn_etl_view_query/handler.py:104",
            message="view_query_missing_external_id",
            data={"task_id": _first_nonempty(data.get("task_id"), fn_external_id)},
        )
        # #endregion
        raise ValueError("config.view_external_id is required for fn_etl_view_query")

    instance_space_raw = _first_nonempty(cfg.get("instance_space"))
    _ins = str(instance_space_raw or "").strip()
    instance_space = None if (not _ins or _ins.lower() == "all_spaces") else _ins

    from cognite.client.data_classes.data_modeling.ids import ViewId

    view_id = ViewId(space=view_space, external_id=view_external_id, version=view_version)
    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    # #region agent log
    emit_agent_debug_log(
        run_id=run_id,
        hypothesis_id="H2",
        location="fn_etl_view_query/handler.py:118",
        message="view_query_runtime_mode",
        data={
            "task_id": task_id,
            "incremental_enabled": bool(data.get("incremental_change_processing")),
            "has_client": client is not None,
        },
    )
    # #endregion

    include_properties = cfg.get("include_properties") or []
    if not isinstance(include_properties, list):
        include_properties = []

    scope_view = {
        "view_space": view_space,
        "view_external_id": view_external_id,
        "view_version": view_version,
        "instance_space": instance_space or None,
        "filters": cfg.get("filters") or [],
    }
    scope_key = scope_key_from_view_dict(scope_view)
    workflow_scope = resolve_workflow_scope(data)
    persist_state = incremental_change_processing_enabled(data, cfg)
    listing_narrowed = incremental_listing_narrowed(data, cfg)
    incremental_change_processing = resolve_effective_incremental_change_processing(data, cfg)
    hash_skip = incremental_skip_unchanged(data, cfg, listing_narrowed=listing_narrowed)

    state_t0 = time.perf_counter()
    query_cfg = dict(cfg)
    wm_before: Optional[int] = None
    latest_by_node_count = 0
    inc_raw_db = ""
    inc_raw_table = ""
    if client is not None and persist_state:
        inc_raw_db, inc_raw_table = resolve_incremental_state_sink(data)
        if listing_narrowed:
            wm_key = scope_watermark_row_key(scope_key, workflow_scope)
            wm_before = read_watermark_high_ms(client, inc_raw_db, inc_raw_table, wm_key)
            if wm_before is not None:
                query_cfg["_watermark_filter"] = _watermark_filter(wm_before)
    state_load_duration_sec = round(time.perf_counter() - state_t0, 6)

    if log is not None and hasattr(log, "info"):
        log.info(
            "%s view=%s/%s/%s persist_state=%s incremental_change_processing=%s listing_narrowed=%s "
            "hash_skip=%s watermark_before=%s prior_hash_nodes=%s state_load_duration_sec=%s",
            task_id,
            view_space,
            view_external_id,
            view_version,
            persist_state,
            incremental_change_processing,
            listing_narrowed,
            hash_skip,
            wm_before,
            latest_by_node_count,
            state_load_duration_sec,
        )

    if not isinstance(data.get("etl_view_property_names_cache"), dict):
        data["etl_view_property_names_cache"] = {}

    dm_filter = None
    skip_view_listing = False
    if should_restrict_view_query_to_predecessors(cfg):
        has_pred_edges = bool(predecessor_canvas_node_ids(data, task_id))
        pred_ids = predecessor_external_ids(data, task_id)
        if has_pred_edges and not pred_ids:
            skip_view_listing = True
        elif pred_ids:
            pred_filter = build_predecessor_instance_dm_filter(view_id, pred_ids)
            if pred_filter is not None:
                watermark_filter = query_cfg.pop("_watermark_filter", None)
                dm_filter = combine_view_query_filter(
                    view_id,
                    user_filters=query_cfg.get("filters") or [],
                    instance_space=instance_space,
                    watermark_filter=watermark_filter,
                )
                from cognite.client import data_modeling as dm

                dm_filter = dm.filters.And(dm_filter, pred_filter)

    rows: list[dict[str, Any]] = []
    stats = ViewQueryStats()
    n_listed = 0
    n_skipped_hash = 0
    max_last_updated: Optional[int] = wm_before if listing_narrowed else None
    incremental_hash_pending: list[dict[str, Any]] = []
    query_scope_mode = resolve_query_scope_mode(cfg)
    run_record_cap = resolve_run_record_cap(data, cfg)
    checkpoint_enabled = (not lookup_full_scan) and (incremental_change_processing or run_record_cap > 0)
    checkpoint = (
        load_query_checkpoint_state(client, data, task_id=task_id)
        if checkpoint_enabled
        else None
    )

    loop_t0 = time.perf_counter()
    if client is not None and not skip_view_listing:
        for inst in query_all_view_instances(
            client,
            view_id=view_id,
            instance_space=instance_space,
            dm_filter=dm_filter,
            cfg=query_cfg,
            logger=log,
            progress_context=f"task={task_id}",
            stats_out=stats,
            property_names_cache=data["etl_view_property_names_cache"],
            initial_cursor=(checkpoint.continuation_token if checkpoint is not None else "") or None,
            max_items=run_record_cap,
        ):
            ext_id = _first_nonempty(getattr(inst, "external_id", None))
            if not ext_id:
                continue
            nid = node_instance_id_str(inst)
            if not nid:
                continue
            n_listed += 1
            props = extract_view_properties(inst, view_id)
            if include_properties:
                props = {k: props[k] for k in include_properties if k in props}
            props["instance_space"] = _first_nonempty(getattr(inst, "space", None))
            lu = node_last_updated_time_ms(inst)
            if lu is not None:
                max_last_updated = lu if max_last_updated is None else max(max_last_updated, lu)

            prev_hash: str | None = None
            if hash_skip and inc_raw_db and inc_raw_table:
                prev_hash = load_incremental_hashes_for_nodes(
                    client,
                    inc_raw_db,
                    inc_raw_table,
                    workflow_scope=workflow_scope,
                    scope_key=scope_key,
                    node_instance_ids=[nid],
                ).get(nid)
                if prev_hash:
                    latest_by_node_count += 1

            if _can_skip_hash_by_watermark(
                hash_skip=hash_skip,
                listing_narrowed=listing_narrowed,
                wm_before=wm_before,
                lu=lu,
                previous_hash=prev_hash,
            ):
                n_skipped_hash += 1
                continue

            content_hash = row_content_hash(props)
            if hash_skip and should_skip_unchanged(
                content_hash=content_hash,
                previous_hash=prev_hash,
                incremental_skip_unchanged=True,
            ):
                n_skipped_hash += 1
                continue

            rows.append({"columns": {"node_instance_id": nid, "external_id": ext_id}, "properties": props})
            if persist_state:
                incremental_hash_pending.append(
                    {
                        "node_instance_id": nid,
                        "external_id": ext_id,
                        "extraction_inputs_hash": content_hash,
                        "last_updated_ms": lu,
                    }
                )
    if run_record_cap > 0 and len(rows) >= run_record_cap and stats.next_cursor:
        enum_stats = query_stats_to_enumeration(stats)
        mark_truncated(enum_stats, reason="max_records_per_run")
    else:
        enum_stats = query_stats_to_enumeration(stats)

    query_duration_sec = stats.list_duration_sec
    loop_wall_sec = time.perf_counter() - loop_t0
    loop_duration_sec = round(max(0.0, loop_wall_sec - query_duration_sec), 6)

    if client is not None and persist_state and inc_raw_db and inc_raw_table:
        if incremental_hash_pending:
            upsert_incremental_entity_hashes_raw(
                client,
                raw_db=inc_raw_db,
                raw_table=inc_raw_table,
                workflow_scope=workflow_scope,
                scope_key=scope_key,
                run_id=run_id,
                source_view_fingerprint="",
                items=incremental_hash_pending,
            )
        if max_last_updated is not None:
            write_incremental_watermark_raw(
                client,
                raw_db=inc_raw_db,
                raw_table=inc_raw_table,
                scope_key=scope_key,
                workflow_scope=workflow_scope,
                high_ms=int(max_last_updated),
                run_id=run_id,
            )

    scope_key_handoff = _first_nonempty(cfg.get("scope_key"), data.get("scope_key"), "default")
    entity_type = _first_nonempty(cfg.get("entity_type"), view_external_id)
    cohort_summary = maybe_handoff_predecessor_rows(
        client,
        data,
        run_id=run_id,
        scope_key=scope_key_handoff,
        task_id=task_id,
        query_source="view",
        entity_type=entity_type,
        view_space=view_space,
        view_external_id=view_external_id,
        view_version=view_version,
        rows=rows,
        log=log,
    )
    total_duration_sec = round(time.perf_counter() - wall_t0, 6)
    extra: Dict[str, Any] = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "instances_listed": n_listed,
        "instances_written": len(rows),
        "instances_skipped_hash": n_skipped_hash,
        "run_id": run_id,
        "view": f"{view_space}/{view_external_id}/{view_version}",
        "instance_space": instance_space or None,
        "predecessor_mode": "cohort" if cohort_summary else "in_memory",
        "incremental_change_processing": incremental_change_processing,
        "listing_narrowed": listing_narrowed,
        "persist_state": persist_state,
        "hash_skip": hash_skip,
        "watermark_before_ms": wm_before,
        "workflow_scope": workflow_scope or None,
        "incremental_raw_db": inc_raw_db or None,
        "incremental_raw_table": inc_raw_table or None,
        "prior_hash_nodes": latest_by_node_count,
        "state_load_duration_sec": state_load_duration_sec,
        "query_duration_sec": query_duration_sec,
        "loop_duration_sec": loop_duration_sec,
        "total_duration_sec": total_duration_sec,
    }
    if log is not None and hasattr(log, "info"):
        log.info(
            "%s timing state_load_sec=%s query_sec=%s loop_sec=%s total_sec=%s "
            "listed=%s written=%s skipped_hash=%s",
            task_id,
            state_load_duration_sec,
            query_duration_sec,
            loop_duration_sec,
            total_duration_sec,
            n_listed,
            len(rows),
            n_skipped_hash,
        )
    if cohort_summary:
        extra.update(cohort_summary)
    if checkpoint_enabled and checkpoint is not None:
        save_query_checkpoint_state(
            client,
            data,
            task_id=task_id,
            run_id=run_id,
            rows_completed=checkpoint.rows_completed + len(rows),
            is_complete=not enum_stats.rows_truncated,
            continuation_token=stats.next_cursor,
        )
    extra["query_scope_mode"] = query_scope_mode
    extra["effective_scope_mode"] = "all" if lookup_full_scan else query_scope_mode
    extra["lookup_full_scan"] = lookup_full_scan
    extra["effective_run_cap"] = run_record_cap if run_record_cap > 0 else None
    extra["resume_checkpoint_rows"] = checkpoint.rows_completed if checkpoint is not None else 0
    extra["resume_checkpoint_complete"] = checkpoint.is_complete if checkpoint is not None else False
    return enumeration_summary(enum_stats, extra=extra)


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_view_query("fn_etl_view_query", data, client, log=None)
