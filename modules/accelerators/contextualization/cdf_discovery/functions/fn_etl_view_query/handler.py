"""CDF handler: ETL DM view query."""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, MutableMapping, Optional

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_cohort_handoff import maybe_handoff_predecessor_rows
from cdf_fn_common.etl_cohort_storage import resolve_incremental_state_sink
from cdf_fn_common.etl_common import (
    _first_nonempty,
    extract_view_properties,
    node_instance_id_str,
    resolve_run_id,
    resolve_task_config,
)
from cdf_fn_common.etl_dm_query import query_all_view_instances, query_stats_to_enumeration, ViewQueryStats
from cdf_fn_common.etl_incremental_hash import row_content_hash, should_skip_unchanged
from cdf_fn_common.etl_incremental_scope import (
    build_latest_hash_index_for_table,
    node_last_updated_time_ms,
    scope_key_from_view_dict,
    scope_watermark_row_key,
    read_watermark_high_ms,
    upsert_incremental_entity_hashes_raw,
    write_incremental_watermark_raw,
)
from cdf_fn_common.etl_run_scope import (
    incremental_change_processing_enabled,
    incremental_listing_narrowed,
    incremental_skip_unchanged,
    resolve_effective_incremental_change_processing,
    resolve_workflow_scope,
)
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data
from cdf_fn_common.query_enumeration import enumeration_summary

HashIndexByScope = Dict[str, Dict[str, str]]
HashIndexGetter = Callable[[Any, str, str, str], HashIndexByScope]


def _watermark_filter(high_ms: int) -> Any:
    from cognite.client import data_modeling as dm

    return dm.filters.Range(("node", "lastUpdatedTime"), gt=int(high_ms))


def _hash_index_sink_key(raw_db: str, raw_table: str, workflow_scope: str) -> str:
    return f"{raw_db}:{raw_table}:{workflow_scope or '(none)'}"


def _resolve_full_hash_index(
    client: Any,
    data: MutableMapping[str, Any],
    *,
    raw_db: str,
    raw_table: str,
    workflow_scope: str,
) -> HashIndexByScope:
    """
    Full table index: scope_key -> node_instance_id -> extraction_inputs_hash.

    Local runner may set ``etl_raw_hash_index_cache`` to a callable; deployed invocations
    use a dict keyed by sink (db/table/workflow_scope).
    """
    cache = data.get("etl_raw_hash_index_cache")
    if callable(cache):
        full = cache(client, raw_db, raw_table, workflow_scope)
        return dict(full) if isinstance(full, dict) else {}

    sink_key = _hash_index_sink_key(raw_db, raw_table, workflow_scope)
    store: Dict[str, HashIndexByScope]
    if isinstance(cache, dict):
        store = cache
    else:
        store = {}
        data["etl_raw_hash_index_cache"] = store

    full = store.get(sink_key)
    if not isinstance(full, dict):
        full = build_latest_hash_index_for_table(
            client,
            raw_db,
            raw_table,
            workflow_scope=workflow_scope,
        )
        store[sink_key] = full
    return full


def _load_hash_index(
    client: Any,
    data: MutableMapping[str, Any],
    *,
    raw_db: str,
    raw_table: str,
    scope_key: str,
    workflow_scope: str,
) -> Dict[str, str]:
    full = _resolve_full_hash_index(
        client,
        data,
        raw_db=raw_db,
        raw_table=raw_table,
        workflow_scope=workflow_scope,
    )
    return dict(full.get(scope_key, {}))


def _can_skip_hash_by_watermark(
    *,
    hash_skip: bool,
    listing_narrowed: bool,
    wm_before: Optional[int],
    lu: Optional[int],
    nid: str,
    latest_by_node: Dict[str, str],
) -> bool:
    if not hash_skip or not listing_narrowed or wm_before is None:
        return False
    if nid not in latest_by_node:
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
    view_space = _first_nonempty(cfg.get("view_space"), "cdf_cdm")
    view_external_id = _first_nonempty(cfg.get("view_external_id"))
    view_version = _first_nonempty(cfg.get("view_version"), "v1")
    if not view_external_id:
        raise ValueError("config.view_external_id is required for fn_etl_view_query")

    instance_space_raw = _first_nonempty(cfg.get("instance_space"))
    _ins = str(instance_space_raw or "").strip()
    instance_space = None if (not _ins or _ins.lower() == "all_spaces") else _ins

    from cognite.client.data_classes.data_modeling.ids import ViewId

    view_id = ViewId(space=view_space, external_id=view_external_id, version=view_version)
    run_id = resolve_run_id(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)

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
    latest_by_node: Dict[str, str] = {}
    inc_raw_db = ""
    inc_raw_table = ""
    if client is not None and persist_state:
        inc_raw_db, inc_raw_table = resolve_incremental_state_sink(data)
        if listing_narrowed:
            wm_key = scope_watermark_row_key(scope_key, workflow_scope)
            wm_before = read_watermark_high_ms(client, inc_raw_db, inc_raw_table, wm_key)
            if wm_before is not None:
                query_cfg["_watermark_filter"] = _watermark_filter(wm_before)
            if hash_skip:
                latest_by_node = _load_hash_index(
                    client,
                    data,
                    raw_db=inc_raw_db,
                    raw_table=inc_raw_table,
                    scope_key=scope_key,
                    workflow_scope=workflow_scope,
                )
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
            len(latest_by_node),
            state_load_duration_sec,
        )

    if not isinstance(data.get("etl_view_property_names_cache"), dict):
        data["etl_view_property_names_cache"] = {}

    rows: list[dict[str, Any]] = []
    stats = ViewQueryStats()
    n_listed = 0
    n_skipped_hash = 0
    max_last_updated: Optional[int] = wm_before if listing_narrowed else None
    incremental_hash_pending: list[dict[str, Any]] = []

    loop_t0 = time.perf_counter()
    if client is not None:
        for inst in query_all_view_instances(
            client,
            view_id=view_id,
            instance_space=instance_space,
            cfg=query_cfg,
            logger=log,
            progress_context=f"task={task_id}",
            stats_out=stats,
            property_names_cache=data["etl_view_property_names_cache"],
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

            if _can_skip_hash_by_watermark(
                hash_skip=hash_skip,
                listing_narrowed=listing_narrowed,
                wm_before=wm_before,
                lu=lu,
                nid=nid,
                latest_by_node=latest_by_node,
            ):
                n_skipped_hash += 1
                continue

            content_hash = row_content_hash(props)
            if hash_skip and should_skip_unchanged(
                content_hash=content_hash,
                previous_hash=latest_by_node.get(nid),
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
    enum_stats = query_stats_to_enumeration(stats)
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
        "prior_hash_nodes": len(latest_by_node),
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
    return enumeration_summary(enum_stats, extra=extra)


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_view_query("fn_etl_view_query", data, client, log=None)
