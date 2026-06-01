"""ETL save stages: DM view apply, RAW cohort replication, classic resource updates."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple

from cognite.client.data_classes import AssetUpdate, FileMetadataUpdate, TimeSeriesUpdate
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData
from cognite.client.data_classes.data_modeling.ids import ViewId

from cdf_fn_common.etl_build_index_orchestration import _iter_index_rows_for_save
from cdf_fn_common.etl_cohort_storage import (
    canvas_node_id_for_task,
    iter_cohort_entity_rows,
    predecessor_node_table_locations,
    require_pipeline_run_key,
)
from cdf_fn_common.etl_inverted_index import persist_inverted_index_rows
from cdf_fn_common.etl_common import emit_agent_debug_log, iter_predecessor_rows
from cdf_fn_common.etl_discovery_cohort import (
    _props_from_row_columns,
    iter_predecessor_raw_locations,
)
from cdf_fn_common.etl_discovery_query_shared import (
    ENTITY_TYPE_COLUMN,
    EXTERNAL_ID_COLUMN,
    INSTANCE_SPACE_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    SCOPE_KEY_COLUMN,
    VIEW_EXTERNAL_ID_COLUMN,
    VIEW_SPACE_COLUMN,
    VIEW_VERSION_COLUMN,
    _first_nonempty,
    _flush_rows,
    build_entity_cohort_row,
    cohort_instance_space_and_external_id,
    resolve_raw_save_sink,
    resolve_task_config,
)
from cdf_fn_common.etl_predecessor_mode import use_in_memory_predecessors
from cdf_fn_common.etl_property_merge import FieldPolicy, STRATEGY_MERGE_LIST, parse_field_policies
from cdf_fn_common.etl_raw_upload import RawRowsUploadQueue
from cdf_fn_common.etl_save_merge import (
    SAVE_FAN_IN_MERGE,
    SAVE_FAN_IN_NONE,
    build_merged_props_for_instance,
    filter_props_internal,
    score_cohort_row,
    validate_save_config,
)
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data
from cdf_fn_common.etl_ui_progress import emit_handler_progress

_INTERNAL_PROP_KEYS = frozenset(
    {
        "raw_columns",
        "_variant_index",
        "instance_space",
        "space",
    }
)


_DEFAULT_DM_LIST_PROPERTIES = frozenset({"aliases"})
DEFAULT_SAVE_BATCH_SIZE = 500


def _resolve_save_batch_size(cfg: Mapping[str, Any]) -> int:
    raw = cfg.get("batch_size")
    if raw is None:
        return DEFAULT_SAVE_BATCH_SIZE
    try:
        size = int(raw)
    except (TypeError, ValueError) as e:
        raise ValueError(f"save batch_size must be a positive integer; got {raw!r}") from e
    if size < 1:
        raise ValueError(f"save batch_size must be >= 1; got {size}")
    return size


def _prepare_save_cfg(data: MutableMapping[str, Any]) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = dict(resolve_task_config(data))
    if not _first_nonempty(cfg.get("save_fan_in_mode")):
        cfg["save_fan_in_mode"] = SAVE_FAN_IN_NONE
    return cfg


def _resolve_dry_run(data: Mapping[str, Any], client: Any, cfg: Mapping[str, Any]) -> bool:
    return bool(data.get("dry_run") or cfg.get("dry_run") or client is None)


def _list_property_names_for_view_apply(
    policy_map: Mapping[str, FieldPolicy],
) -> frozenset[str]:
    names = set(_DEFAULT_DM_LIST_PROPERTIES)
    for prop, pol in policy_map.items():
        if pol.strategy == STRATEGY_MERGE_LIST:
            names.add(prop)
    return frozenset(names)


def _dedupe_strings_preserve_order(items: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for s in items:
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _coerce_dm_list_property_value(value: Any) -> Optional[List[str]]:
    if value is None:
        return None
    if isinstance(value, list):
        parts: List[str] = []
        for x in value:
            if isinstance(x, dict):
                v = _first_nonempty(x.get("value"), x.get("alias"), x.get("key"))
                if v:
                    parts.append(str(v).strip())
            else:
                s = str(x).strip()
                if s:
                    parts.append(s)
        out = _dedupe_strings_preserve_order(parts)
        return out or None
    if isinstance(value, dict) and "value" in value:
        s = str(value.get("value") or "").strip()
        return [s] if s else None
    s = str(value).strip()
    if not s:
        return None
    if "," in s:
        parts = _dedupe_strings_preserve_order([p.strip() for p in s.split(",") if p.strip()])
        return parts or None
    return [s]


def _prepare_view_apply_properties(
    props: Mapping[str, Any],
    *,
    list_properties: frozenset[str],
) -> Optional[Dict[str, Any]]:
    out: Dict[str, Any] = {}
    for key, val in props.items():
        if key in list_properties:
            coerced = _coerce_dm_list_property_value(val)
            if coerced is None:
                continue
            out[key] = coerced
        elif val is None or val == "":
            continue
        else:
            out[key] = val
    return out or None


def _classic_instance_key(cols: Mapping[str, Any]) -> Tuple[str, str]:
    ext_id = _first_nonempty(cols.get(EXTERNAL_ID_COLUMN))
    return ("", ext_id)


def _iter_entity_rows_for_save(
    client: Any,
    data: Mapping[str, Any],
    task_id: str,
) -> List[Tuple[int, Dict[str, Any], Dict[str, Any]]]:
    out: List[Tuple[int, Dict[str, Any], Dict[str, Any]]] = []
    if use_in_memory_predecessors(data):
        for pred_index, (cols, props) in enumerate(iter_predecessor_rows(data)):
            out.append(
                (
                    pred_index,
                    dict(cols),
                    filter_props_internal(dict(props), _INTERNAL_PROP_KEYS),
                )
            )
        return out
    if client is None:
        return out
    for pred_index, (source_db, source_table) in enumerate(
        predecessor_node_table_locations(data, task_id)
    ):
        for row in iter_cohort_entity_rows(client, source_db, source_table):
            cols = dict(getattr(row, "columns", None) or {})
            if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
                continue
            props = filter_props_internal(
                _props_from_row_columns(cols),
                _INTERNAL_PROP_KEYS,
            )
            out.append((pred_index, cols, props))
    return out


def _gather_view_rows_by_instance(
    rows: List[Tuple[int, Dict[str, Any], Dict[str, Any]]],
    *,
    cfg: Mapping[str, Any],
    data: Mapping[str, Any],
) -> Tuple[
    Dict[Tuple[str, str], List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]],
    int,
]:
    from collections import defaultdict

    acc: Dict[
        Tuple[str, str], List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]
    ] = defaultdict(list)
    gather_skipped_missing_identity = 0
    for pred_index, cols, props in rows:
        inst_space, ext_id = cohort_instance_space_and_external_id(
            cols, cfg=cfg, data=data, props=props
        )
        if not ext_id or not inst_space:
            gather_skipped_missing_identity += 1
            continue
        sc = score_cohort_row(cols, pred_index)
        acc[(inst_space, ext_id)].append((sc, pred_index, props))
    return dict(acc), gather_skipped_missing_identity


def etl_apply_view_save(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    cfg = _prepare_save_cfg(data)
    inst_space_cfg = _first_nonempty(cfg.get("instance_space"), data.get("instance_space"))
    if inst_space_cfg:
        cfg["instance_space"] = inst_space_cfg
        data["instance_space"] = inst_space_cfg
    validate_save_config(cfg, save_kind="view")

    view_space = _first_nonempty(cfg.get("view_space"), "cdf_cdm")
    view_external_id = _first_nonempty(cfg.get("view_external_id"))
    view_version = _first_nonempty(cfg.get("view_version"), "v1")
    pre_run_id = _first_nonempty(data.get("run_id"), "pre-run")
    # #region agent log
    emit_agent_debug_log(
        run_id=pre_run_id,
        hypothesis_id="H4",
        location="cdf_fn_common/etl_save_apply.py:248",
        message="view_save_entry",
        data={
            "task_id": _first_nonempty(data.get("task_id"), fn_external_id),
            "view_space": view_space,
            "view_external_id": view_external_id,
            "view_version": view_version,
            "client_present": client is not None,
        },
    )
    # #endregion
    if not view_external_id:
        raise ValueError("save_view requires config.view_external_id")

    view_id = ViewId(space=view_space, external_id=view_external_id, version=view_version)
    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    dry_run = _resolve_dry_run(data, client, cfg)
    fan_mode = str(cfg.get("save_fan_in_mode") or "").strip()
    policy_map = parse_field_policies(cfg)
    list_properties = _list_property_names_for_view_apply(policy_map)

    pred_locations = iter_predecessor_raw_locations(data, task_id)
    rows_read = 0
    instances_applied = 0
    skipped = 0
    gather_skipped_missing_identity = 0
    batch: List[NodeApply] = []
    batch_size = _resolve_save_batch_size(cfg)

    all_rows = _iter_entity_rows_for_save(client, data, task_id)
    rows_read = len(all_rows)
    apply_targets = 0
    processed_targets = 0

    def emit_apply(inst_space: str, ext_id: str, props: Mapping[str, Any]) -> None:
        nonlocal instances_applied, skipped, batch, processed_targets
        prepared = _prepare_view_apply_properties(
            props,
            list_properties=list_properties,
        )
        if not prepared:
            skipped += 1
            return
        batch.append(
            NodeApply(
                space=inst_space,
                external_id=ext_id,
                sources=[NodeOrEdgeData(view_id, prepared)],
            )
        )
        if len(batch) >= batch_size:
            if not dry_run and client is not None:
                try:
                    client.data_modeling.instances.apply(nodes=batch)
                except Exception as ex:
                    # #region agent log
                    emit_agent_debug_log(
                        run_id=run_id,
                        hypothesis_id="H4",
                        location="cdf_fn_common/etl_save_apply.py:305",
                        message="view_save_apply_exception",
                        data={
                            "task_id": task_id,
                            "batch_size": len(batch),
                            "error": str(ex),
                        },
                    )
                    # #endregion
                    raise
            instances_applied += len(batch)
            batch.clear()
        processed_targets += 1
        if apply_targets > 0:
            emit_handler_progress(processed_targets, total=apply_targets, label="instances")

    if fan_mode == SAVE_FAN_IN_MERGE:
        grouped, gather_skipped_missing_identity = _gather_view_rows_by_instance(
            all_rows, cfg=cfg, data=data
        )
        apply_targets = len(grouped)
        for (inst_space, ext_id), scored in grouped.items():
            merged = build_merged_props_for_instance(scored, policy_map)
            emit_apply(inst_space, ext_id, merged)
    else:
        apply_targets = rows_read
        for pred_index, cols, props in all_rows:
            inst_space, ext_id = cohort_instance_space_and_external_id(
                cols, cfg=cfg, data=data, props=props
            )
            if not ext_id or not inst_space:
                skipped += 1
                continue
            scored = [(score_cohort_row(cols, pred_index), pred_index, props)]
            merged = build_merged_props_for_instance(scored, policy_map)
            emit_apply(inst_space, ext_id, merged)

    if batch:
        if not dry_run and client is not None:
            try:
                client.data_modeling.instances.apply(nodes=batch)
            except Exception as ex:
                # #region agent log
                emit_agent_debug_log(
                    run_id=run_id,
                    hypothesis_id="H4",
                    location="cdf_fn_common/etl_save_apply.py:344",
                    message="view_save_final_apply_exception",
                    data={
                        "task_id": task_id,
                        "batch_size": len(batch),
                        "error": str(ex),
                    },
                )
                # #endregion
                raise
        instances_applied += len(batch)

    if apply_targets > 0:
        emit_handler_progress(apply_targets, total=apply_targets, label="instances", force=True)

    if log and hasattr(log, "info"):
        log.info(
            "%s view_save rows_read=%s instances_applied=%s skipped=%s view=%s/%s/%s dry_run=%s fan_in=%s",
            fn_external_id,
            rows_read,
            instances_applied,
            skipped,
            view_space,
            view_external_id,
            view_version,
            dry_run,
            fan_mode,
        )

    summary: Dict[str, Any] = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "rows_read": rows_read,
        "rows_written": instances_applied,
        "instances_applied": instances_applied,
        "skipped": skipped,
        "gather_skipped_missing_identity": gather_skipped_missing_identity,
        "run_id": run_id,
        "view_space": view_space,
        "view_external_id": view_external_id,
        "view_version": view_version,
        "dry_run": dry_run,
        "save_fan_in_mode": fan_mode,
        "status": "ok",
        "description": _first_nonempty(cfg.get("description")),
        "predecessor_raw_sources": [{"raw_db": d, "raw_table": t} for d, t in pred_locations],
    }
    if policy_map:
        summary["save_field_policies_count"] = len(policy_map)
    return summary


def etl_persist_inverted_index_save(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    """Persist inverted-index cohort rows from predecessors to configured RAW sink."""
    cfg = _prepare_save_cfg(data)
    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    sink_db, sink_table = resolve_raw_save_sink(cfg)
    dry_run = _resolve_dry_run(data, client, cfg)

    index_rows = _iter_index_rows_for_save(client, data, task_id)
    rows_read = len(index_rows)
    rows_written = 0

    batch_size = _resolve_save_batch_size(cfg)
    if not dry_run and client is not None and index_rows:
        rows_written = persist_inverted_index_rows(
            client,
            raw_db=sink_db,
            raw_table=sink_table,
            index_rows=index_rows,
            merge_with_existing=True,
            batch_size=batch_size,
            log=log,
        )

    pred_locations = iter_predecessor_raw_locations(data, task_id)
    if log and hasattr(log, "info"):
        log.info(
            "%s inverted_index_save rows_read=%s rows_written=%s sink=%s/%s dry_run=%s",
            fn_external_id,
            rows_read,
            rows_written,
            sink_db,
            sink_table,
            dry_run,
        )

    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "index_rows_written": rows_written,
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
        "dry_run": dry_run,
        "save_kind": "inverted_index",
        "status": "ok",
        "description": _first_nonempty(cfg.get("description")),
        "predecessor_raw_sources": [{"raw_db": d, "raw_table": t} for d, t in pred_locations],
    }


def etl_replicate_raw_save(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    cfg = _prepare_save_cfg(data)
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    index_rows = _iter_index_rows_for_save(client, data, task_id)
    if index_rows:
        return etl_persist_inverted_index_save(fn_external_id, data, client, log)

    validate_save_config(cfg, save_kind="raw")

    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    writer_canvas = canvas_node_id_for_task(data, task_id)
    sink_db, sink_table = resolve_raw_save_sink(cfg)
    fan_mode = str(cfg.get("save_fan_in_mode") or "").strip()
    policy_map = parse_field_policies(cfg)
    dry_run = _resolve_dry_run(data, client, cfg)

    pred_locations = iter_predecessor_raw_locations(data, task_id)
    queue = RawRowsUploadQueue(client) if client is not None else None
    pending: List[Dict[str, Any]] = []
    rows_read = 0
    rows_written = 0
    batch_size = _resolve_save_batch_size(cfg)

    all_rows = _iter_entity_rows_for_save(client, data, task_id)
    rows_read = len(all_rows)

    def flush_one(
        cols: Mapping[str, Any],
        props: Mapping[str, Any],
    ) -> None:
        nonlocal rows_written, pending
        scope_key = _first_nonempty(cols.get(SCOPE_KEY_COLUMN), "raw_save")
        nid = _first_nonempty(cols.get(NODE_INSTANCE_ID_COLUMN))
        ext_id = _first_nonempty(cols.get(EXTERNAL_ID_COLUMN), nid)
        pending.append(
            build_entity_cohort_row(
                run_id=run_id,
                scope_key=scope_key,
                canvas_node_id=writer_canvas,
                query_source="raw_save",
                node_instance_id=str(nid or ext_id or rows_written),
                external_id=str(ext_id or nid or rows_written),
                entity_type=_first_nonempty(cols.get(ENTITY_TYPE_COLUMN), "entity"),
                view_space=_first_nonempty(cols.get(VIEW_SPACE_COLUMN)),
                view_external_id=_first_nonempty(cols.get(VIEW_EXTERNAL_ID_COLUMN)),
                view_version=_first_nonempty(cols.get(VIEW_VERSION_COLUMN)),
                properties=dict(props),
            )
        )
        rows_written += 1
        if queue is not None and len(pending) >= batch_size:
            _flush_rows(queue, sink_db, sink_table, pending, client=client)

    if fan_mode == SAVE_FAN_IN_MERGE:
        grouped: Dict[
            Tuple[str, str], List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]
        ] = {}
        cols_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for pred_index, cols, props in all_rows:
            inst_space, ext_id = cohort_instance_space_and_external_id(
                cols, cfg=cfg, data=data, props=props
            )
            if not ext_id or not inst_space:
                continue
            key = (inst_space, ext_id)
            sc = score_cohort_row(cols, pred_index)
            grouped.setdefault(key, []).append((sc, pred_index, props))
            cols_by_key[key] = cols
        for key, scored in grouped.items():
            cols = cols_by_key[key]
            merged = build_merged_props_for_instance(scored, policy_map)
            flush_one(cols, merged)
    else:
        for pred_index, cols, props in all_rows:
            inst_space, ext_id = cohort_instance_space_and_external_id(
                cols, cfg=cfg, data=data, props=props
            )
            if not ext_id or not inst_space:
                continue
            scored = [(score_cohort_row(cols, pred_index), pred_index, props)]
            merged = build_merged_props_for_instance(scored, policy_map)
            flush_one(cols, merged)

    if queue is not None and not dry_run:
        _flush_rows(queue, sink_db, sink_table, pending, client=client)
    elif dry_run:
        pending.clear()

    if log and hasattr(log, "info"):
        log.info(
            "%s raw_save rows_read=%s rows_written=%s sink=%s/%s fan_in=%s dry_run=%s",
            fn_external_id,
            rows_read,
            rows_written,
            sink_db,
            sink_table,
            fan_mode,
            dry_run,
        )

    summary = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
        "dry_run": dry_run,
        "save_fan_in_mode": fan_mode,
        "status": "ok",
        "description": _first_nonempty(cfg.get("description")),
        "predecessor_raw_sources": [{"raw_db": d, "raw_table": t} for d, t in pred_locations],
    }
    if policy_map:
        summary["save_field_policies_count"] = len(policy_map)
    return summary


def _coerce_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def _classic_build_update(
    resource_type: str, external_id: str, props: Mapping[str, Any]
) -> Optional[Any]:
    rt = resource_type.strip().lower().rstrip("s")
    name = _coerce_str(props.get("name"))
    desc = _coerce_str(props.get("description"))
    meta = props.get("metadata")
    if not isinstance(meta, dict):
        meta = None
    src = _coerce_str(props.get("source"))

    if rt == "asset":
        u = AssetUpdate(external_id=external_id)
        changed = False
        if name:
            u.name.set(name)
            changed = True
        if desc:
            u.description.set(desc)
            changed = True
        if meta is not None:
            u.metadata.set(meta)
            changed = True
        if src:
            u.source.set(src)
            changed = True
        return u if changed else None

    if rt == "file":
        u = FileMetadataUpdate(external_id=external_id)
        changed = False
        if name:
            u.name.set(name)
            changed = True
        if desc:
            u.description.set(desc)
            changed = True
        if meta is not None:
            u.metadata.set(meta)
            changed = True
        if src:
            u.source.set(src)
            changed = True
        return u if changed else None

    if rt in ("timeseries", "time_series"):
        u = TimeSeriesUpdate(external_id=external_id)
        changed = False
        if name:
            u.name.set(name)
            changed = True
        if desc:
            u.description.set(desc)
            changed = True
        unit = _coerce_str(props.get("unit"))
        if unit:
            u.unit.set(unit)
            changed = True
        return u if changed else None

    raise ValueError(
        f"classic_save unsupported resource_type={resource_type!r} "
        f"(supported: assets, files, time_series)"
    )


def _classic_issue_updates(client: Any, resource_type: str, updates: List[Any]) -> None:
    if not updates:
        return
    rt = resource_type.strip().lower().rstrip("s")
    if rt == "asset":
        client.assets.update(updates)
    elif rt == "file":
        client.files.update(updates)
    elif rt in ("timeseries", "time_series"):
        client.time_series.update(updates)
    else:
        raise ValueError(
            f"classic_save unsupported resource_type={resource_type!r} "
            f"(supported: assets, files, time_series)"
        )


def etl_apply_classic_save(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    cfg = _prepare_save_cfg(data)
    validate_save_config(cfg, save_kind="classic")

    resource_type = _first_nonempty(cfg.get("resource_type"), "assets")
    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    dry_run = _resolve_dry_run(data, client, cfg)
    fan_mode = str(cfg.get("save_fan_in_mode") or "").strip()
    policy_map = parse_field_policies(cfg)

    pred_locations = iter_predecessor_raw_locations(data, task_id)
    rows_read = 0
    updates_applied = 0
    skipped = 0
    batch_size = _resolve_save_batch_size(cfg)
    pending_updates: List[Any] = []

    all_rows = _iter_entity_rows_for_save(client, data, task_id)
    rows_read = len(all_rows)

    def flush_updates() -> None:
        nonlocal updates_applied, pending_updates
        if not pending_updates:
            return
        if not dry_run and client is not None:
            _classic_issue_updates(client, resource_type, pending_updates)
        updates_applied += len(pending_updates)
        pending_updates.clear()

    def apply_one(ext_id: str, props: Mapping[str, Any]) -> None:
        nonlocal skipped, pending_updates
        try:
            upd = _classic_build_update(resource_type, ext_id, props)
            if upd is None:
                skipped += 1
                return
            pending_updates.append(upd)
            if len(pending_updates) >= batch_size:
                flush_updates()
        except Exception:
            skipped += 1
            if log and hasattr(log, "warning"):
                log.warning(
                    "classic_save skip external_id=%s resource_type=%s",
                    ext_id,
                    resource_type,
                    exc_info=True,
                )

    if fan_mode == SAVE_FAN_IN_MERGE:
        grouped: Dict[
            Tuple[str, str], List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]
        ] = {}
        for pred_index, cols, props in all_rows:
            key = _classic_instance_key(cols)
            if not key[1]:
                continue
            sc = score_cohort_row(cols, pred_index)
            grouped.setdefault(key, []).append((sc, pred_index, props))
        for key, scored in grouped.items():
            ext_id = key[1]
            merged = build_merged_props_for_instance(scored, policy_map)
            apply_one(ext_id, merged)
    else:
        for pred_index, cols, props in all_rows:
            ext_id = _first_nonempty(cols.get(EXTERNAL_ID_COLUMN))
            if not ext_id:
                skipped += 1
                continue
            scored = [(score_cohort_row(cols, pred_index), pred_index, props)]
            merged = build_merged_props_for_instance(scored, policy_map)
            apply_one(ext_id, merged)

    flush_updates()

    if log and hasattr(log, "info"):
        log.info(
            "%s classic_save rows_read=%s updates_applied=%s skipped=%s resource_type=%s dry_run=%s fan_in=%s",
            fn_external_id,
            rows_read,
            updates_applied,
            skipped,
            resource_type,
            dry_run,
            fan_mode,
        )

    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "rows_read": rows_read,
        "rows_written": updates_applied,
        "updates_applied": updates_applied,
        "skipped": skipped,
        "run_id": run_id,
        "resource_type": resource_type,
        "dry_run": dry_run,
        "save_fan_in_mode": fan_mode,
        "status": "ok",
        "description": _first_nonempty(cfg.get("description")),
        "predecessor_raw_sources": [{"raw_db": d, "raw_table": t} for d, t in pred_locations],
    }
