"""Discovery save stages: DM view apply, RAW cohort replication, classic resource updates.

Save tasks require ``save_fan_in_mode`` (``none`` | ``merge_per_instance``) and optional
``save_field_policies`` for per-field ``merge_list`` vs default ``tie_break``.
Legacy keys ``write_back_fields``, ``write_properties``, ``include_properties`` are rejected.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple

from cognite.client.data_classes import AssetUpdate, FileMetadataUpdate, TimeSeriesUpdate
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData
from cognite.client.data_classes.data_modeling.ids import ViewId

from .discovery_cohort import _props_from_row_columns, iter_predecessor_raw_locations
from .discovery_query_shared import (
    ENTITY_TYPE_COLUMN,
    EXTERNAL_ID_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    SCOPE_KEY_COLUMN,
    VIEW_EXTERNAL_ID_COLUMN,
    VIEW_SPACE_COLUMN,
    VIEW_VERSION_COLUMN,
    build_entity_cohort_row,
    resolve_query_sink,
    resolve_run_id,
    resolve_task_config,
    _first_nonempty,
    _flush_rows,
)
from .incremental_scope import iter_inter_node_raw_rows_for_filter_run
from .raw_upload import RawRowsUploadQueue
from .save_merge import (
    SAVE_FAN_IN_MERGE,
    build_merged_props_for_instance,
    filter_props_internal,
    parse_field_policies,
    validate_save_config,
    score_cohort_row,
)
from .task_runtime import merge_compiled_task_into_data

_UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")

_INTERNAL_PROP_KEYS = frozenset(
    {
        "raw_columns",
        "_variant_index",
    }
)


def _instance_space_and_external_id(
    cols: Mapping[str, Any],
    *,
    cfg: Mapping[str, Any],
    data: Mapping[str, Any],
) -> Tuple[str, str]:
    ext_id = _first_nonempty(cols.get(EXTERNAL_ID_COLUMN))
    cfg_space = _first_nonempty(cfg.get("instance_space"), data.get("instance_space"))
    nid = str(cols.get(NODE_INSTANCE_ID_COLUMN) or "").strip()
    if nid and ":" in nid:
        head, tail = nid.split(":", 1)
        tail = tail.strip()
        if _UUID_RE.match(tail):
            return _first_nonempty(cfg_space, head), ext_id
    return cfg_space, ext_id


def _classic_instance_key(cols: Mapping[str, Any]) -> Tuple[str, str]:
    ext_id = _first_nonempty(cols.get(EXTERNAL_ID_COLUMN))
    return ("", ext_id)


def _iter_entity_rows_for_save(
    client: Any,
    data: Mapping[str, Any],
    task_id: str,
    filter_run: str,
) -> List[Tuple[int, Dict[str, Any], Dict[str, Any]]]:
    """Return list of (pred_index, cols, props_filtered) for entity cohort rows."""
    pred_locations = iter_predecessor_raw_locations(data, task_id)
    out: List[Tuple[int, Dict[str, Any], Dict[str, Any]]] = []
    for pred_index, (source_db, source_table) in enumerate(pred_locations):
        for row in iter_inter_node_raw_rows_for_filter_run(
            client, source_db, source_table, filter_run or ""
        ):
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
) -> Dict[Tuple[str, str], List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]]:
    from collections import defaultdict

    acc: DefaultDict[
        Tuple[str, str], List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]
    ] = defaultdict(list)
    for pred_index, cols, props in rows:
        inst_space, ext_id = _instance_space_and_external_id(cols, cfg=cfg, data=data)
        if not ext_id or not inst_space:
            continue
        sc = score_cohort_row(cols, pred_index)
        acc[(inst_space, ext_id)].append((sc, pred_index, props))
    return dict(acc)


def _gather_classic_rows_by_instance(
    rows: List[Tuple[int, Dict[str, Any], Dict[str, Any]]],
) -> Dict[Tuple[str, str], List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]]:
    from collections import defaultdict

    acc: DefaultDict[
        Tuple[str, str], List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]
    ] = defaultdict(list)
    for pred_index, cols, props in rows:
        key = _classic_instance_key(cols)
        if not key[1]:
            continue
        sc = score_cohort_row(cols, pred_index)
        acc[key].append((sc, pred_index, props))
    return dict(acc)


def discovery_apply_view_save(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    validate_save_config(cfg, save_kind="view")

    view_space = _first_nonempty(cfg.get("view_space"), "cdf_cdm")
    view_external_id = _first_nonempty(cfg.get("view_external_id"))
    view_version = _first_nonempty(cfg.get("view_version"), "v1")
    if not view_external_id:
        raise ValueError("save_view requires config.view_external_id")

    view_id = ViewId(space=view_space, external_id=view_external_id, version=view_version)
    run_id = resolve_run_id(data)
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    filter_run = _first_nonempty(cfg.get("filter_run_id"), run_id)
    dry_run = bool(data.get("dry_run") or cfg.get("dry_run"))
    fan_mode = str(cfg.get("save_fan_in_mode") or "").strip()
    policy_map = parse_field_policies(cfg)

    pred_locations = iter_predecessor_raw_locations(data, task_id)
    rows_read = 0
    instances_applied = 0
    skipped = 0
    batch: List[NodeApply] = []
    batch_size = int(cfg.get("apply_batch_size") or 50)

    all_rows = _iter_entity_rows_for_save(client, data, task_id, filter_run or "")
    rows_read = len(all_rows)

    def emit_apply(inst_space: str, ext_id: str, props: Mapping[str, Any]) -> None:
        nonlocal instances_applied, skipped, batch
        if not props:
            skipped += 1
            return
        batch.append(
            NodeApply(
                space=inst_space,
                external_id=ext_id,
                sources=[NodeOrEdgeData(view_id, dict(props))],
            )
        )
        if len(batch) >= batch_size:
            if not dry_run:
                client.data_modeling.instances.apply(nodes=batch)
            instances_applied += len(batch)
            batch.clear()

    if fan_mode == SAVE_FAN_IN_MERGE:
        grouped = _gather_view_rows_by_instance(all_rows, cfg=cfg, data=data)
        for (inst_space, ext_id), scored in grouped.items():
            merged = build_merged_props_for_instance(scored, policy_map)
            emit_apply(inst_space, ext_id, merged)
    else:
        for _pred_index, cols, props in all_rows:
            inst_space, ext_id = _instance_space_and_external_id(cols, cfg=cfg, data=data)
            if not ext_id or not inst_space:
                skipped += 1
                continue
            scored = [(score_cohort_row(cols, _pred_index), _pred_index, props)]
            merged = build_merged_props_for_instance(scored, policy_map)
            emit_apply(inst_space, ext_id, merged)

    if batch:
        if not dry_run:
            client.data_modeling.instances.apply(nodes=batch)
        instances_applied += len(batch)

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
        "instances_applied": instances_applied,
        "skipped": skipped,
        "run_id": run_id,
        "view_space": view_space,
        "view_external_id": view_external_id,
        "view_version": view_version,
        "dry_run": dry_run,
        "save_fan_in_mode": fan_mode,
        "predecessor_raw_sources": [{"raw_db": d, "raw_table": t} for d, t in pred_locations],
    }
    if policy_map:
        summary["save_field_policies_count"] = len(policy_map)
    data["run_id"] = run_id
    return summary


def discovery_replicate_raw_save(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    """Copy entity cohort rows from predecessor RAW tables into this task's query sink."""
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    validate_save_config(cfg, save_kind="raw")

    run_id = resolve_run_id(data)
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    sink_db, sink_table = resolve_query_sink(data)
    filter_run = _first_nonempty(cfg.get("filter_run_id"), run_id)
    fan_mode = str(cfg.get("save_fan_in_mode") or "").strip()
    policy_map = parse_field_policies(cfg)

    pred_locations = iter_predecessor_raw_locations(data, task_id)
    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    rows_read = 0
    rows_written = 0

    all_rows = _iter_entity_rows_for_save(client, data, task_id, filter_run or "")
    rows_read = len(all_rows)

    def flush_one(
        cols: Mapping[str, Any],
        props: Mapping[str, Any],
    ) -> None:
        nonlocal rows_written, pending
        scope_key = _first_nonempty(cols.get(SCOPE_KEY_COLUMN), "raw_save")
        nid = _first_nonempty(cols.get(NODE_INSTANCE_ID_COLUMN), row_key)
        ext_id = _first_nonempty(cols.get(EXTERNAL_ID_COLUMN), nid)
        pending.append(
            build_entity_cohort_row(
                run_id=run_id,
                scope_key=scope_key,
                task_id=task_id,
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
        if len(pending) >= 500:
            _flush_rows(queue, sink_db, sink_table, pending)

    if fan_mode == SAVE_FAN_IN_MERGE:
        grouped: Dict[Tuple[str, str], List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]] = {}
        cols_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for pred_index, cols, props in all_rows:
            inst_space, ext_id = _instance_space_and_external_id(cols, cfg=cfg, data=data)
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
            inst_space, ext_id = _instance_space_and_external_id(cols, cfg=cfg, data=data)
            if not ext_id or not inst_space:
                continue
            scored = [(score_cohort_row(cols, pred_index), pred_index, props)]
            merged = build_merged_props_for_instance(scored, policy_map)
            flush_one(cols, merged)

    _flush_rows(queue, sink_db, sink_table, pending)

    if log and hasattr(log, "info"):
        log.info(
            "%s raw_save rows_read=%s rows_written=%s sink=%s/%s fan_in=%s",
            fn_external_id,
            rows_read,
            rows_written,
            sink_db,
            sink_table,
            fan_mode,
        )

    summary = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
        "save_fan_in_mode": fan_mode,
        "predecessor_raw_sources": [{"raw_db": d, "raw_table": t} for d, t in pred_locations],
    }
    if policy_map:
        summary["save_field_policies_count"] = len(policy_map)
    data["run_id"] = run_id
    return summary


def _coerce_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def _classic_build_update(
    resource_type: str, external_id: str, props: Mapping[str, Any]
) -> Optional[Any]:
    """Build an Asset/File/TimeSeries update, or None when there is nothing to write."""
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
        if name is not None:
            u.name.set(name)
            changed = True
        if desc is not None:
            u.description.set(desc)
            changed = True
        if meta is not None:
            u.metadata.set(meta)
            changed = True
        if src is not None:
            u.source.set(src)
            changed = True
        return u if changed else None

    if rt == "file":
        u = FileMetadataUpdate(external_id=external_id)
        changed = False
        if name is not None:
            u.name.set(name)
            changed = True
        if desc is not None:
            u.description.set(desc)
            changed = True
        if meta is not None:
            u.metadata.set(meta)
            changed = True
        if src is not None:
            u.source.set(src)
            changed = True
        return u if changed else None

    if rt in ("timeseries", "time_series"):
        u = TimeSeriesUpdate(external_id=external_id)
        changed = False
        if name is not None:
            u.name.set(name)
            changed = True
        if desc is not None:
            u.description.set(desc)
            changed = True
        unit = _coerce_str(props.get("unit"))
        if unit is not None:
            u.unit.set(unit)
            changed = True
        return u if changed else None

    raise ValueError(
        f"classic_save unsupported resource_type={resource_type!r} "
        f"(supported: assets, files, time_series)"
    )


def _classic_issue_update(client: Any, upd: Any) -> None:
    if isinstance(upd, AssetUpdate):
        client.assets.update(upd)
    elif isinstance(upd, FileMetadataUpdate):
        client.files.update(upd)
    elif isinstance(upd, TimeSeriesUpdate):
        client.time_series.update(upd)


def discovery_apply_classic_save(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    validate_save_config(cfg, save_kind="classic")

    resource_type = _first_nonempty(cfg.get("resource_type"), cfg.get("classic_resource_type"), "assets")
    run_id = resolve_run_id(data)
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    filter_run = _first_nonempty(cfg.get("filter_run_id"), run_id)
    dry_run = bool(data.get("dry_run") or cfg.get("dry_run"))
    fan_mode = str(cfg.get("save_fan_in_mode") or "").strip()
    policy_map = parse_field_policies(cfg)

    pred_locations = iter_predecessor_raw_locations(data, task_id)
    rows_read = 0
    updates_applied = 0
    skipped = 0

    all_rows = _iter_entity_rows_for_save(client, data, task_id, filter_run or "")
    rows_read = len(all_rows)

    def apply_one(ext_id: str, props: Mapping[str, Any]) -> None:
        nonlocal updates_applied, skipped
        try:
            upd = _classic_build_update(resource_type, ext_id, props)
            if upd is None:
                skipped += 1
                return
            if not dry_run:
                _classic_issue_update(client, upd)
            updates_applied += 1
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
        grouped: Dict[Tuple[str, str], List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]] = {}
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

    summary = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "rows_read": rows_read,
        "updates_applied": updates_applied,
        "skipped": skipped,
        "run_id": run_id,
        "resource_type": resource_type,
        "dry_run": dry_run,
        "save_fan_in_mode": fan_mode,
        "predecessor_raw_sources": [{"raw_db": d, "raw_table": t} for d, t in pred_locations],
    }
    if policy_map:
        summary["save_field_policies_count"] = len(policy_map)
    data["run_id"] = run_id
    return summary


def run_discovery_save_with_status(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    impl: Any,
) -> Dict[str, Any]:
    """Run a save *impl* (summary dict), set ``data`` status on success, return Cognite-style response."""
    from .function_logging import resolve_function_logger

    log: Any = None
    try:
        log = resolve_function_logger(data, None)
        if not client:
            raise ValueError("CogniteClient is required")
        summary = impl(fn_external_id, data, client, log)
        msg = json.dumps(summary, default=str)
        data["status"] = "succeeded"
        data["message"] = msg
        if log and hasattr(log, "info"):
            log.info("%s complete", fn_external_id)
        return {"status": "succeeded", "message": msg}
    except Exception as ex:
        message = f"{fn_external_id} failed: {ex!s}"
        if log and hasattr(log, "error"):
            log.error(message)
        return {"status": "failure", "message": message}
