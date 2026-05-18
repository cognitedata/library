"""Shared cohort RAW helpers: predecessor sink resolution and cohort row rebuild."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .discovery_query_shared import (
    ENTITY_TYPE_COLUMN,
    EXTERNAL_ID_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    PROPERTIES_JSON_COLUMN,
    SCOPE_KEY_COLUMN,
    VIEW_EXTERNAL_ID_COLUMN,
    VIEW_SPACE_COLUMN,
    VIEW_VERSION_COLUMN,
    build_entity_cohort_row,
    merge_confidence_column_into_properties,
    _as_dict,
    _first_nonempty,
)
from .task_runtime import find_compiled_task


def _parse_pred_summary(pred_snap: Any) -> Dict[str, Any]:
    if not isinstance(pred_snap, dict):
        return {}
    msg = pred_snap.get("message")
    if isinstance(msg, str) and msg.strip():
        try:
            parsed = json.loads(msg)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return pred_snap


def _pred_raw_location(
    dep_task_id: str,
    *,
    pred_outputs: Mapping[str, Any],
    compiled_workflow: Any,
) -> Optional[Tuple[str, str]]:
    summary = _parse_pred_summary(pred_outputs.get(dep_task_id))
    db = _first_nonempty(summary.get("raw_db"), summary.get("sink_raw_db"))
    table = _first_nonempty(
        summary.get("raw_table"),
        summary.get("raw_table_key"),
        summary.get("sink_raw_table"),
    )
    if db and table:
        return db, table
    task = find_compiled_task(compiled_workflow, task_id=dep_task_id) if compiled_workflow else None
    if isinstance(task, dict):
        pers = _as_dict(task.get("persistence"))
        db = _first_nonempty(pers.get("raw_db"), pers.get("sink_raw_db"))
        table = _first_nonempty(
            pers.get("raw_table_key"),
            pers.get("raw_table"),
            pers.get("sink_raw_table"),
        )
        if db and table:
            return db, table
    return None


def raw_sink_for_dependency_task(
    data: Mapping[str, Any], dep_task_id: str
) -> Optional[Tuple[str, str]]:
    """Resolve ``(raw_db, raw_table)`` for a single predecessor task id (join / explicit wiring)."""
    cw = data.get("compiled_workflow")
    pred_outputs = data.get("discovery_predecessor_outputs")
    if not isinstance(pred_outputs, dict):
        pred_outputs = {}
    ds = str(dep_task_id).strip()
    if not ds:
        return None
    return _pred_raw_location(ds, pred_outputs=pred_outputs, compiled_workflow=cw)


def iter_predecessor_raw_locations(
    data: Mapping[str, Any], task_id: str
) -> List[Tuple[str, str]]:
    cw = data.get("compiled_workflow")
    task = find_compiled_task(cw, task_id=str(task_id)) if cw else None
    deps = task.get("depends_on") if isinstance(task, dict) else []
    if not isinstance(deps, list):
        deps = []
    pred_outputs = data.get("discovery_predecessor_outputs")
    if not isinstance(pred_outputs, dict):
        pred_outputs = {}
    seen: set[Tuple[str, str]] = set()
    out: List[Tuple[str, str]] = []
    for dep in deps:
        ds = str(dep).strip()
        if not ds:
            continue
        loc = _pred_raw_location(ds, pred_outputs=pred_outputs, compiled_workflow=cw)
        if loc and loc not in seen:
            seen.add(loc)
            out.append(loc)
    return out


def _props_from_row_columns(cols: Mapping[str, Any]) -> Dict[str, Any]:
    props_raw = cols.get(PROPERTIES_JSON_COLUMN)
    if isinstance(props_raw, str) and props_raw.strip():
        try:
            parsed = json.loads(props_raw)
            props: Dict[str, Any] = dict(parsed) if isinstance(parsed, dict) else {"raw_columns": dict(cols)}
        except json.JSONDecodeError:
            props = {"raw_columns": dict(cols)}
    else:
        props = {"raw_columns": dict(cols)}
    merge_confidence_column_into_properties(cols, props)
    return props


def _cohort_row_from_columns(
    *,
    cols: Mapping[str, Any],
    row_key: str,
    run_id: str,
    task_id: str,
    properties: Mapping[str, Any],
    query_source: str = "transform",
    value_field: str = "aliases",
) -> Dict[str, Any]:
    nid = _first_nonempty(cols.get(NODE_INSTANCE_ID_COLUMN), row_key)
    ext_id = _first_nonempty(cols.get(EXTERNAL_ID_COLUMN), nid)
    scope_key = _first_nonempty(cols.get(SCOPE_KEY_COLUMN), "transform")
    return build_entity_cohort_row(
        run_id=run_id,
        scope_key=scope_key,
        task_id=task_id,
        query_source=query_source,
        node_instance_id=str(nid or ext_id or row_key),
        external_id=str(ext_id or nid or row_key),
        entity_type=_first_nonempty(cols.get(ENTITY_TYPE_COLUMN), "entity"),
        view_space=_first_nonempty(cols.get(VIEW_SPACE_COLUMN)),
        view_external_id=_first_nonempty(cols.get(VIEW_EXTERNAL_ID_COLUMN), "transform"),
        view_version=_first_nonempty(cols.get(VIEW_VERSION_COLUMN)),
        properties=properties,
        value_field=value_field,
    )
