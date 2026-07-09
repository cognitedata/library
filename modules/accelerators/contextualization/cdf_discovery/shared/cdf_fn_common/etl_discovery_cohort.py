"""Shared cohort RAW helpers: predecessor node tables and cohort row rebuild."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Optional, Tuple

from cdf_fn_common.etl_cohort_storage import (
    TableLocation,
    cohort_row_indexes_for_tables,
    fan_in_cohort_props_by_instance,
    get_or_build_cohort_row_index,
    iter_cohort_entity_rows,
    node_cohort_table_name,
    predecessor_canvas_node_ids,
    predecessor_node_table_locations,
    require_pipeline_run_key,
    resolve_base_cohort_table,
    sanitize_canvas_node_id_for_table,
)
from cdf_fn_common.etl_discovery_query_shared import (
    ENTITY_TYPE_COLUMN,
    EXTERNAL_ID_COLUMN,
    EXTRACTION_INPUTS_HASH_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    PROPERTIES_JSON_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    SCOPE_KEY_COLUMN,
    VIEW_EXTERNAL_ID_COLUMN,
    VIEW_SPACE_COLUMN,
    VIEW_VERSION_COLUMN,
    build_entity_cohort_row,
    merge_confidence_column_into_properties,
    _first_nonempty,
)
from cdf_fn_common.etl_task_runtime import find_compiled_task


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


def raw_sink_for_dependency_task(
    data: Mapping[str, Any], dep_task_id: str
) -> Optional[Tuple[str, str]]:
    """Resolve ``(raw_db, node_table)`` for a single predecessor task id (join wiring)."""
    ds = str(dep_task_id).strip()
    if not ds:
        return None
    cw = data.get("compiled_workflow")
    pred = find_compiled_task(cw, task_id=ds) if cw else None
    if isinstance(pred, dict):
        cn = _first_nonempty(pred.get("canvas_node_id"), pred.get("pipeline_node_id"))
    else:
        cn = ""
    if not cn:
        cn = sanitize_canvas_node_id_for_table(ds)
    run_id = require_pipeline_run_key(data)
    raw_db, base_table = resolve_base_cohort_table(data)
    return raw_db, node_cohort_table_name(base_table, run_id, cn)


def iter_predecessor_raw_locations(
    data: Mapping[str, Any], task_id: str
) -> List[Tuple[str, str]]:
    """``(raw_db, node_table)`` for each direct predecessor canvas node."""
    return predecessor_node_table_locations(data, task_id)


def iter_predecessor_cohort_rows(
    client: Any,
    data: Mapping[str, Any],
    task_id: str,
) -> Any:
    """Yield entity cohort rows from all direct predecessor node tables."""
    for raw_db, raw_table in predecessor_node_table_locations(data, task_id):
        yield from iter_cohort_entity_rows(client, raw_db, raw_table)


def iter_predecessor_instance_props(
    client: Any,
    data: Mapping[str, Any],
    task_id: str,
) -> Any:
    """
    Yield ``(cols, props)`` once per DM instance from predecessor node tables.

    When multiple predecessors exist, property dicts are merged per instance (fan-in).
    """
    pred_nodes = predecessor_canvas_node_ids(data, task_id)
    if not pred_nodes:
        return
    raw_db, base_table = resolve_base_cohort_table(data)
    run_id = require_pipeline_run_key(data)
    index_cache = data.get("etl_cohort_row_index_cache") if hasattr(data, "get") else None
    if len(pred_nodes) > 1:
        table_indexes = cohort_row_indexes_for_tables(
            client,
            [
                (raw_db, node_cohort_table_name(base_table, run_id, cn))
                for cn in pred_nodes
            ],
            index_cache=index_cache,
        )
        yield from fan_in_cohort_props_by_instance(
            client,
            raw_db,
            base_table,
            run_id,
            pred_nodes,
            table_indexes=table_indexes,
            index_cache=index_cache,
        )
        return
    locs = predecessor_node_table_locations(data, task_id)
    if len(locs) == 1:
        raw_db_one, tbl = locs[0]
        row_index = get_or_build_cohort_row_index(
            client, raw_db_one, tbl, index_cache=index_cache
        )
        for _rk, snap in row_index.items():
            cols = dict(snap.columns)
            if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
                continue
            yield cols, dict(snap.properties)
        return
    for row in iter_predecessor_cohort_rows(client, data, task_id):
        cols = dict(getattr(row, "columns", None) or {})
        if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
            continue
        yield cols, _props_from_row_columns(cols)


def _cohort_row_from_columns(
    *,
    cols: Mapping[str, Any],
    row_key: str,
    run_id: str,
    canvas_node_id: str,
    properties: Mapping[str, Any],
    query_source: str = "transform",
    value_field: str = "aliases",
) -> Dict[str, Any]:
    nid = _first_nonempty(cols.get(NODE_INSTANCE_ID_COLUMN), row_key)
    ext_id = _first_nonempty(cols.get(EXTERNAL_ID_COLUMN), nid)
    scope_key = _first_nonempty(cols.get(SCOPE_KEY_COLUMN), "transform")
    last_updated_ms: Optional[int] = None
    raw_lu = cols.get("LAST_UPDATED_TIME_MS")
    if raw_lu is not None:
        try:
            last_updated_ms = int(raw_lu)
        except (TypeError, ValueError):
            last_updated_ms = None
    inputs_hash = cols.get(EXTRACTION_INPUTS_HASH_COLUMN)
    if inputs_hash is not None and not str(inputs_hash).strip():
        inputs_hash = None
    return build_entity_cohort_row(
        run_id=run_id,
        scope_key=scope_key,
        canvas_node_id=canvas_node_id,
        query_source=query_source,
        node_instance_id=str(nid or ext_id or row_key),
        external_id=str(ext_id or nid or row_key),
        entity_type=_first_nonempty(cols.get(ENTITY_TYPE_COLUMN), "entity"),
        view_space=_first_nonempty(cols.get(VIEW_SPACE_COLUMN)),
        view_external_id=_first_nonempty(cols.get(VIEW_EXTERNAL_ID_COLUMN), "transform"),
        view_version=_first_nonempty(cols.get(VIEW_VERSION_COLUMN)),
        properties=properties,
        last_updated_ms=last_updated_ms,
        extraction_inputs_hash=str(inputs_hash).strip() if inputs_hash is not None else None,
        value_field=value_field,
    )
