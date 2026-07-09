"""Cohort RAW row shape for Records stream query/save handoff."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from cdf_fn_common.etl_common import _first_nonempty
from cdf_fn_common.etl_discovery_query_shared import (
    CONFIDENCE_COLUMN,
    ENTITY_TYPE_COLUMN,
    EXTERNAL_ID_COLUMN,
    INSTANCE_SPACE_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    PROPERTIES_JSON_COLUMN,
    QUERY_SOURCE_COLUMN,
    QUERY_TASK_ID_COLUMN,
    RECORD_KIND_COLUMN,
    RUN_ID_COLUMN,
    SCOPE_KEY_COLUMN,
    VIEW_EXTERNAL_ID_COLUMN,
    VIEW_SPACE_COLUMN,
    VIEW_VERSION_COLUMN,
    build_entity_cohort_row,
    split_properties_and_confidence_column,
)
from cdf_fn_common.etl_incremental_scope import RECORD_KIND_RECORD  # noqa: F401 — re-export for callers

QUERY_SOURCE_RECORDS = "records"

STREAM_EXTERNAL_ID_COLUMN = "STREAM_EXTERNAL_ID"
RECORD_SPACE_COLUMN = "RECORD_SPACE"
RECORD_SOURCES_JSON_COLUMN = "RECORD_SOURCES_JSON"


def parse_record_sources_json(raw: Any) -> List[Dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [dict(x) for x in raw if isinstance(x, dict)]
    if isinstance(raw, str) and raw.strip():
        try:
            v = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if isinstance(v, list):
            return [dict(x) for x in v if isinstance(x, dict)]
        if isinstance(v, dict):
            return [v]
    if isinstance(raw, dict):
        return [dict(raw)]
    return []


def record_node_instance_id(record_space: str, external_id: str, *, stream_external_id: str = "") -> str:
    space = str(record_space or "").strip()
    ext = str(external_id or "").strip()
    if space and ext:
        return f"{space}:{ext}"
    if ext:
        return ext
    if space:
        return space
    return str(stream_external_id or "").strip() or "record:unknown"


def build_record_cohort_row(
    *,
    run_id: str,
    scope_key: str,
    canvas_node_id: str,
    stream_external_id: str,
    record_space: str,
    external_id: str,
    properties: Mapping[str, Any],
    sources: Optional[List[Mapping[str, Any]]] = None,
    entity_type: str = "record",
    value_field: str = "aliases",
    confidence: Any = None,
) -> Dict[str, Any]:
    """Build a RAW cohort row dict for ``RECORD_KIND=record``."""
    props_in = dict(properties)
    if confidence is not None:
        from cdf_fn_common.etl_confidence_property import confidence_property_key

        props_in[confidence_property_key(value_field)] = confidence
    props_clean, conf_col = split_properties_and_confidence_column(props_in, value_field=value_field)
    sources_list = list(sources) if sources else parse_record_sources_json(props_clean.pop("record_sources", None))
    sources_json = json.dumps(sources_list, ensure_ascii=False) if sources_list else None
    nid = record_node_instance_id(record_space, external_id, stream_external_id=stream_external_id)
    wrapped = build_entity_cohort_row(
        run_id=run_id,
        scope_key=scope_key,
        canvas_node_id=canvas_node_id,
        query_source=QUERY_SOURCE_RECORDS,
        node_instance_id=nid,
        external_id=external_id,
        entity_type=entity_type,
        view_space="",
        view_external_id="",
        view_version="",
        properties=props_clean,
        value_field=value_field,
    )
    cols = wrapped.get("columns") if isinstance(wrapped.get("columns"), dict) else {}
    cols[RECORD_KIND_COLUMN] = RECORD_KIND_RECORD
    cols[STREAM_EXTERNAL_ID_COLUMN] = stream_external_id
    cols[RECORD_SPACE_COLUMN] = record_space
    if sources_json:
        cols[RECORD_SOURCES_JSON_COLUMN] = sources_json
    if conf_col:
        cols[CONFIDENCE_COLUMN] = conf_col
    return wrapped


def write_record_rows_to_cohort_sink(
    client: Any,
    data: MutableMapping[str, Any],
    *,
    run_id: str,
    scope_key: str,
    task_id: str,
    stream_external_id: str,
    rows: List[Dict[str, Any]],
    log: Any = None,
) -> Dict[str, Any]:
    from cdf_fn_common.etl_cohort_storage import canvas_node_id_for_task
    from cdf_fn_common.etl_discovery_query_shared import _flush_rows, resolve_query_sink
    from cdf_fn_common.etl_raw_upload import RawRowsUploadQueue
    from cdf_fn_common.etl_ui_progress import (
        COHORT_WRITE_ROW_INTERVAL,
        emit_cohort_write_progress_complete,
        emit_cohort_write_progress_every_n_rows,
        set_cohort_write_progress_total,
    )

    if client is None:
        raise ValueError("cohort handoff requires a CDF client")
    raw_db, raw_table = resolve_query_sink(data)
    canvas_node_id = canvas_node_id_for_task(data, task_id)
    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    n_written = 0
    cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
    entity_type = _first_nonempty(cfg.get("entity_type"), "record")
    value_field = str(cfg.get("value_field") or "aliases")
    set_cohort_write_progress_total(sum(1 for item in rows if isinstance(item, dict)))

    for item in rows:
        if not isinstance(item, dict):
            continue
        cols = dict(item.get("columns") or {})
        props = dict(item.get("properties") or {})
        rec = item.get("record") if isinstance(item.get("record"), dict) else {}
        space = _first_nonempty(
            cols.get("record_space"),
            rec.get("space"),
            props.get("space"),
        )
        ext_id = _first_nonempty(cols.get("external_id"), rec.get("externalId"), rec.get("external_id"))
        sources = None
        if isinstance(rec.get("sources"), list):
            sources = rec.get("sources")
        elif isinstance(props.get("record_sources"), list):
            sources = props.get("record_sources")
        pending.append(
            build_record_cohort_row(
                run_id=run_id,
                scope_key=scope_key,
                canvas_node_id=canvas_node_id,
                stream_external_id=stream_external_id,
                record_space=space,
                external_id=ext_id,
                properties=props,
                sources=sources,
                entity_type=entity_type,
                value_field=value_field,
            )
        )
        n_written += 1
        if len(pending) >= COHORT_WRITE_ROW_INTERVAL:
            _flush_rows(queue, raw_db, raw_table, pending, client=client)
            emit_cohort_write_progress_every_n_rows(n_written)

    _flush_rows(queue, raw_db, raw_table, pending, client=client)
    emit_cohort_write_progress_complete(n_written)
    if log and hasattr(log, "info"):
        log.info(
            "record cohort handoff task=%s wrote=%s sink=%s/%s stream=%s",
            task_id,
            n_written,
            raw_db,
            raw_table,
            stream_external_id,
        )
    return {
        "rows_written": n_written,
        "raw_db": raw_db,
        "raw_table": raw_table,
        "predecessor_mode": "cohort",
        "stream_external_id": stream_external_id,
    }


def maybe_handoff_record_rows(
    client: Any,
    data: MutableMapping[str, Any],
    *,
    run_id: str,
    scope_key: str,
    task_id: str,
    stream_external_id: str,
    rows: List[Dict[str, Any]],
    log: Any = None,
) -> Optional[Dict[str, Any]]:
    from cdf_fn_common.etl_predecessor_mode import use_in_memory_predecessors

    cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
    if use_in_memory_predecessors(data, cfg):
        data["_predecessor_rows"] = rows
        return None
    summary = write_record_rows_to_cohort_sink(
        client,
        data,
        run_id=run_id,
        scope_key=scope_key,
        task_id=task_id,
        stream_external_id=stream_external_id,
        rows=rows,
        log=log,
    )
    data.pop("_predecessor_rows", None)
    return summary


def record_props_from_cohort_columns(cols: Mapping[str, Any]) -> Dict[str, Any]:
    from cdf_fn_common.etl_discovery_query_shared import parse_raw_row_properties, merge_confidence_column_into_properties

    props = parse_raw_row_properties(cols)
    merge_confidence_column_into_properties(cols, props)
    sources = parse_record_sources_json(cols.get(RECORD_SOURCES_JSON_COLUMN))
    if sources:
        props["record_sources"] = sources
    return props
