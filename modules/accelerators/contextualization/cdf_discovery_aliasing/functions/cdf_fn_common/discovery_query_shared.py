"""Shared discovery query utilities: task config, RAW sink resolution, cohort row shape, flush.

Cohort rows use a dedicated RAW column ``CONFIDENCE`` (JSON array of floats, parallel to
``discoveredKey`` string list) so scores are not only embedded in ``PROPERTIES_JSON``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple

from .incremental_scope import (
    EXTERNAL_ID_COLUMN,
    EXTRACTION_INPUTS_HASH_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    RAW_ROW_KEY_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RUN_ID_COLUMN,
    SCOPE_KEY_COLUMN,
    WORKFLOW_STATUS_COLUMN,
    WORKFLOW_STATUS_DETECTED,
    WORKFLOW_STATUS_UPDATED_AT_COLUMN,
    cohort_row_key,
)
from .raw_upload import RawRowsUploadQueue

QUERY_SOURCE_COLUMN = "QUERY_SOURCE"
QUERY_TASK_ID_COLUMN = "QUERY_TASK_ID"
ENTITY_TYPE_COLUMN = "ENTITY_TYPE"
PROPERTIES_JSON_COLUMN = "PROPERTIES_JSON"
CONFIDENCE_COLUMN = "CONFIDENCE"
VIEW_SPACE_COLUMN = "VIEW_SPACE"
VIEW_EXTERNAL_ID_COLUMN = "VIEW_EXTERNAL_ID"
VIEW_VERSION_COLUMN = "VIEW_VERSION"

DEFAULT_RAW_DB = "db_discovery"
DEFAULT_RAW_TABLE = "discovery_state"


def _utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _first_nonempty(*values: Any) -> str:
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def resolve_task_config(data: Mapping[str, Any]) -> Dict[str, Any]:
    """Task ``config`` from inlined IR payload (``data.config``)."""
    cfg = data.get("config")
    return _as_dict(cfg)


def resolve_run_id(data: Mapping[str, Any]) -> str:
    rid = _first_nonempty(data.get("run_id"))
    return rid or _utc_run_id()


def resolve_query_sink(data: Mapping[str, Any]) -> Tuple[str, str]:
    """Return ``(raw_db, raw_table)`` for discovery query cohort writes."""
    persistence = _as_dict(data.get("persistence"))
    cfg = resolve_task_config(data)
    configuration = _as_dict(data.get("configuration"))
    ke_params = _as_dict(
        _as_dict(_as_dict(configuration.get("key_extraction")).get("config")).get("parameters")
    )
    raw_db = _first_nonempty(
        persistence.get("raw_db"),
        persistence.get("sink_raw_db"),
        cfg.get("raw_db"),
        cfg.get("sink_raw_db"),
        ke_params.get("raw_db"),
        DEFAULT_RAW_DB,
    )
    raw_table = _first_nonempty(
        persistence.get("raw_table_key"),
        persistence.get("raw_table"),
        persistence.get("sink_raw_table"),
        cfg.get("raw_table_key"),
        cfg.get("raw_table"),
        cfg.get("sink_raw_table"),
        ke_params.get("raw_table_key"),
        DEFAULT_RAW_TABLE,
    )
    return raw_db, raw_table


def _serialize_confidence_column(conf: Any) -> Optional[str]:
    """Serialize per-key scores for the dedicated RAW ``CONFIDENCE`` column (JSON array string)."""
    if conf is None:
        return None
    if isinstance(conf, list):
        nums: List[float] = []
        for x in conf:
            try:
                nums.append(float(x))
            except (TypeError, ValueError):
                return None
        if not nums:
            return None
        return json.dumps(nums)
    try:
        f = float(conf)
    except (TypeError, ValueError):
        return None
    return json.dumps([f])


def split_properties_and_confidence_column(
    properties: Mapping[str, Any],
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Remove confidence scores from the JSON blob and return them for ``CONFIDENCE``.

    Uses top-level ``confidence``, or derives floats from ``discoveredKey`` when it is a list
    of ``{value, confidence}`` objects. Drops legacy ``aliases_confidence`` / ``discoveredKey_confidence``
    keys so they are not persisted inside ``PROPERTIES_JSON``.
    """
    props = dict(properties)
    props.pop("discoveredKey_confidence", None)
    props.pop("aliases_confidence", None)
    conf = props.pop("confidence", None)
    if conf is None:
        dk = props.get("discoveredKey")
        if isinstance(dk, list) and dk and isinstance(dk[0], dict):
            conf_list: List[float] = []
            for item in dk:
                if isinstance(item, dict):
                    try:
                        conf_list.append(float(item.get("confidence", 0)))
                    except (TypeError, ValueError):
                        conf_list.append(0.0)
            if conf_list:
                conf = conf_list
    return props, _serialize_confidence_column(conf)


def parse_confidence_column_value(raw: Any) -> Optional[List[float]]:
    """Parse RAW ``CONFIDENCE`` cell into floats (parallel to ``discoveredKey`` strings)."""
    if raw is None:
        return None
    if isinstance(raw, list):
        out: List[float] = []
        for x in raw:
            try:
                out.append(float(x))
            except (TypeError, ValueError):
                return None
        return out if out else None
    if isinstance(raw, (int, float)):
        return [float(raw)]
    s = str(raw).strip()
    if not s:
        return None
    if s.startswith("["):
        try:
            v = json.loads(s)
        except json.JSONDecodeError:
            return None
        if isinstance(v, list):
            return parse_confidence_column_value(v)
        return None
    try:
        return [float(s)]
    except ValueError:
        return None


def merge_confidence_column_into_properties(
    cols: Mapping[str, Any], props: MutableMapping[str, Any]
) -> None:
    """If ``CONFIDENCE`` is set on the RAW row, copy it into ``props`` as ``confidence``."""
    lst = parse_confidence_column_value(cols.get(CONFIDENCE_COLUMN))
    if lst is not None:
        props["confidence"] = lst


def build_entity_cohort_row(
    *,
    run_id: str,
    scope_key: str,
    task_id: str,
    query_source: str,
    node_instance_id: str,
    external_id: str,
    entity_type: str,
    view_space: str,
    view_external_id: str,
    view_version: str,
    properties: Mapping[str, Any],
    last_updated_ms: Optional[int] = None,
    extraction_inputs_hash: Optional[str] = None,
) -> Dict[str, Any]:
    props_body, conf_cell = split_properties_and_confidence_column(properties)
    cols: Dict[str, Any] = {
        RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
        WORKFLOW_STATUS_COLUMN: WORKFLOW_STATUS_DETECTED,
        WORKFLOW_STATUS_UPDATED_AT_COLUMN: datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"
        ),
        RUN_ID_COLUMN: run_id,
        SCOPE_KEY_COLUMN: scope_key,
        NODE_INSTANCE_ID_COLUMN: node_instance_id,
        RAW_ROW_KEY_COLUMN: cohort_row_key(run_id, node_instance_id, scope_key),
        EXTERNAL_ID_COLUMN: external_id,
        ENTITY_TYPE_COLUMN: entity_type,
        VIEW_SPACE_COLUMN: view_space,
        VIEW_EXTERNAL_ID_COLUMN: view_external_id,
        VIEW_VERSION_COLUMN: view_version,
        QUERY_SOURCE_COLUMN: query_source,
        QUERY_TASK_ID_COLUMN: task_id,
        PROPERTIES_JSON_COLUMN: json.dumps(props_body, default=str, sort_keys=True),
    }
    if conf_cell is not None:
        cols[CONFIDENCE_COLUMN] = conf_cell
    if last_updated_ms is not None:
        cols["LAST_UPDATED_TIME_MS"] = int(last_updated_ms)
    if extraction_inputs_hash and str(extraction_inputs_hash).strip():
        cols[EXTRACTION_INPUTS_HASH_COLUMN] = str(extraction_inputs_hash).strip()
    return {"key": cohort_row_key(run_id, node_instance_id, scope_key), "columns": cols}


def _flush_rows(queue: RawRowsUploadQueue, raw_db: str, raw_table: str, rows: List[Dict[str, Any]]) -> None:
    for row in rows:
        queue.add_to_upload_queue(database=raw_db, table=raw_table, raw_row=row)
    rows.clear()
    if queue.upload_queue_size:
        queue.upload()
