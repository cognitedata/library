"""Shared discovery query utilities: task config, RAW sink resolution, cohort row shape, flush.

Cohort rows use a dedicated RAW column ``CONFIDENCE`` (JSON array of floats, parallel to the
task ``value_field`` list) so ``{value_field}_confidence`` is not stored in ``PROPERTIES_JSON``.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple

from cdf_fn_common.etl_cohort_storage import (
    canvas_node_id_for_task,
    instance_cohort_row_key,
    resolve_node_cohort_sink,
)
from cdf_fn_common.etl_cdf_utils import create_table_if_not_exists
from cdf_fn_common.etl_incremental_scope import (
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
)
from cdf_fn_common.etl_confidence_property import confidence_property_key
from cdf_fn_common.etl_raw_upload import RawRowsUploadQueue

QUERY_SOURCE_COLUMN = "QUERY_SOURCE"
QUERY_TASK_ID_COLUMN = "QUERY_TASK_ID"
ENTITY_TYPE_COLUMN = "ENTITY_TYPE"
PROPERTIES_JSON_COLUMN = "PROPERTIES_JSON"
CONFIDENCE_COLUMN = "CONFIDENCE"
VIEW_SPACE_COLUMN = "VIEW_SPACE"
VIEW_EXTERNAL_ID_COLUMN = "VIEW_EXTERNAL_ID"
VIEW_VERSION_COLUMN = "VIEW_VERSION"
INSTANCE_SPACE_COLUMN = "INSTANCE_SPACE"

_UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")


def instance_space_from_node_instance_id(node_instance_id: str) -> str:
    """Parse DM instance space from ``{space}:{uuid}`` node keys."""
    nid = str(node_instance_id or "").strip()
    if ":" not in nid:
        return ""
    head, _, tail = nid.partition(":")
    head = head.strip()
    tail = tail.strip()
    if head and tail:
        return head
    return ""


def cohort_instance_space_and_external_id(
    cols: Mapping[str, Any],
    *,
    cfg: Optional[Mapping[str, Any]] = None,
    data: Optional[Mapping[str, Any]] = None,
    props: Optional[Mapping[str, Any]] = None,
) -> Tuple[str, str]:
    """Resolve instance identity from cohort row columns, with config/pipeline fallback."""
    ext_id = _first_nonempty(cols.get(EXTERNAL_ID_COLUMN))
    fallback_space = _first_nonempty(
        (cfg or {}).get("instance_space"),
        (data or {}).get("instance_space"),
    )
    props_space = _first_nonempty(
        props.get("instance_space") if isinstance(props, Mapping) else None,
        props.get("space") if isinstance(props, Mapping) else None,
    )
    col_space = _first_nonempty(cols.get(INSTANCE_SPACE_COLUMN))
    nid = str(cols.get(NODE_INSTANCE_ID_COLUMN) or "").strip()
    nid_space = instance_space_from_node_instance_id(nid)
    if nid and ":" in nid:
        head, _, tail = nid.partition(":")
        head = head.strip()
        tail = tail.strip()
        if head and _UUID_RE.match(tail):
            return _first_nonempty(col_space, head, props_space, nid_space, fallback_space), ext_id
    return _first_nonempty(col_space, nid_space, props_space, fallback_space), ext_id


DEFAULT_RAW_DB = "db_discovery"
DEFAULT_RAW_TABLE = "discovery_state"


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


def resolve_query_sink(data: Mapping[str, Any]) -> Tuple[str, str]:
    """Return ``(raw_db, writer_node_cohort_table)`` for inter-node cohort handoff."""
    task_id = _first_nonempty(data.get("task_id"))
    if not task_id:
        raise ValueError("cohort handoff requires non-empty data.task_id")
    return resolve_node_cohort_sink(data, task_id)


def resolve_raw_save_sink(cfg: Mapping[str, Any]) -> Tuple[str, str]:
    """Return configured RAW destination for ``save_raw`` (not ephemeral cohort tables)."""
    raw_db = _first_nonempty(
        cfg.get("source_raw_db"),
        cfg.get("raw_db"),
        cfg.get("sink_raw_db"),
    )
    raw_table = _first_nonempty(
        cfg.get("source_raw_table"),
        cfg.get("source_raw_table_key"),
        cfg.get("raw_table"),
        cfg.get("raw_table_key"),
        cfg.get("sink_raw_table"),
        cfg.get("sink_raw_table_key"),
    )
    if not raw_db or not raw_table:
        raise ValueError(
            "save_raw requires config.source_raw_db and source_raw_table_key "
            "(or raw_db/raw_table_key)"
        )
    return raw_db, raw_table


def resolve_inverted_index_sink(data: Mapping[str, Any]) -> Tuple[str, str]:
    """Return ``(raw_db, inverted_index_raw_table)`` for ``fn_dm_inverted_index`` writes."""
    from cdf_fn_common.etl_inverted_index_naming import inverted_index_raw_table_from_key_extraction_table

    persistence = _as_dict(data.get("persistence"))
    cfg = resolve_task_config(data)
    configuration = _as_dict(data.get("configuration"))
    ke_params = _as_dict(
        _as_dict(_as_dict(configuration.get("key_extraction")).get("config")).get("parameters")
    )
    source_table = _first_nonempty(
        persistence.get("raw_table_key"),
        persistence.get("raw_table"),
        persistence.get("sink_raw_table"),
        cfg.get("source_raw_table_key"),
        cfg.get("raw_table_key"),
        cfg.get("raw_table"),
        ke_params.get("raw_table_key"),
        DEFAULT_RAW_TABLE,
    )
    default_inv = inverted_index_raw_table_from_key_extraction_table(str(source_table))
    raw_db = _first_nonempty(
        persistence.get("raw_db"),
        persistence.get("sink_raw_db"),
        persistence.get("inverted_index_raw_db"),
        cfg.get("raw_db"),
        cfg.get("sink_raw_db"),
        cfg.get("inverted_index_raw_db"),
        ke_params.get("raw_db"),
        DEFAULT_RAW_DB,
    )
    raw_table = _first_nonempty(
        persistence.get("inverted_index_raw_table"),
        persistence.get("inverted_index_raw_table_key"),
        cfg.get("inverted_index_raw_table"),
        cfg.get("inverted_index_raw_table_key"),
        ke_params.get("inverted_index_raw_table_key"),
        default_inv,
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
    *,
    value_field: str = "aliases",
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Remove ``{value_field}_confidence`` from the JSON blob and return scores for ``CONFIDENCE``.

    Also strips top-level ``confidence`` and any other ``*_confidence`` keys from props.
    """
    props = dict(properties)
    props.pop("confidence", None)
    score_key = confidence_property_key(value_field)
    conf = props.pop(score_key, None)
    for k in list(props.keys()):
        if k.endswith("_confidence"):
            props.pop(k, None)
    if conf is None:
        ik = props.get("indexKey")
        if isinstance(ik, list) and ik and isinstance(ik[0], dict):
            conf_list: List[float] = []
            for item in ik:
                if isinstance(item, dict):
                    try:
                        conf_list.append(float(item.get("confidence", 0)))
                    except (TypeError, ValueError):
                        conf_list.append(0.0)
            if conf_list:
                conf = conf_list
    return props, _serialize_confidence_column(conf)


def parse_confidence_column_value(raw: Any) -> Optional[List[float]]:
    """Parse RAW ``CONFIDENCE`` cell into floats (parallel to ``indexKey`` strings)."""
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
    cols: Mapping[str, Any],
    props: MutableMapping[str, Any],
    *,
    value_field: str = "aliases",
) -> None:
    """If ``CONFIDENCE`` is set on the RAW row, copy into ``props`` as ``{value_field}_confidence``."""
    lst = parse_confidence_column_value(cols.get(CONFIDENCE_COLUMN))
    if lst is not None:
        props[confidence_property_key(value_field)] = lst


def build_entity_cohort_row(
    *,
    run_id: str,
    scope_key: str,
    canvas_node_id: str,
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
    value_field: str = "aliases",
) -> Dict[str, Any]:
    props_body, conf_cell = split_properties_and_confidence_column(
        properties, value_field=value_field
    )
    inst_space_col = instance_space_from_node_instance_id(node_instance_id)
    if not inst_space_col and isinstance(properties, Mapping):
        inst_space_col = _first_nonempty(
            properties.get("instance_space"),
            properties.get("space"),
        )
    cols: Dict[str, Any] = {
        RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
        WORKFLOW_STATUS_COLUMN: WORKFLOW_STATUS_DETECTED,
        WORKFLOW_STATUS_UPDATED_AT_COLUMN: datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"
        ),
        RUN_ID_COLUMN: run_id,
        SCOPE_KEY_COLUMN: scope_key,
        NODE_INSTANCE_ID_COLUMN: node_instance_id,
        INSTANCE_SPACE_COLUMN: inst_space_col,
        RAW_ROW_KEY_COLUMN: instance_cohort_row_key(node_instance_id, scope_key),
        EXTERNAL_ID_COLUMN: external_id,
        ENTITY_TYPE_COLUMN: entity_type,
        VIEW_SPACE_COLUMN: view_space,
        VIEW_EXTERNAL_ID_COLUMN: view_external_id,
        VIEW_VERSION_COLUMN: view_version,
        QUERY_SOURCE_COLUMN: query_source,
        QUERY_TASK_ID_COLUMN: canvas_node_id,
        PROPERTIES_JSON_COLUMN: json.dumps(props_body, default=str, sort_keys=True),
    }
    if conf_cell is not None:
        cols[CONFIDENCE_COLUMN] = conf_cell
    if last_updated_ms is not None:
        cols["LAST_UPDATED_TIME_MS"] = int(last_updated_ms)
    if extraction_inputs_hash and str(extraction_inputs_hash).strip():
        cols[EXTRACTION_INPUTS_HASH_COLUMN] = str(extraction_inputs_hash).strip()
    row_key = instance_cohort_row_key(node_instance_id, scope_key)
    return {"key": row_key, "columns": cols}


def _flush_rows(
    queue: RawRowsUploadQueue,
    raw_db: str,
    raw_table: str,
    rows: List[Dict[str, Any]],
    *,
    client: Any = None,
) -> None:
    if rows and client is not None:
        create_table_if_not_exists(client, raw_db, raw_table)
    for row in rows:
        queue.add_to_upload_queue(database=raw_db, table=raw_table, raw_row=row)
    rows.clear()
    if queue.upload_queue_size:
        queue.upload()
