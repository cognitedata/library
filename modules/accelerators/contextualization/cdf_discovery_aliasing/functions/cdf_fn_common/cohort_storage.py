"""Per-run, per-canvas-node RAW tables and instance-scoped cohort row keys."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from .incremental_scope import (
    NODE_INSTANCE_ID_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    SCOPE_KEY_COLUMN,
    iter_raw_table_rows_chunked,
    raw_row_columns,
)
from .property_merge import FieldPolicy, build_merged_props_for_instance
from .save_merge import score_cohort_row
from .task_runtime import find_compiled_task

DEFAULT_RAW_DB = "db_discovery"
DEFAULT_RAW_TABLE = "discovery_state"
TABLE_SEGMENT_SEPARATOR = "__"

_MAX_TABLE_SEGMENT_LEN = 80
_MAX_FULL_TABLE_NAME_LEN = 230

_INVALID_TABLE_CHARS = re.compile(r"[^A-Za-z0-9_]+")


def _first_nonempty(*values: Any) -> str:
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""
def require_run_id(data: Mapping[str, Any]) -> str:
    """Return pipeline ``run_id`` from task data or raise."""
    rid = _first_nonempty(data.get("run_id"))
    if not rid:
        raise ValueError("cohort handoff requires non-empty data.run_id")
    return rid


def _sanitize_table_segment(value: str, *, label: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError(f"{label} is required for cohort table name")
    cleaned = _INVALID_TABLE_CHARS.sub("_", raw)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        cleaned = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    if len(cleaned) > _MAX_TABLE_SEGMENT_LEN:
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
        head = cleaned[: _MAX_TABLE_SEGMENT_LEN - 13].rstrip("_")
        cleaned = f"{head}_{digest}"
    return cleaned


def sanitize_run_id_for_table(run_id: str) -> str:
    """CDF-safe run segment for ``{base}__{run}__{node}`` tables."""
    return _sanitize_table_segment(run_id, label="run_id")


def sanitize_canvas_node_id_for_table(canvas_node_id: str) -> str:
    """CDF-safe canvas node segment for per-node cohort tables."""
    return _sanitize_table_segment(canvas_node_id, label="canvas_node_id")


def node_cohort_table_name(base_table: str, run_id: str, canvas_node_id: str) -> str:
    """Ephemeral cohort table for one pipeline run and one canvas node."""
    base = _first_nonempty(base_table) or DEFAULT_RAW_TABLE
    run_seg = sanitize_run_id_for_table(run_id)
    node_seg = sanitize_canvas_node_id_for_table(canvas_node_id)
    name = f"{base}{TABLE_SEGMENT_SEPARATOR}{run_seg}{TABLE_SEGMENT_SEPARATOR}{node_seg}"
    if len(name) <= _MAX_FULL_TABLE_NAME_LEN:
        return name
    digest = hashlib.sha256(f"{run_id}\0{canvas_node_id}".encode("utf-8")).hexdigest()[:16]
    budget = _MAX_FULL_TABLE_NAME_LEN - len(base) - 2 * len(TABLE_SEGMENT_SEPARATOR) - 17
    half = max(8, budget // 2)
    run_head = run_seg[:half].rstrip("_")
    node_head = node_seg[: max(8, budget - len(run_head) - 1)].rstrip("_")
    return (
        f"{base}{TABLE_SEGMENT_SEPARATOR}{run_head}_{digest}"
        f"{TABLE_SEGMENT_SEPARATOR}{node_head}"
    )


def run_node_table_prefix(base_table: str, run_id: str) -> str:
    """Prefix for all node tables in one run: ``discovery_state__{run}__``."""
    base = _first_nonempty(base_table) or DEFAULT_RAW_TABLE
    run_seg = sanitize_run_id_for_table(run_id)
    return f"{base}{TABLE_SEGMENT_SEPARATOR}{run_seg}{TABLE_SEGMENT_SEPARATOR}"


def instance_cohort_row_key(
    node_instance_id: str,
    scope_key: Optional[str] = None,
) -> str:
    """RAW row key: ``{scope_key}:{node_instance_id}`` (run + node implied by table)."""
    nid = str(node_instance_id or "").strip()
    if not nid:
        raise ValueError("node_instance_id is required for cohort row key")
    sk = str(scope_key or "").strip()
    if sk:
        return f"{sk}:{nid}"
    return nid


def canvas_node_id_for_task(data: Mapping[str, Any], task_id: str) -> str:
    """Resolve writer canvas node id for compiled *task_id*."""
    cw = data.get("compiled_workflow")
    task = find_compiled_task(cw, task_id=str(task_id)) if cw else None
    if isinstance(task, dict):
        cn = _first_nonempty(task.get("canvas_node_id"), task.get("pipeline_node_id"))
        if cn:
            return cn
    cn = _first_nonempty(data.get("canvas_node_id"), data.get("pipeline_node_id"))
    if cn:
        return cn
    return sanitize_canvas_node_id_for_table(str(task_id))


def predecessor_canvas_node_ids(data: Mapping[str, Any], task_id: str) -> List[str]:
    """Canvas node ids for direct ``depends_on`` predecessors of *task_id*."""
    cw = data.get("compiled_workflow")
    task = find_compiled_task(cw, task_id=str(task_id)) if cw else None
    deps = task.get("depends_on") if isinstance(task, dict) else []
    if not isinstance(deps, list):
        deps = []
    out: List[str] = []
    seen: set[str] = set()
    for dep in deps:
        ds = str(dep).strip()
        if not ds:
            continue
        pred = find_compiled_task(cw, task_id=ds) if cw else None
        if not isinstance(pred, dict):
            continue
        cn = _first_nonempty(pred.get("canvas_node_id"), pred.get("pipeline_node_id"))
        if not cn:
            cn = sanitize_canvas_node_id_for_table(ds)
        if cn in seen:
            continue
        seen.add(cn)
        out.append(cn)
    return out


def resolve_base_cohort_table(data: Mapping[str, Any]) -> Tuple[str, str]:
    """Return ``(raw_db, base_table_key)`` from task data / scope configuration."""
    persistence = _as_dict(data.get("persistence"))
    cfg = _as_dict(data.get("config"))
    configuration = _as_dict(data.get("configuration")) if hasattr(data, "get") else {}
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
    base_table = _first_nonempty(
        persistence.get("raw_table_key"),
        persistence.get("raw_table"),
        persistence.get("sink_raw_table"),
        cfg.get("raw_table_key"),
        cfg.get("raw_table"),
        cfg.get("sink_raw_table"),
        ke_params.get("raw_table_key"),
        DEFAULT_RAW_TABLE,
    )
    return raw_db, base_table


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def resolve_node_cohort_sink(data: Mapping[str, Any], task_id: str) -> Tuple[str, str]:
    """``(raw_db, node_cohort_table)`` for the writer canvas node of *task_id*."""
    run_id = require_run_id(data)
    raw_db, base_table = resolve_base_cohort_table(data)
    writer = canvas_node_id_for_task(data, task_id)
    return raw_db, node_cohort_table_name(base_table, run_id, writer)


def predecessor_node_table_locations(
    data: Mapping[str, Any], task_id: str
) -> List[Tuple[str, str]]:
    """``(raw_db, table)`` for each direct predecessor canvas node."""
    run_id = require_run_id(data)
    raw_db, base_table = resolve_base_cohort_table(data)
    out: List[Tuple[str, str]] = []
    for cn in predecessor_canvas_node_ids(data, task_id):
        out.append((raw_db, node_cohort_table_name(base_table, run_id, cn)))
    return out


def instance_identity_from_columns(cols: Mapping[str, Any]) -> Tuple[str, str]:
    """``(scope_key, node_instance_id)`` for fan-in grouping."""
    scope_key = _first_nonempty(cols.get(SCOPE_KEY_COLUMN), "default")
    nid = _first_nonempty(cols.get(NODE_INSTANCE_ID_COLUMN))
    return scope_key, nid


def iter_cohort_entity_rows(
    client: Any,
    raw_db: str,
    raw_table: str,
    *,
    chunk_size: int = 2500,
) -> Iterator[Any]:
    """Yield entity cohort rows from a single node table (full scan)."""
    for row in iter_raw_table_rows_chunked(client, raw_db, raw_table, chunk_size=chunk_size):
        cols = raw_row_columns(row)
        if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
            continue
        yield row


TableLocation = Tuple[str, str]


@dataclass(frozen=True)
class CohortRowSnapshot:
    """Entity cohort row payload from one RAW table scan (no per-key retrieve)."""

    columns: Dict[str, Any]
    properties: Dict[str, Any]


CohortRowIndex = Dict[str, CohortRowSnapshot]


def _snapshot_from_cohort_columns(cols: Mapping[str, Any]) -> Optional[CohortRowSnapshot]:
    from .discovery_cohort import _props_from_row_columns

    if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
        return None
    scope_key, nid = instance_identity_from_columns(cols)
    if not nid:
        return None
    body = dict(cols)
    return CohortRowSnapshot(columns=body, properties=_props_from_row_columns(body))


def build_cohort_row_index(
    client: Any,
    raw_db: str,
    raw_table: str,
    *,
    chunk_size: int = 2500,
) -> CohortRowIndex:
    """
    One chunked RAW scan: ``row_key`` -> snapshot for entity cohort rows.

    Last row wins when duplicate keys appear (same as repeated retrieve).
    """
    index: CohortRowIndex = {}
    for row in iter_cohort_entity_rows(client, raw_db, raw_table, chunk_size=chunk_size):
        cols = dict(raw_row_columns(row))
        scope_key, nid = instance_identity_from_columns(cols)
        if not nid:
            continue
        snap = _snapshot_from_cohort_columns(cols)
        if snap is not None:
            index[instance_cohort_row_key(nid, scope_key)] = snap
    return index


def get_or_build_cohort_row_index(
    client: Any,
    raw_db: str,
    raw_table: str,
    *,
    chunk_size: int = 2500,
    index_cache: Any = None,
) -> CohortRowIndex:
    """
    Return a cohort row index, optionally via *index_cache* (local runner).

    When *index_cache* is callable, it is invoked as
    ``index_cache(client, raw_db, raw_table)`` and must return a ``CohortRowIndex``.
    Deployed functions omit the cache and build once per table per invocation.
    """
    if callable(index_cache):
        return index_cache(client, raw_db, raw_table)
    return build_cohort_row_index(client, raw_db, raw_table, chunk_size=chunk_size)


def cohort_row_indexes_for_tables(
    client: Any,
    table_locations: Sequence[TableLocation],
    *,
    chunk_size: int = 2500,
    index_cache: Any = None,
) -> Dict[TableLocation, CohortRowIndex]:
    """Build or reuse indexes for each distinct ``(raw_db, raw_table)``."""
    out: Dict[TableLocation, CohortRowIndex] = {}
    for loc in table_locations:
        if loc in out:
            continue
        db, tbl = loc
        out[loc] = get_or_build_cohort_row_index(
            client, db, tbl, chunk_size=chunk_size, index_cache=index_cache
        )
    return out


def default_fan_in_field_policies() -> Dict[str, FieldPolicy]:
    from .property_merge import MergeListOptions, STRATEGY_MERGE_LIST

    def _pol(prop: str) -> FieldPolicy:
        return FieldPolicy(
            property_name=prop,
            strategy=STRATEGY_MERGE_LIST,
            merge_list=MergeListOptions(unique=True, branch_order="by_score"),
        )

    return {"aliases": _pol("aliases"), "indexKey": _pol("indexKey")}


def _iter_pred_table_snapshots(
    client: Any,
    raw_db: str,
    tbl: str,
    pred_index: int,
    *,
    row_index: Optional[CohortRowIndex] = None,
    chunk_size: int = 2500,
) -> Iterable[Tuple[Dict[str, Any], Dict[str, Any], Tuple[float, str, int]]]:
    """Yield ``(cols, props, score_tuple)`` for each entity in one predecessor table."""
    if row_index is not None:
        for _rk, snap in row_index.items():
            cols = dict(snap.columns)
            sc = score_cohort_row(cols, pred_index)
            yield cols, dict(snap.properties), sc
        return
    for row in iter_cohort_entity_rows(client, raw_db, tbl, chunk_size=chunk_size):
        cols = dict(raw_row_columns(row))
        scope_key, nid = instance_identity_from_columns(cols)
        if not nid:
            continue
        from .discovery_cohort import _props_from_row_columns

        props = _props_from_row_columns(cols)
        sc = score_cohort_row(cols, pred_index)
        yield cols, props, sc


def fan_in_cohort_props_by_instance(
    client: Any,
    raw_db: str,
    base_table: str,
    run_id: str,
    predecessor_canvas_node_ids: Sequence[str],
    *,
    field_policies: Optional[Mapping[str, FieldPolicy]] = None,
    table_indexes: Optional[Mapping[TableLocation, CohortRowIndex]] = None,
    index_cache: Any = None,
    chunk_size: int = 2500,
) -> Iterable[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Merge cohort rows from multiple predecessor node tables per DM instance.

    Yields ``(representative_cols, merged_props)`` once per ``(scope_key, node_instance_id)``.
    """
    from collections import defaultdict

    policy_map = dict(field_policies or default_fan_in_field_policies())
    grouped: Dict[
        Tuple[str, str],
        List[Tuple[Tuple[float, str, int], int, Dict[str, Any], Dict[str, Any]]],
    ] = defaultdict(list)
    pred_list = list(predecessor_canvas_node_ids)
    for pred_index, canvas_nid in enumerate(pred_list):
        tbl = node_cohort_table_name(base_table, run_id, canvas_nid)
        loc: TableLocation = (raw_db, tbl)
        row_index: Optional[CohortRowIndex] = None
        if table_indexes is not None:
            row_index = table_indexes.get(loc)
        elif callable(index_cache):
            row_index = get_or_build_cohort_row_index(
                client, raw_db, tbl, chunk_size=chunk_size, index_cache=index_cache
            )
        for cols, props, sc in _iter_pred_table_snapshots(
            client,
            raw_db,
            tbl,
            pred_index,
            row_index=row_index,
            chunk_size=chunk_size,
        ):
            scope_key, nid = instance_identity_from_columns(cols)
            if not nid:
                continue
            grouped[(scope_key, nid)].append((sc, pred_index, props, cols))

    for (_scope, _nid), scored in grouped.items():
        if not scored:
            continue
        rep_cols = scored[0][3]
        merged = build_merged_props_for_instance(
            [(s, p, pr) for s, p, pr, _ in scored],
            policy_map,
        )
        if merged:
            yield rep_cols, merged


def list_run_cohort_tables(
    client: Any,
    raw_db: str,
    run_id: str,
    *,
    base_table: str = DEFAULT_RAW_TABLE,
) -> List[str]:
    """Table names matching ``{base}__{sanitized_run_id}__*``."""
    prefix = run_node_table_prefix(base_table, run_id)
    try:
        tables = client.raw.tables.list(raw_db, limit=-1)
    except Exception:
        return []
    out: List[str] = []
    for tbl in tables:
        name = str(getattr(tbl, "name", None) or getattr(tbl, "table", "") or "").strip()
        if name.startswith(prefix):
            out.append(name)
    return sorted(out)
