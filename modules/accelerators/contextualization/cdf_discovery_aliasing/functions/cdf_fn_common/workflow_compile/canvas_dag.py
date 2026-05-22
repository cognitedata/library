"""Compile ``compiled_workflow`` IR from a UI canvas (arbitrary DAG over executable node kinds).

``subgraph`` nodes are flattened before compile: ``inner_canvas`` nodes and edges are promoted to
the parent graph, and edges that targeted or sourced the subgraph frame are rewired to
``subflow_hub_input_id`` / ``subflow_hub_output_id`` (same ``in__`` / ``out__`` handle ids).
"""

from __future__ import annotations

import copy
import hashlib
import re
from typing import Any, Dict, FrozenSet, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

COMPILED_WORKFLOW_SCHEMA_VERSION = 1

# Canvas node kinds that never become Cognite function tasks.
STRUCTURAL_KINDS: FrozenSet[str] = frozenset(
    {
        "start",
        "end",
        "source_view",
        "subflow_graph_in",
        "subflow_graph_out",
    }
)

PASS_THROUGH_KINDS: FrozenSet[str] = frozenset(
    {
        "match_validation_source_view",
    }
)

_KIND_FN: Dict[str, Tuple[str, str]] = {
    "save_view": ("fn_dm_view_save", "save_view"),
    "save_raw": ("fn_dm_raw_save", "save_raw"),
    "save_classic": ("fn_dm_classic_save", "save_classic"),
    "query_view": ("fn_dm_view_query", "query_view"),
    "query_raw": ("fn_dm_raw_query", "query_raw"),
    "query_classic": ("fn_dm_classic_query", "query_classic"),
    "query_sql": ("fn_dm_sql_query", "query_sql"),
    "transform": ("fn_dm_transform", "transform"),
    "merge": ("fn_dm_merge", "merge"),
    "join": ("fn_dm_join", "join"),
    "validation": ("fn_dm_validate", "validate"),
    "instance_filter": ("fn_dm_filter", "instance_filter"),
    "confidence_filter": ("fn_dm_confidence_filter", "confidence_filter"),
    "inverted_index": ("fn_dm_inverted_index", "inverted_index"),
    "discovery_raw_cleanup": ("fn_dm_discovery_raw_cleanup", "discovery_raw_cleanup"),
}


def discovery_local_pipeline_specs() -> Dict[str, Tuple[str, str]]:
    """
    Map Cognite ``function_external_id`` → (``importlib`` module path, pipeline entry function).

    Built from :data:`_KIND_FN` so the local Kahn runner stays aligned with canvas compile: add a
    canvas executable kind there and the same ``fn_dm_*`` task can be executed under ``module.py run``.
    """
    out: Dict[str, Tuple[str, str]] = {}
    for _kind, (fn_ext, exec_kind) in _KIND_FN.items():
        if fn_ext not in out:
            out[fn_ext] = (f"{fn_ext}.pipeline", exec_kind)
    return out


# Executable canvas kinds that read stage settings from ``node.data.config``.
_DISCOVERY_KINDS_WITH_CONFIG: FrozenSet[str] = frozenset(
    {
        "query_view",
        "query_raw",
        "query_classic",
        "query_sql",
        "transform",
        "merge",
        "join",
        "validation",
        "instance_filter",
        "confidence_filter",
        "inverted_index",
        "save_view",
        "save_raw",
        "save_classic",
        "discovery_raw_cleanup",
    }
)

# Join node inputs: explicit handles (aligned with ``SUBFLOW_PORT_HANDLE_IN_PREFIX`` + port id).
JOIN_TARGET_HANDLE_LEFT = "in__left"
JOIN_TARGET_HANDLE_RIGHT = "in__right"

_JOIN_INPUT_SOURCE_KINDS: FrozenSet[str] = frozenset(
    {
        "query_view",
        "query_raw",
        "query_classic",
        "query_sql",
        "transform",
        "validation",
        "instance_filter",
        "confidence_filter",
        "join",
        "merge",
    }
)


def discovery_stage_inline_nonempty(kind: str, value: Any) -> bool:
    """True when *value* is a non-empty ``data.config`` object for discovery *kind*."""
    if kind == "discovery_raw_cleanup":
        return isinstance(value, dict)
    if not isinstance(value, dict) or not value:
        return False
    if kind in ("query_view", "save_view"):
        return bool(str(value.get("view_external_id") or "").strip())
    if kind == "query_sql":
        return bool(
            str(value.get("sql_query") or value.get("query") or "").strip()
        )
    if kind in ("query_raw", "query_classic", "save_raw", "save_classic"):
        return bool(
            str(value.get("view_external_id") or "").strip()
            or str(value.get("description") or "").strip()
            or str(value.get("raw_db") or "").strip()
            or str(value.get("raw_table_key") or "").strip()
            or str(value.get("raw_table") or "").strip()
        )
    if kind in ("transform", "validation", "merge"):
        return bool(str(value.get("description") or "").strip())
    if kind == "instance_filter":
        if not bool(str(value.get("description") or "").strip()):
            return False
        fl = value.get("filters")
        return isinstance(fl, list) and bool(fl)
    if kind == "confidence_filter":
        return bool(str(value.get("description") or "").strip())
    if kind == "join":
        if not str(value.get("description") or "").strip():
            return False
        jo = value.get("join_on")
        return isinstance(jo, dict) and bool(jo)
    if kind == "inverted_index":
        kinds = value.get("index_kinds")
        return isinstance(kinds, dict) and bool(kinds)
    return False


def _discovery_payload_fragment(kind: str, data: Mapping[str, Any]) -> Dict[str, Any]:
    """``node.data.config`` merged into compiled task ``payload`` as ``payload.config``."""
    if kind not in _DISCOVERY_KINDS_WITH_CONFIG:
        return {}
    cfg = data.get("config")
    if kind == "discovery_raw_cleanup":
        base = cfg if isinstance(cfg, dict) else {}
        return {"config": copy.deepcopy(base)}
    if discovery_stage_inline_nonempty(kind, cfg):
        return {"config": copy.deepcopy(cfg)}
    return {}


class CanvasCompileError(ValueError):
    """Invalid canvas for workflow compilation."""


def _compile_dag_mode(doc: Mapping[str, Any]) -> None:
    """Validate ``compile_workflow_dag``; only ``canvas`` or omitted is allowed."""
    raw = doc.get("compile_workflow_dag")
    if raw is None:
        return
    s = str(raw).strip().lower()
    if s == "canvas":
        return
    if s == "legacy":
        raise ValueError(
            "compile_workflow_dag value 'legacy' is not supported; use 'canvas' or omit the key."
        )
    if s == "auto":
        raise ValueError(
            'compile_workflow_dag: use "canvas" or omit the key ("auto" is not supported).'
        )
    raise ValueError(f"Unsupported compile_workflow_dag value: {raw!r}")


def _canvas_doc(doc: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    c = doc.get("canvas")
    return c if isinstance(c, dict) else None


def _has_executable_canvas(canvas: Mapping[str, Any]) -> bool:
    nodes = canvas.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        return False
    for n in nodes:
        if not isinstance(n, dict):
            continue
        if not _is_canvas_node_enabled(n):
            continue
        k = str(n.get("kind") or "").strip()
        if k in _KIND_FN:
            return True
        if k == "subgraph":
            data = n.get("data") if isinstance(n.get("data"), dict) else {}
            inner = data.get("inner_canvas")
            if isinstance(inner, dict) and _has_executable_canvas(inner):
                return True
    return False


def _flatten_canvas_subgraphs(canvas: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Inline ``subgraph`` nodes: append ``inner_canvas`` nodes/edges and rewire parent edges
    to ``subflow_hub_output_id`` / ``subflow_hub_input_id`` (same ``in__`` / ``out__`` handles).
    """
    base = copy.deepcopy(dict(canvas))
    nodes_raw = base.get("nodes")
    edges_raw = base.get("edges")
    if not isinstance(nodes_raw, list):
        return base
    if not isinstance(edges_raw, list):
        edges_raw = []
        base["edges"] = edges_raw

    flat_nodes: List[Dict[str, Any]] = []
    inner_edges_accum: List[Dict[str, Any]] = []
    rewrites: List[Tuple[str, str, str]] = []

    for n in nodes_raw:
        if not isinstance(n, dict):
            continue
        if _kind(n) == "subgraph":
            if not _is_canvas_node_enabled(n):
                flat_nodes.append(copy.deepcopy(n))
                continue
            nid = str(n.get("id") or "").strip()
            if not nid:
                raise CanvasCompileError("Canvas subgraph node missing id")
            data = n.get("data") if isinstance(n.get("data"), dict) else {}
            inner = data.get("inner_canvas")
            if not isinstance(inner, dict):
                raise CanvasCompileError(
                    f"Canvas subgraph node {nid!r} missing data.inner_canvas "
                    "(open the subgraph in the UI and save)."
                )
            hub_in = str(data.get("subflow_hub_input_id") or "").strip()
            hub_out = str(data.get("subflow_hub_output_id") or "").strip()
            if not hub_in or not hub_out:
                raise CanvasCompileError(
                    f"Canvas subgraph node {nid!r} missing subflow_hub_input_id "
                    "or subflow_hub_output_id in data."
                )
            inner_flat = _flatten_canvas_subgraphs(inner)
            inn = inner_flat.get("nodes")
            ine = inner_flat.get("edges")
            if not isinstance(inn, list) or not inn:
                raise CanvasCompileError(
                    f"Canvas subgraph {nid!r} has no inner nodes (inner_canvas.nodes)."
                )
            if not isinstance(ine, list):
                ine = []
            flat_nodes.extend(inn)
            inner_edges_accum.extend(ine)
            rewrites.append((nid, hub_in, hub_out))
        else:
            flat_nodes.append(copy.deepcopy(n))

    flat_edges: List[Dict[str, Any]] = []
    for e in edges_raw:
        if not isinstance(e, dict):
            continue
        e2 = copy.deepcopy(e)
        s = str(e2.get("source") or "").strip()
        t = str(e2.get("target") or "").strip()
        for sg_id, hi, ho in rewrites:
            if s == sg_id:
                e2["source"] = ho
            if t == sg_id:
                e2["target"] = hi
        flat_edges.append(e2)
    flat_edges.extend(inner_edges_accum)

    out = {k: copy.deepcopy(v) for k, v in base.items() if k not in ("nodes", "edges")}
    out["nodes"] = flat_nodes
    out["edges"] = flat_edges
    return out


def flatten_subgraphs_in_canvas(canvas: Mapping[str, Any]) -> Dict[str, Any]:
    """Public entry: flatten ``subgraph`` nodes for deploy payloads or tooling."""
    return _flatten_canvas_subgraphs(copy.deepcopy(dict(canvas)))


def sanitize_task_id(node_id: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(node_id or "").strip())
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "node"
    return f"kea__{s}"


# Cognite workflow task externalId: keep conservative for logs, YAML, and API limits.
_TASK_ID_MAX_LEN = 120


def _slug_token(text: str) -> str:
    """Single URL-ish token: letters, digits, underscore, hyphen."""
    s = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(text or "").strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "x"


def canvas_node_label(data: Mapping[str, Any]) -> str:
    """Operator label from canvas node ``data.label`` (used for CDF workflow task ``name`` at build)."""
    return str(data.get("label") or "").strip()


def _name_source_for_task_id(kind: str, canvas_node_id: str, data: Mapping[str, Any]) -> str:
    """
    Human-oriented label for ``kea__<executor_kind>__<this>`` (before slugging).

    Priority matches the human-readable task id plan: description, view id, raw fields,
    persistence hints, UI label, then canvas node id.
    """
    cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
    desc = str(cfg.get("description") or "").strip()
    if desc:
        return desc
    if kind in ("query_view", "save_view"):
        ve = str(cfg.get("view_external_id") or "").strip()
        if ve:
            vv = str(cfg.get("view_version") or "").strip()
            return f"{ve}_{vv}" if vv else ve
    if kind == "query_sql":
        sq = str(cfg.get("sql_query") or cfg.get("query") or "").strip()
        if sq:
            return sq[:48]
    if kind in ("query_raw", "query_classic", "save_raw", "save_classic"):
        for key in ("description", "raw_table_key", "raw_table", "view_external_id", "raw_db"):
            v = str(cfg.get(key) or "").strip()
            if v:
                return v
    if kind == "inverted_index":
        inv_desc = str(cfg.get("description") or "").strip()
        if inv_desc:
            return inv_desc
    pc = data.get("persistence_config") if isinstance(data.get("persistence_config"), dict) else {}
    for key in ("description", "inverted_index_raw_table", "source_raw_table_key", "raw_table_key"):
        v = str(pc.get(key) or "").strip()
        if v:
            return v
    label = canvas_node_label(data)
    if label:
        return label
    return canvas_node_id


def _apply_task_id_max_length(task_id: str, canvas_node_id: str, *, max_len: int = _TASK_ID_MAX_LEN) -> str:
    if len(task_id) <= max_len:
        return task_id
    digest = hashlib.sha256(canvas_node_id.encode("utf-8")).hexdigest()[:8]
    suffix = "__" + digest
    if len(suffix) >= max_len:
        return ("kea__" + digest)[:max_len]
    head = task_id[: max_len - len(suffix)].rstrip("_")
    if not head.startswith("kea__"):
        head = "kea__" + digest
    return (head + suffix)[:max_len]


def human_readable_task_id_candidate(
    canvas_node_id: str,
    canvas_kind: str,
    executor_kind: str,
    node_data: Mapping[str, Any],
) -> str:
    """
    Compiled task id for one executable canvas node, before global collision disambiguation.

    Shape: ``kea__<executor_kind>__<name_slug>`` with length cap and hash tail if truncated.
    """
    exec_s = _slug_token(executor_kind)
    name_src = _name_source_for_task_id(canvas_kind, canvas_node_id, node_data)
    name_s = _slug_token(name_src)
    if name_s == "x":
        name_s = _slug_token(canvas_node_id)
    raw = f"kea__{exec_s}__{name_s}"
    return _apply_task_id_max_length(raw, canvas_node_id)


def _resolve_executable_task_ids(
    rows: Sequence[Tuple[str, str, str, Mapping[str, Any]]],
) -> Dict[str, str]:
    """
    Map canvas node id -> final compiled task id.

    ``rows`` are ``(canvas_node_id, canvas_kind, executor_kind, node_data)`` in graph iteration order.
    """
    preliminary = {
        nid: human_readable_task_id_candidate(nid, k, ek, data) for nid, k, ek, data in rows
    }
    used: Set[str] = set()
    out: Dict[str, str] = {}
    for nid in sorted(preliminary.keys(), key=lambda x: (preliminary[x], x)):
        cand = preliminary[nid]
        final = cand
        salt = 0
        while final in used:
            hx = hashlib.sha256(f"{nid}:{salt}".encode("utf-8")).hexdigest()[:8]
            final = _apply_task_id_max_length(f"{cand}__{hx}", nid)
            salt += 1
            if salt > 500:
                raise CanvasCompileError(f"Unable to assign unique task id near canvas node {nid!r}")
        used.add(final)
        out[nid] = final
    return out


def _node_by_id(nodes: Sequence[Mapping[str, Any]]) -> Dict[str, Mapping[str, Any]]:
    out: Dict[str, Mapping[str, Any]] = {}
    for n in nodes:
        if not isinstance(n, dict):
            continue
        nid = str(n.get("id") or "").strip()
        if nid:
            out[nid] = n
    return out


def _kind(n: Mapping[str, Any]) -> str:
    return str(n.get("kind") or "").strip()


def _is_canvas_node_enabled(n: Mapping[str, Any]) -> bool:
    """True when the canvas node participates in workflow compile (default enabled)."""
    return n.get("enabled", True) is not False


def _is_disabled_pass_through(n: Mapping[str, Any]) -> bool:
    """Disabled executable or disabled subgraph — walk upstream when resolving deps."""
    k = _kind(n)
    if k == "subgraph" and not _is_canvas_node_enabled(n):
        return True
    if k in _KIND_FN and not _is_canvas_node_enabled(n):
        return True
    return False


def _resolve_enabled_executable_canvas_id(
    canvas_id: str,
    *,
    by_id: Mapping[str, Mapping[str, Any]],
    rev_adj: Mapping[str, List[str]],
    executable_ids: Set[str],
) -> Optional[str]:
    """Nearest enabled executable canvas node upstream of ``canvas_id`` (BFS)."""
    stack = [canvas_id]
    visited: Set[str] = set()
    while stack:
        cur = stack.pop()
        if cur in visited:
            continue
        visited.add(cur)
        if cur in executable_ids:
            return cur
        if cur not in by_id:
            continue
        n = by_id[cur]
        k = _kind(n)
        if k in PASS_THROUGH_KINDS | STRUCTURAL_KINDS:
            stack.extend(rev_adj.get(cur, []))
            continue
        if _is_disabled_pass_through(n):
            stack.extend(rev_adj.get(cur, []))
            continue
    return None


def _join_input_task_ids_from_edges(
    join_canvas_id: str,
    edges_raw: Sequence[Mapping[str, Any]],
    *,
    task_id_by_canvas: Mapping[str, str],
    by_id: Mapping[str, Mapping[str, Any]],
    rev_adj: Mapping[str, List[str]],
    executable_ids: Set[str],
) -> Tuple[str, str]:
    """
    Resolve compiled task ids for join ``in__left`` / ``in__right`` inputs.

    Each side must have exactly one incoming edge from an allowed discovery cohort producer,
    with matching ``target_handle``.
    """
    left_tid: Optional[str] = None
    right_tid: Optional[str] = None
    left_src: Optional[str] = None
    right_src: Optional[str] = None

    for e in edges_raw:
        if not isinstance(e, dict):
            continue
        if str(e.get("target") or "").strip() != join_canvas_id:
            continue
        src = str(e.get("source") or "").strip()
        if not src:
            continue
        pred = by_id.get(src)
        if pred is None:
            continue
        pk = _kind(pred)
        if pk not in _JOIN_INPUT_SOURCE_KINDS:
            raise CanvasCompileError(
                f"join node {join_canvas_id!r}: predecessor {src!r} kind={pk!r} is not allowed "
                f"as join input (expected one of: {sorted(_JOIN_INPUT_SOURCE_KINDS)})"
            )
        th = str(e.get("target_handle") or "").strip()
        exec_src = _resolve_enabled_executable_canvas_id(
            src,
            by_id=by_id,
            rev_adj=rev_adj,
            executable_ids=executable_ids,
        )
        if not exec_src:
            raise CanvasCompileError(
                f"join node {join_canvas_id!r}: predecessor {src!r} has no enabled executable "
                "upstream (enable the stage or rewire the join input)"
            )
        tid = task_id_by_canvas.get(exec_src)
        if not tid:
            raise CanvasCompileError(
                f"join node {join_canvas_id!r}: predecessor {src!r} is not an executable canvas node"
            )
        if th == JOIN_TARGET_HANDLE_LEFT:
            if left_tid is not None:
                raise CanvasCompileError(
                    f"join node {join_canvas_id!r}: multiple edges target {JOIN_TARGET_HANDLE_LEFT!r}"
                )
            left_tid = tid
            left_src = exec_src
        elif th == JOIN_TARGET_HANDLE_RIGHT:
            if right_tid is not None:
                raise CanvasCompileError(
                    f"join node {join_canvas_id!r}: multiple edges target {JOIN_TARGET_HANDLE_RIGHT!r}"
                )
            right_tid = tid
            right_src = exec_src
        else:
            raise CanvasCompileError(
                f"join node {join_canvas_id!r}: edge from {src!r} must use "
                f"target_handle {JOIN_TARGET_HANDLE_LEFT!r} or {JOIN_TARGET_HANDLE_RIGHT!r}, "
                f"got {th!r}"
            )

    if left_tid is None or right_tid is None:
        raise CanvasCompileError(
            f"join node {join_canvas_id!r}: require exactly one edge to {JOIN_TARGET_HANDLE_LEFT!r} "
            f"and one to {JOIN_TARGET_HANDLE_RIGHT!r} (from cohort-producing stages)"
        )
    if left_src == right_src:
        raise CanvasCompileError(
            f"join node {join_canvas_id!r}: left and right inputs must be different nodes"
        )
    return left_tid, right_tid


def _edge_kind(edge: Mapping[str, Any]) -> str:
    k = str(edge.get("kind") or "data").strip().lower()
    if k in ("sequence", "parallel_group"):
        return k
    return "data"


def _build_rev_adj(
    edges_raw: Sequence[Mapping[str, Any]],
    *,
    edge_kinds: Optional[FrozenSet[str]] = None,
) -> Dict[str, List[str]]:
    """Reverse adjacency: target -> [sources]. Optional filter by edge kind."""
    rev: Dict[str, List[str]] = {}
    for e in edges_raw:
        if not isinstance(e, dict):
            continue
        ek = _edge_kind(e)
        if edge_kinds is not None and ek not in edge_kinds:
            continue
        s = str(e.get("source") or "").strip()
        t = str(e.get("target") or "").strip()
        if s and t:
            rev.setdefault(t, []).append(s)
            rev.setdefault(s, rev.get(s, []))
    return rev


def _immediate_executable_predecessors(
    start_id: str,
    *,
    rev_adj: Mapping[str, List[str]],
    executable_ids: Set[str],
) -> Set[str]:
    """Executable canvas nodes with a direct edge into ``start_id`` (per rev_adj filter)."""
    return {p for p in rev_adj.get(start_id, []) if p in executable_ids}


def _transform_depends_on_canvas_ids(
    nid: str,
    *,
    edges_raw: Sequence[Mapping[str, Any]],
    rev_adj_data: Mapping[str, List[str]],
    by_id: Dict[str, Mapping[str, Any]],
    rev_adj_all: Mapping[str, List[str]],
    executable_ids: Set[str],
) -> Set[str]:
    """
    Transform task predecessors: sequence edge wins (single chained pred);
    otherwise data-edge immediate preds, else data-edge ancestors.
    """
    seq_preds = _immediate_executable_predecessors(
        nid,
        rev_adj=_build_rev_adj(edges_raw, edge_kinds=frozenset({"sequence"})),
        executable_ids=executable_ids,
    )
    if seq_preds:
        return seq_preds
    data_preds = _immediate_executable_predecessors(
        nid, rev_adj=rev_adj_data, executable_ids=executable_ids
    )
    if data_preds:
        return data_preds
    return _collect_executable_ancestors(
        nid,
        by_id=by_id,
        rev_adj=dict(rev_adj_data),
        executable_ids=executable_ids,
    )


def _collect_executable_ancestors(
    start_id: str,
    *,
    by_id: Dict[str, Mapping[str, Any]],
    rev_adj: Dict[str, List[str]],
    executable_ids: Set[str],
) -> Set[str]:
    """Executable canvas node ids that must finish before ``start_id`` (via pass-through / structural)."""
    out: Set[str] = set()
    stack = list(rev_adj.get(start_id, []))
    visited: Set[str] = set()
    while stack:
        cur = stack.pop()
        if cur in visited:
            continue
        visited.add(cur)
        if cur in executable_ids:
            out.add(cur)
            continue
        if cur not in by_id:
            continue
        n = by_id[cur]
        k = _kind(n)
        if k in PASS_THROUGH_KINDS | STRUCTURAL_KINDS:
            stack.extend(rev_adj.get(cur, []))
            continue
        if _is_disabled_pass_through(n):
            stack.extend(rev_adj.get(cur, []))
            continue
        # unknown kind — stop this branch
    return out


def _sink_task_ids(tasks: Sequence[Mapping[str, Any]]) -> List[str]:
    """Task ids that are never listed as a dependency of another task (DAG sinks)."""
    id_list: List[str] = []
    for t in tasks:
        if not isinstance(t, dict):
            continue
        tid = str(t.get("id") or "").strip()
        if tid:
            id_list.append(tid)
    referenced: Set[str] = set()
    for t in tasks:
        if not isinstance(t, dict):
            continue
        for d in t.get("depends_on") or []:
            ds = str(d).strip()
            if ds:
                referenced.add(ds)
    return sorted(tid for tid in id_list if tid not in referenced)


def _validate_task_dag(tasks: Sequence[Mapping[str, Any]]) -> None:
    ids = [str(t.get("id") or "") for t in tasks if isinstance(t, dict)]
    id_set = {x for x in ids if x}
    if len(id_set) != len([x for x in ids if x]):
        raise CanvasCompileError("Duplicate task ids in compiled workflow")
    for t in tasks:
        if not isinstance(t, dict):
            continue
        tid = str(t.get("id") or "")
        deps = t.get("depends_on") if isinstance(t.get("depends_on"), list) else []
        for d in deps:
            ds = str(d)
            if ds not in id_set:
                raise CanvasCompileError(f"Task {tid!r} depends on unknown task {ds!r}")
    remaining = set(id_set)
    pred_map = {
        str(t.get("id")): set(str(x) for x in (t.get("depends_on") or []) if x)
        for t in tasks
        if isinstance(t, dict) and t.get("id")
    }
    completed: Set[str] = set()
    while remaining:
        ready = {tid for tid in remaining if pred_map.get(tid, set()) <= completed}
        if not ready:
            raise CanvasCompileError("Task graph has a cycle or unsatisfiable dependencies")
        completed |= ready
        remaining -= ready


def _walk_scope_canvas_nodes(nodes: Any) -> List[Dict[str, Any]]:
    """Depth-first list of canvas node dicts including ``subgraph`` ``inner_canvas`` members."""
    out: List[Dict[str, Any]] = []
    if not isinstance(nodes, list):
        return out
    for n in nodes:
        if not isinstance(n, dict):
            continue
        out.append(n)
        if _kind(n) == "subgraph":
            data = n.get("data") if isinstance(n.get("data"), dict) else {}
            inner = data.get("inner_canvas")
            if isinstance(inner, dict):
                out.extend(_walk_scope_canvas_nodes(inner.get("nodes")))
    return out


def _overlay_persistence_by_canvas_node(doc: Dict[str, Any], cw: MutableMapping[str, Any]) -> None:
    """Apply ``node.data.persistence_config`` onto the task with matching ``canvas_node_id`` / id."""
    canvas = doc.get("canvas")
    if not isinstance(canvas, dict):
        return
    nodes = _walk_scope_canvas_nodes(canvas.get("nodes"))
    tasks = cw.get("tasks")
    if not isinstance(tasks, list):
        return

    def find_task_by_canvas_node_id(canvas_nid: str) -> Optional[MutableMapping[str, Any]]:
        for t in tasks:
            if not isinstance(t, dict):
                continue
            if str(t.get("canvas_node_id") or "") == canvas_nid:
                return t
        return None

    for n in nodes:
        if not isinstance(n, dict):
            continue
        nid = str(n.get("id") or "").strip()
        if not nid:
            continue
        data = n.get("data")
        if not isinstance(data, dict):
            continue
        pc = data.get("persistence_config")
        if not isinstance(pc, dict):
            continue
        t = find_task_by_canvas_node_id(nid)
        if not t:
            continue
        base = dict(t.get("persistence") or {}) if isinstance(t.get("persistence"), dict) else {}
        for kk, vv in pc.items():
            if kk in ("kind", "profile"):
                continue
            if vv is not None and vv != "":
                base[kk] = vv
        if base:
            t["persistence"] = base


def compile_canvas_dag(doc: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(doc, dict):
        raise CanvasCompileError("document must be a mapping")
    canvas = _canvas_doc(doc)
    if not canvas:
        raise CanvasCompileError("canvas is missing")

    canvas = _flatten_canvas_subgraphs(copy.deepcopy(canvas))

    nodes_raw = canvas.get("nodes")
    edges_raw = canvas.get("edges")
    if not isinstance(nodes_raw, list):
        raise CanvasCompileError("canvas.nodes must be a list")
    if not isinstance(edges_raw, list):
        edges_raw = []

    by_id = _node_by_id(nodes_raw)
    rev_adj_all = _build_rev_adj(edges_raw)
    rev_adj_data = _build_rev_adj(edges_raw, edge_kinds=frozenset({"data"}))

    executable_canvas_ids: List[str] = []
    executable_rows: List[Tuple[str, str, str, Mapping[str, Any]]] = []
    for nid, n in by_id.items():
        k = _kind(n)
        if k == "filter":
            raise CanvasCompileError(
                f"canvas node id={nid!r}: kind 'filter' was renamed to 'instance_filter' "
                "(use confidence_filter for per-alias score pruning)"
            )
        if k not in _KIND_FN:
            continue
        if not _is_canvas_node_enabled(n):
            continue
        _fn_ext, exec_kind = _KIND_FN[k]
        data = n.get("data") if isinstance(n.get("data"), dict) else {}
        executable_canvas_ids.append(nid)
        executable_rows.append((nid, k, exec_kind, data))
    task_id_by_canvas = _resolve_executable_task_ids(executable_rows)

    if not executable_canvas_ids:
        raise CanvasCompileError(
            "canvas has no executable nodes (expected one of: save_view, save_raw, save_classic, "
            "query_view, query_raw, query_classic, query_sql, transform, merge, join, validation, instance_filter, "
            "confidence_filter, inverted_index, "
            "discovery_raw_cleanup)"
        )

    executable_id_set = set(executable_canvas_ids)

    tasks: List[Dict[str, Any]] = []
    channels: List[Dict[str, Any]] = []

    def add_channel(frm: str, to: str, name: str) -> None:
        channels.append({"from": frm, "to": to, "channel": name})

    for nid in executable_canvas_ids:
        n = by_id[nid]
        k = _kind(n)
        fn_ext, exec_kind = _KIND_FN[k]
        tid = task_id_by_canvas[nid]
        if k == "transform":
            pred_canvas = _transform_depends_on_canvas_ids(
                nid,
                edges_raw=edges_raw,
                rev_adj_data=rev_adj_data,
                by_id=by_id,
                rev_adj_all=rev_adj_all,
                executable_ids=executable_id_set,
            )
        elif k == "merge":
            pred_canvas = _immediate_executable_predecessors(
                nid, rev_adj=rev_adj_data, executable_ids=executable_id_set
            )
            if not pred_canvas:
                pred_canvas = _collect_executable_ancestors(
                    nid, by_id=by_id, rev_adj=rev_adj_data, executable_ids=executable_id_set
                )
        else:
            pred_canvas = _collect_executable_ancestors(
                nid, by_id=by_id, rev_adj=rev_adj_all, executable_ids=executable_id_set
            )
        depends_on: Set[str] = {task_id_by_canvas[p] for p in pred_canvas if p in task_id_by_canvas}
        dep_list = sorted(depends_on)

        data = n.get("data") if isinstance(n.get("data"), dict) else {}
        payload: Dict[str, Any] = {}
        frag = _discovery_payload_fragment(k, data)
        if frag:
            payload.update(frag)
        elif k in _DISCOVERY_KINDS_WITH_CONFIG:
            raise CanvasCompileError(
                f"canvas node id={nid!r} kind={k!r}: set non-empty data.config on the canvas node "
                f"(top-level scope lists are not used for discovery compile)"
            )
        if k == "inverted_index":
            payload["inverted_index_input_source"] = "discovery_predecessor_payloads"
            payload["upstream_compiled_task_ids"] = list(dep_list)
        if k == "join":
            left_tid, right_tid = _join_input_task_ids_from_edges(
                nid,
                edges_raw,
                task_id_by_canvas=task_id_by_canvas,
                by_id=by_id,
                rev_adj=rev_adj_all,
                executable_ids=executable_id_set,
            )
            payload["join_left_task_id"] = left_tid
            payload["join_right_task_id"] = right_tid
        persistence: Optional[Dict[str, Any]] = None

        entry: Dict[str, Any] = {
            "id": tid,
            "canvas_node_id": nid,
            "function_external_id": fn_ext,
            "executor_kind": exec_kind,
            "depends_on": dep_list,
            "pipeline_node_id": tid,
            "payload": payload,
        }
        node_label = canvas_node_label(data)
        if node_label:
            entry["label"] = node_label
        if persistence is not None:
            entry["persistence"] = persistence
        tasks.append(entry)
        for p in dep_list:
            add_channel(p, tid, f"{p}_to_{tid}")

    CLEANUP_TASK_ID = "kea__discovery_raw_cleanup"
    if not any(
        isinstance(t, dict) and str(t.get("function_external_id") or "").strip()
        == "fn_dm_discovery_raw_cleanup"
        for t in tasks
    ):
        sinks = _sink_task_ids(tasks)
        if sinks:
            cleanup_entry: Dict[str, Any] = {
                "id": CLEANUP_TASK_ID,
                "canvas_node_id": "n_discovery_raw_cleanup",
                "function_external_id": "fn_dm_discovery_raw_cleanup",
                "executor_kind": "discovery_raw_cleanup",
                "depends_on": sinks,
                "pipeline_node_id": CLEANUP_TASK_ID,
                "payload": {"config": {}},
            }
            tasks.append(cleanup_entry)
            for s in sinks:
                add_channel(s, CLEANUP_TASK_ID, f"{s}_to_{CLEANUP_TASK_ID}")

    out: Dict[str, Any] = {
        "schemaVersion": COMPILED_WORKFLOW_SCHEMA_VERSION,
        "tasks": tasks,
        "channels": channels,
        "dag_source": "canvas",
    }
    _overlay_persistence_by_canvas_node(doc, out)
    _validate_task_dag(out["tasks"])
    return out


def compile_workflow_from_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build ``compiled_workflow`` IR for ``workflow.input`` from the flow canvas.

    Always uses :func:`compile_canvas_dag`. ``compile_workflow_dag`` must be ``canvas`` or omitted.
    """
    _compile_dag_mode(doc)
    return compile_canvas_dag(doc)


def compiled_workflow_for_scope_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Return ``compiled_workflow`` for ``workflow.input`` (canvas DAG from scope canvas)."""
    return compile_workflow_from_document(doc)
