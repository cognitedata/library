"""Compile ``compiled_workflow`` IR from a UI canvas (arbitrary DAG over executable node kinds).

``subgraph`` nodes are flattened before compile: ``inner_canvas`` nodes and edges are promoted to
the parent graph, and edges that targeted or sourced the subgraph frame are rewired to
``subflow_hub_input_id`` / ``subflow_hub_output_id`` (same ``in__`` / ``out__`` handle ids).
"""

from __future__ import annotations

import copy
import re
from typing import Any, Dict, FrozenSet, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

from .legacy_ir import (
    COMPILED_WORKFLOW_SCHEMA_VERSION,
    TASK_INCREMENTAL,
    _default_alias_persistence_payload,
    _default_aliasing_payload,
    _default_reference_index_payload,
)

# Canvas node kinds that never become Cognite function tasks.
STRUCTURAL_KINDS: FrozenSet[str] = frozenset(
    {
        "start",
        "end",
        "source_view",
        "subflow",
        "subflow_graph_in",
        "subflow_graph_out",
        # Legacy layout-only cards (removed from UI); still traversable for dependency closure.
        "writeback_raw",
        "writeback_data_modeling",
    }
)

PASS_THROUGH_KINDS: FrozenSet[str] = frozenset(
    {
        "validation",
        "match_validation_source_view",
        "match_validation_extraction",
        "match_validation_aliasing",
    }
)

_KIND_FN: Dict[str, Tuple[str, str, Optional[str]]] = {
    "extraction": ("fn_dm_key_extraction", "key_extraction", None),
    "aliasing": ("fn_dm_aliasing", "aliasing", "aliasing"),
    "reference_index": ("fn_dm_reference_index", "reference_index", "reference_index"),
    "alias_persistence": ("fn_dm_alias_persistence", "alias_persistence", "alias_persistence"),
}


class CanvasCompileError(ValueError):
    """Invalid canvas for workflow compilation."""


def _legacy_compile_requested(doc: Mapping[str, Any]) -> bool:
    """True when scope still sets the removed legacy compile mode (for diagnostics / UI)."""
    for raw in (
        doc.get("compile_workflow_dag"),
        (
            doc.get("workflow", {}).get("compile_dag_mode")
            if isinstance(doc.get("workflow"), dict)
            else None
        ),
    ):
        if raw is None:
            continue
        if str(raw).strip().lower() == "legacy":
            return True
    return False


def _compile_dag_mode(doc: Mapping[str, Any]) -> str:
    raw = doc.get("compile_workflow_dag")
    if raw is None:
        w = doc.get("workflow")
        if isinstance(w, dict) and w.get("compile_dag_mode") is not None:
            raw = w.get("compile_dag_mode")
    if raw is None:
        return "auto"
    s = str(raw).strip().lower()
    if s == "legacy":
        raise ValueError(
            "compile_workflow_dag / workflow.compile_dag_mode value 'legacy' is no longer "
            "supported; use 'auto' or 'canvas' with a valid canvas."
        )
    if s in ("canvas", "auto"):
        return s
    return "auto"


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


def should_use_canvas_dag(doc: Mapping[str, Any]) -> bool:
    """
    Whether the scope document has a canvas with at least one executable node.

    Used for diagnostics/UI. Legacy compile mode (removed) yields false here without raising.
    """
    if _legacy_compile_requested(doc):
        return False
    try:
        mode = _compile_dag_mode(doc)
    except ValueError:
        return False
    if mode == "canvas":
        return True
    c = _canvas_doc(doc)
    if not c:
        return False
    return _has_executable_canvas(c)


def sanitize_task_id(node_id: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(node_id or "").strip())
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "node"
    return f"kea__{s}"


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
        k = _kind(by_id[cur])
        if k in PASS_THROUGH_KINDS | STRUCTURAL_KINDS:
            stack.extend(rev_adj.get(cur, []))
            continue
        # unknown kind — stop this branch
    return out


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

    def find_task(canvas_nid: str, task_tid: str) -> Optional[MutableMapping[str, Any]]:
        for t in tasks:
            if not isinstance(t, dict):
                continue
            if str(t.get("canvas_node_id") or "") == canvas_nid:
                return t
            if str(t.get("id") or "") == task_tid:
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
        tid = sanitize_task_id(nid)
        t = find_task(nid, tid)
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
    rev_adj: Dict[str, List[str]] = {nid: [] for nid in by_id}
    for e in edges_raw:
        if not isinstance(e, dict):
            continue
        s = str(e.get("source") or "").strip()
        t = str(e.get("target") or "").strip()
        if s and t:
            if t not in rev_adj:
                rev_adj[t] = []
            rev_adj[t].append(s)
            if s not in rev_adj:
                rev_adj[s] = rev_adj.get(s, [])

    executable_canvas_ids: List[str] = []
    task_id_by_canvas: Dict[str, str] = {}
    for nid, n in by_id.items():
        k = _kind(n)
        if k not in _KIND_FN:
            continue
        task_id_by_canvas[nid] = sanitize_task_id(nid)
        executable_canvas_ids.append(nid)

    if not executable_canvas_ids:
        raise CanvasCompileError(
            "canvas has no executable nodes (extraction, aliasing, reference_index, alias_persistence)"
        )

    executable_id_set = set(executable_canvas_ids)
    has_extraction = any(_kind(by_id[nid]) == "extraction" for nid in executable_canvas_ids)

    tasks: List[Dict[str, Any]] = []
    channels: List[Dict[str, Any]] = []

    def add_channel(frm: str, to: str, name: str) -> None:
        channels.append({"from": frm, "to": to, "channel": name})

    if has_extraction:
        tasks.append(
            {
                "id": TASK_INCREMENTAL,
                "function_external_id": "fn_dm_incremental_state_update",
                "executor_kind": "incremental_state",
                "depends_on": [],
                "pipeline_node_id": TASK_INCREMENTAL,
                "payload": {},
            }
        )

    for nid in executable_canvas_ids:
        n = by_id[nid]
        k = _kind(n)
        fn_ext, exec_kind, pers_key = _KIND_FN[k]
        tid = task_id_by_canvas[nid]
        pred_canvas = _collect_executable_ancestors(
            nid, by_id=by_id, rev_adj=rev_adj, executable_ids=executable_id_set
        )
        depends_on: Set[str] = {task_id_by_canvas[p] for p in pred_canvas if p in task_id_by_canvas}
        if has_extraction and k == "extraction":
            depends_on.add(TASK_INCREMENTAL)
        dep_list = sorted(depends_on)

        data = n.get("data") if isinstance(n.get("data"), dict) else {}
        ref = data.get("ref") if isinstance(data.get("ref"), dict) else {}
        payload: Dict[str, Any] = {}
        if k == "extraction":
            rn = ref.get("extraction_rule_name")
            if isinstance(rn, str) and rn.strip():
                payload["extraction_rule_names"] = [rn.strip()]
        if k == "aliasing":
            an = ref.get("aliasing_rule_name")
            if isinstance(an, str) and an.strip():
                payload["aliasing_rule_names"] = [an.strip()]

        persistence: Optional[Dict[str, Any]] = None
        if pers_key == "reference_index":
            persistence = dict(_default_reference_index_payload(doc))
        elif pers_key == "aliasing":
            persistence = dict(_default_aliasing_payload(doc))
        elif pers_key == "alias_persistence":
            persistence = dict(_default_alias_persistence_payload(doc))

        entry: Dict[str, Any] = {
            "id": tid,
            "canvas_node_id": nid,
            "function_external_id": fn_ext,
            "executor_kind": exec_kind,
            "depends_on": dep_list,
            "pipeline_node_id": tid,
            "payload": payload,
        }
        if persistence is not None:
            entry["persistence"] = persistence
        tasks.append(entry)
        for p in dep_list:
            add_channel(p, tid, f"{p}_to_{tid}")

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

    Always uses :func:`compile_canvas_dag`. ``compile_workflow_dag`` may be ``canvas`` (default
    behaviour) or ``auto`` / unset (same compile path today). The value ``legacy`` is rejected.
    """
    _compile_dag_mode(doc)
    return compile_canvas_dag(doc)


def compiled_workflow_for_scope_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Return ``compiled_workflow`` for ``workflow.input`` (canvas DAG from scope canvas)."""
    return compile_workflow_from_document(doc)
