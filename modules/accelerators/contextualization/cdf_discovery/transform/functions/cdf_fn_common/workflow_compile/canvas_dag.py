"""Compile ETL canvas DAG to ``compiled_workflow`` IR."""

from __future__ import annotations

from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Sequence, Set, Tuple

COMPILED_WORKFLOW_SCHEMA_VERSION = 1

JOIN_TARGET_HANDLE_LEFT = "in__left"
JOIN_TARGET_HANDLE_RIGHT = "in__right"

_JOIN_INPUT_SOURCE_KINDS: FrozenSet[str] = frozenset(
    {
        "query_view",
        "query_raw",
        "query_classic",
        "query_sql",
        "transform",
        "filter",
        "field_map",
        "score",
        "join",
        "merge",
        "build_index",
    }
)


class CanvasCompileError(ValueError):
    """Invalid canvas graph for compile."""

STRUCTURAL_KINDS: FrozenSet[str] = frozenset({"start", "end"})

# canvas kind -> (function external id OR None for non-function, executor_kind, cdf task type)
_KIND_SPEC: Dict[str, Tuple[str | None, str, str]] = {
    "query_view": ("fn_etl_view_query", "query_view", "function"),
    "query_raw": ("fn_etl_raw_query", "query_raw", "function"),
    "query_classic": ("fn_etl_classic_query", "query_classic", "function"),
    "query_sql": ("fn_etl_sql_query", "query_sql", "function"),
    "transform": ("fn_etl_transform", "transform", "function"),
    "filter": ("fn_etl_filter", "filter", "function"),
    "field_map": ("fn_etl_field_map", "field_map", "function"),
    "instance_filter": ("fn_etl_filter", "filter", "function"),
    "score": ("fn_etl_score", "score", "function"),
    "validation": ("fn_etl_score", "score", "function"),
    "join": ("fn_etl_join", "join", "function"),
    "merge": ("fn_etl_merge", "merge", "function"),
    "build_index": ("fn_etl_build_index", "build_index", "function"),
    "save_view": ("fn_etl_view_save", "save_view", "function"),
    "save_raw": ("fn_etl_raw_save", "save_raw", "function"),
    "save_classic": ("fn_etl_classic_save", "save_classic", "function"),
    "spark_transform": (None, "spark_transform", "transformation"),
    "transformation_ref": (None, "transformation_ref", "transformation"),
    "function_ref": (None, "function_ref", "function"),
    "dynamic_fanout": (None, "dynamic_fanout", "dynamic"),
    "subworkflow": (None, "subworkflow", "subworkflow"),
    "simulation": (None, "simulation", "simulation"),
    "cdf_task": (None, "cdf_task", "cdf"),
    "raw_cleanup": ("fn_etl_raw_cleanup", "raw_cleanup", "function"),
}

_KIND_FN: Dict[str, Tuple[str, str]] = {
    k: (v[0], v[1]) for k, v in _KIND_SPEC.items() if v[0] is not None
}


def _node_kind(node: Mapping[str, Any]) -> str:
    kind = str(node.get("kind") or "").strip()
    if kind:
        return kind
    data = node.get("data")
    if isinstance(data, dict):
        return str(data.get("kind") or "").strip()
    return ""


def _join_input_task_ids_from_edges(
    join_node_id: str,
    edges: Sequence[Mapping[str, Any]],
    *,
    node_by_id: Mapping[str, Mapping[str, Any]],
    executable_ids: Set[str],
) -> Tuple[str, str]:
    left_tid: Optional[str] = None
    right_tid: Optional[str] = None
    left_src: Optional[str] = None
    right_src: Optional[str] = None

    for e in edges:
        if not isinstance(e, dict):
            continue
        if str(e.get("target") or "").strip() != join_node_id:
            continue
        src = str(e.get("source") or "").strip()
        if not src or src not in executable_ids:
            raise CanvasCompileError(
                f"join node {join_node_id!r}: predecessor {src!r} is not an executable canvas node"
            )
        pred = node_by_id.get(src)
        if pred is None:
            continue
        pk = _node_kind(pred)
        if pk not in _JOIN_INPUT_SOURCE_KINDS:
            raise CanvasCompileError(
                f"join node {join_node_id!r}: predecessor {src!r} kind={pk!r} is not allowed "
                f"as join input (expected one of: {sorted(_JOIN_INPUT_SOURCE_KINDS)})"
            )
        th = str(e.get("target_handle") or "").strip()
        if th == JOIN_TARGET_HANDLE_LEFT:
            if left_tid is not None:
                raise CanvasCompileError(
                    f"join node {join_node_id!r}: multiple edges target {JOIN_TARGET_HANDLE_LEFT!r}"
                )
            left_tid = src
            left_src = src
        elif th == JOIN_TARGET_HANDLE_RIGHT:
            if right_tid is not None:
                raise CanvasCompileError(
                    f"join node {join_node_id!r}: multiple edges target {JOIN_TARGET_HANDLE_RIGHT!r}"
                )
            right_tid = src
            right_src = src
        else:
            raise CanvasCompileError(
                f"join node {join_node_id!r}: edge from {src!r} must use "
                f"target_handle {JOIN_TARGET_HANDLE_LEFT!r} or {JOIN_TARGET_HANDLE_RIGHT!r}, "
                f"got {th!r}"
            )

    if left_tid is None or right_tid is None:
        raise CanvasCompileError(
            f"join node {join_node_id!r}: require exactly one edge to {JOIN_TARGET_HANDLE_LEFT!r} "
            f"and one to {JOIN_TARGET_HANDLE_RIGHT!r}"
        )
    if left_src == right_src:
        raise CanvasCompileError(
            f"join node {join_node_id!r}: left and right inputs must be different nodes"
        )
    return left_tid, right_tid


def _end_node_cleanup_tasks(
    nodes: Sequence[Mapping[str, Any]],
    *,
    pred: Dict[str, List[str]],
    executable_ids: Set[str],
) -> List[Dict[str, Any]]:
    """Compile structural ``end`` nodes as terminal ``raw_cleanup`` tasks."""
    out: List[Dict[str, Any]] = []
    for n in nodes:
        if not isinstance(n, dict):
            continue
        if _node_kind(n) != "end":
            continue
        end_id = str(n.get("id") or "end").strip()
        if not end_id:
            continue
        data = n.get("data") if isinstance(n.get("data"), dict) else {}
        cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
        if not cfg:
            cfg = {"description": "Post-run cohort RAW cleanup"}
        deps = [d for d in (pred.get(end_id) or []) if d in executable_ids]
        out.append(
            {
                "id": end_id,
                "function_external_id": "fn_etl_raw_cleanup",
                "executable_kind": "raw_cleanup",
                "task_type": "function",
                "canvas_node_id": end_id,
                "depends_on": deps,
                "payload": {"config": dict(cfg)},
            }
        )
    return out


def etl_local_pipeline_specs() -> Dict[str, Tuple[str, str]]:
    out: Dict[str, Tuple[str, str]] = {}
    for fn_ext, exec_kind in _KIND_FN.values():
        if fn_ext not in out:
            out[fn_ext] = (f"{fn_ext}.pipeline", exec_kind)
    return out


def compile_canvas_dag(canvas: Mapping[str, Any]) -> Dict[str, Any]:
    """Map executable canvas nodes to compiled_workflow tasks."""
    nodes = canvas.get("nodes") or []
    edges = canvas.get("edges") or []
    pred: Dict[str, List[str]] = {}
    for e in edges:
        if not isinstance(e, dict):
            continue
        src = str(e.get("source") or "").strip()
        tgt = str(e.get("target") or "").strip()
        if src and tgt:
            pred.setdefault(tgt, []).append(src)

    node_by_id: Dict[str, Dict[str, Any]] = {}
    executable_ids: set[str] = set()
    for n in nodes:
        if not isinstance(n, dict):
            continue
        kind = _node_kind(n)
        node_id = str(n.get("id") or kind).strip()
        if node_id:
            node_by_id[node_id] = n
        if kind in STRUCTURAL_KINDS:
            continue
        if kind not in _KIND_SPEC:
            continue
        executable_ids.add(node_id)

    tasks: List[Dict[str, Any]] = []
    for n in nodes:
        if not isinstance(n, dict):
            continue
        kind = _node_kind(n)
        if kind in STRUCTURAL_KINDS:
            continue
        spec = _KIND_SPEC.get(kind)
        if spec is None:
            continue
        fn_ext, exec_kind, task_type = spec
        node_id = str(n.get("id") or kind).strip()
        data = n.get("data") if isinstance(n.get("data"), dict) else {}
        cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
        deps = [d for d in (pred.get(node_id) or []) if d in executable_ids]
        payload: Dict[str, Any] = {"config": cfg}
        if kind == "join":
            left_tid, right_tid = _join_input_task_ids_from_edges(
                node_id,
                edges,
                node_by_id=node_by_id,
                executable_ids=executable_ids,
            )
            payload["join_left_task_id"] = left_tid
            payload["join_right_task_id"] = right_tid
        tasks.append(
            {
                "id": node_id,
                "function_external_id": fn_ext,
                "executable_kind": exec_kind,
                "task_type": task_type,
                "canvas_node_id": node_id,
                "depends_on": deps,
                "payload": payload,
            }
        )

    if not any(t.get("executable_kind") == "raw_cleanup" for t in tasks):
        end_cleanups = _end_node_cleanup_tasks(nodes, pred=pred, executable_ids=executable_ids)
        if end_cleanups:
            tasks.extend(end_cleanups)
        else:
            sink_deps = [t["id"] for t in tasks if t.get("id")]
            tasks.append(
                {
                    "id": "etl__raw_cleanup__sink",
                    "function_external_id": "fn_etl_raw_cleanup",
                    "executable_kind": "raw_cleanup",
                    "task_type": "function",
                    "canvas_node_id": "etl__raw_cleanup__sink",
                    "depends_on": sink_deps,
                    "payload": {"config": {"description": "Post-run cohort RAW cleanup"}},
                }
            )

    return {
        "schema_version": COMPILED_WORKFLOW_SCHEMA_VERSION,
        "tasks": tasks,
    }
