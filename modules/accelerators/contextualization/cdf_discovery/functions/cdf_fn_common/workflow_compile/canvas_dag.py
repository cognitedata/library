"""Compile ETL canvas DAG to ``compiled_workflow`` IR."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Sequence, Set, Tuple

COMPILED_WORKFLOW_SCHEMA_VERSION = 1

_DIAGRAM_ANNOTATION_MAPPER_KINDS: FrozenSet[str] = frozenset(
    {"diagram_detect_to_dm", "diagram_detect_to_classic"}
)

JOIN_TARGET_HANDLE_LEFT = "in__left"
JOIN_TARGET_HANDLE_RIGHT = "in__right"

FILE_ANNOTATION_HANDLE_ENTITIES = "in__entities"
FILE_ANNOTATION_HANDLE_FILES = "in__files"
FANOUT_PLAN_HANDLE_INPUT_A = "in__input_a"
FANOUT_PLAN_HANDLE_INPUT_B = "in__input_b"

_JOIN_INPUT_SOURCE_KINDS: FrozenSet[str] = frozenset(
    {
        "query_view",
        "query_raw",
        "query_classic",
        "query_sql",
        "query_records",
        "transform",
        "filter",
        "score",
        "join",
        "merge",
        "build_index",
    }
)


class CanvasCompileError(ValueError):
    """Invalid canvas graph for compile."""


def _agent_log(hypothesis_id: str, location: str, message: str, data: Mapping[str, Any]) -> None:
    # region agent log
    try:
        payload = {
            "sessionId": "e09635",
            "runId": "pre-fix",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": dict(data),
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        }
        with Path(
            "/Users/darren.downtain@cognitedata.com/Documents/GitHub/library/.cursor/debug-e09635.log"
        ).open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass
    # endregion

STRUCTURAL_KINDS: FrozenSet[str] = frozenset({"start", "end"})

# Stages that materialize or pass cohort rows (required upstream of transform / score / build_index).
_COHORT_SOURCE_KINDS: FrozenSet[str] = frozenset(
    {
        "query_view",
        "query_raw",
        "query_classic",
        "query_sql",
        "query_records",
        "transform",
        "score",
        "build_index",
        "filter",
        "join",
        "merge",
        "file_annotation",
        "workflow_fanout_plan",
        "json_mapping",
    }
)

_COHORT_CONSUMER_KINDS: FrozenSet[str] = frozenset({"transform", "score", "build_index"})
_QUERY_KINDS: FrozenSet[str] = frozenset(
    {"query_view", "query_raw", "query_classic", "query_sql", "query_records"}
)

# Canvas-only nodes: never compiled; dependencies walk through them.
CANVAS_ONLY_KINDS: FrozenSet[str] = frozenset({"subgraph", "node_preview"})

# canvas kind -> (function external id OR None for non-function, executor_kind, cdf task type)
_KIND_SPEC: Dict[str, Tuple[str | None, str, str]] = {
    "query_view": ("fn_etl_view_query", "query_view", "function"),
    "query_raw": ("fn_etl_raw_query", "query_raw", "function"),
    "query_classic": ("fn_etl_classic_query", "query_classic", "function"),
    "query_sql": ("fn_etl_sql_query", "query_sql", "function"),
    "query_records": ("fn_etl_records_query", "query_records", "function"),
    "transform": ("fn_etl_transform", "transform", "function"),
    "filter": ("fn_etl_filter", "filter", "function"),
    "json_mapping": (None, "json_mapping", "jsonMapping"),
    "instance_filter": ("fn_etl_filter", "filter", "function"),
    "score": ("fn_etl_score", "score", "function"),
    "validation": ("fn_etl_score", "score", "function"),
    "join": ("fn_etl_join", "join", "function"),
    "merge": ("fn_etl_merge", "merge", "function"),
    "build_index": ("fn_etl_build_index", "build_index", "function"),
    "file_annotation": ("fn_etl_file_annotation", "file_annotation", "function"),
    "workflow_fanout_plan": ("fn_etl_workflow_fanout_plan", "workflow_fanout_plan", "function"),
    "save_view": ("fn_etl_view_save", "save_view", "function"),
    "save_raw": ("fn_etl_raw_save", "save_raw", "function"),
    "save_classic": ("fn_etl_classic_save", "save_classic", "function"),
    "save_records": ("fn_etl_records_save", "save_records", "function"),
    "save_stream": ("fn_etl_stream_save", "save_stream", "function"),
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


def _resolved_function_external_id(
    kind: str, cfg: Mapping[str, Any], default_fn: str | None
) -> str | None:
    """Canvas ``function_ref`` nodes store the target id on config, not in ``_KIND_SPEC``."""
    if kind == "function_ref":
        ext = str(cfg.get("function_external_id") or "").strip()
        return ext or None
    return default_fn


def _canvas_only_node_ids(nodes: Sequence[Mapping[str, Any]]) -> Set[str]:
    out: Set[str] = set()
    for n in nodes:
        if not isinstance(n, dict):
            continue
        if _node_kind(n) in CANVAS_ONLY_KINDS:
            nid = str(n.get("id") or "").strip()
            if nid:
                out.add(nid)
    return out


def _resolve_executable_predecessors(
    node_id: str,
    pred: Mapping[str, Sequence[str]],
    *,
    executable_ids: Set[str],
    canvas_only_ids: Set[str],
) -> List[str]:
    """Executable predecessors, walking backward through canvas-only nodes."""
    result: List[str] = []
    seen_walk: Set[str] = set()

    def collect_from(pred_id: str) -> None:
        if pred_id in seen_walk:
            return
        seen_walk.add(pred_id)
        if pred_id in executable_ids:
            if pred_id not in result:
                result.append(pred_id)
            return
        if pred_id in canvas_only_ids:
            for pp in pred.get(pred_id) or []:
                collect_from(str(pp).strip())

    for p in pred.get(node_id) or []:
        pid = str(p).strip()
        if pid:
            collect_from(pid)
    return result


def _validate_cohort_consumer_predecessors(
    node_id: str,
    kind: str,
    deps: Sequence[str],
    node_by_id: Mapping[str, Mapping[str, Any]],
) -> None:
    if kind not in _COHORT_CONSUMER_KINDS:
        return
    if not deps:
        raise CanvasCompileError(
            f"{kind} node {node_id!r} requires an upstream stage that materializes cohort rows "
            "(e.g. query, transform, filter); connect a cohort-producing predecessor"
        )
    for dep_id in deps:
        pred = node_by_id.get(dep_id)
        if pred is None:
            continue
        pred_kind = _node_kind(pred)
        if pred_kind not in _COHORT_SOURCE_KINDS:
            raise CanvasCompileError(
                f"{kind} node {node_id!r}: predecessor {dep_id!r} (kind={pred_kind!r}) does not "
                f"materialize cohort rows; allowed upstream kinds include "
                f"{sorted(_COHORT_SOURCE_KINDS)}"
            )


def _node_kind(node: Mapping[str, Any]) -> str:
    kind = str(node.get("kind") or "").strip()
    if kind:
        return kind
    data = node.get("data")
    if isinstance(data, dict):
        return str(data.get("kind") or "").strip()
    return ""


def _dual_input_task_ids_from_edges(
    node_id: str,
    edges: Sequence[Mapping[str, Any]],
    *,
    handle_left: str,
    handle_right: str,
    node_by_id: Mapping[str, Mapping[str, Any]],
    executable_ids: Set[str],
    node_kind_label: str,
    require_right: bool = True,
) -> Tuple[Optional[str], Optional[str]]:
    left_tid: Optional[str] = None
    right_tid: Optional[str] = None
    left_src: Optional[str] = None
    right_src: Optional[str] = None

    for e in edges:
        if not isinstance(e, dict):
            continue
        if str(e.get("target") or "").strip() != node_id:
            continue
        src = str(e.get("source") or "").strip()
        if not src or src not in executable_ids:
            raise CanvasCompileError(
                f"{node_kind_label} node {node_id!r}: predecessor {src!r} is not an executable canvas node"
            )
        pred = node_by_id.get(src)
        if pred is None:
            continue
        pk = _node_kind(pred)
        if pk not in _JOIN_INPUT_SOURCE_KINDS:
            raise CanvasCompileError(
                f"{node_kind_label} node {node_id!r}: predecessor {src!r} kind={pk!r} is not allowed "
                f"as input (expected one of: {sorted(_JOIN_INPUT_SOURCE_KINDS)})"
            )
        th = str(e.get("target_handle") or "").strip()
        if th == handle_left:
            if left_tid is not None:
                raise CanvasCompileError(
                    f"{node_kind_label} node {node_id!r}: multiple edges target {handle_left!r}"
                )
            left_tid = src
            left_src = src
        elif th == handle_right:
            if right_tid is not None:
                raise CanvasCompileError(
                    f"{node_kind_label} node {node_id!r}: multiple edges target {handle_right!r}"
                )
            right_tid = src
            right_src = src
        else:
            raise CanvasCompileError(
                f"{node_kind_label} node {node_id!r}: edge from {src!r} must use "
                f"target_handle {handle_left!r} or {handle_right!r}, got {th!r}"
            )

    if left_tid is None:
        raise CanvasCompileError(
            f"{node_kind_label} node {node_id!r}: require exactly one edge to {handle_left!r}"
        )
    if require_right and right_tid is None:
        raise CanvasCompileError(
            f"{node_kind_label} node {node_id!r}: require exactly one edge to {handle_right!r}"
        )
    if right_tid is not None and left_src == right_src:
        raise CanvasCompileError(
            f"{node_kind_label} node {node_id!r}: left and right inputs must be different nodes"
        )
    return left_tid, right_tid


def _file_annotation_input_task_ids_from_edges(
    node_id: str,
    edges: Sequence[Mapping[str, Any]],
    *,
    node_by_id: Mapping[str, Mapping[str, Any]],
    executable_ids: Set[str],
    require_right: bool,
) -> Tuple[List[str], Optional[str]]:
    """Allow one-or-more entity inputs on left handle and optional/required files on right."""
    left_tids: List[str] = []
    right_tid: Optional[str] = None
    right_src: Optional[str] = None
    seen_left: Set[str] = set()

    for e in edges:
        if not isinstance(e, dict):
            continue
        if str(e.get("target") or "").strip() != node_id:
            continue
        src = str(e.get("source") or "").strip()
        if not src or src not in executable_ids:
            raise CanvasCompileError(
                f"file_annotation node {node_id!r}: predecessor {src!r} is not an executable canvas node"
            )
        pred = node_by_id.get(src)
        if pred is None:
            continue
        pk = _node_kind(pred)
        if pk not in _JOIN_INPUT_SOURCE_KINDS:
            raise CanvasCompileError(
                f"file_annotation node {node_id!r}: predecessor {src!r} kind={pk!r} is not allowed "
                f"as input (expected one of: {sorted(_JOIN_INPUT_SOURCE_KINDS)})"
            )
        th = str(e.get("target_handle") or "").strip()
        if th == FILE_ANNOTATION_HANDLE_ENTITIES:
            if src in seen_left:
                continue
            seen_left.add(src)
            left_tids.append(src)
        elif th == FILE_ANNOTATION_HANDLE_FILES:
            if right_tid is not None:
                raise CanvasCompileError(
                    f"file_annotation node {node_id!r}: multiple edges target {FILE_ANNOTATION_HANDLE_FILES!r}"
                )
            right_tid = src
            right_src = src
        else:
            raise CanvasCompileError(
                f"file_annotation node {node_id!r}: edge from {src!r} must use "
                f"target_handle {FILE_ANNOTATION_HANDLE_ENTITIES!r} or "
                f"{FILE_ANNOTATION_HANDLE_FILES!r}, got {th!r}"
            )

    if not left_tids:
        raise CanvasCompileError(
            f"file_annotation node {node_id!r}: require at least one edge to {FILE_ANNOTATION_HANDLE_ENTITIES!r}"
        )
    if require_right and right_tid is None:
        raise CanvasCompileError(
            f"file_annotation node {node_id!r}: require exactly one edge to {FILE_ANNOTATION_HANDLE_FILES!r}"
        )
    if right_src is not None and right_src in seen_left:
        raise CanvasCompileError(
            f"file_annotation node {node_id!r}: entity and file inputs must be different nodes"
        )
    _agent_log(
        "H1",
        "canvas_dag.py:_file_annotation_input_task_ids_from_edges",
        "compiled file_annotation handles",
        {
            "node_id": node_id,
            "left_tids": left_tids,
            "right_tid": right_tid,
            "require_right": require_right,
        },
    )
    return left_tids, right_tid


def _join_input_task_ids_from_edges(
    join_node_id: str,
    edges: Sequence[Mapping[str, Any]],
    *,
    node_by_id: Mapping[str, Mapping[str, Any]],
    executable_ids: Set[str],
) -> Tuple[str, str]:
    left_tid, right_tid = _dual_input_task_ids_from_edges(
        join_node_id,
        edges,
        handle_left=JOIN_TARGET_HANDLE_LEFT,
        handle_right=JOIN_TARGET_HANDLE_RIGHT,
        node_by_id=node_by_id,
        executable_ids=executable_ids,
        node_kind_label="join",
        require_right=True,
    )
    return left_tid or "", right_tid or ""


def _config_has_file_ids(cfg: Mapping[str, Any]) -> bool:
    raw_ids = cfg.get("file_ids")
    raw_external_ids = cfg.get("file_external_ids")

    def _has_value(raw: Any) -> bool:
        if raw is None or raw == "":
            return False
        if isinstance(raw, list):
            return any(str(x or "").strip() for x in raw)
        if isinstance(raw, str):
            parts = [p.strip() for p in str(raw).replace(";", ",").split(",") if p.strip()]
            return bool(parts)
        return True

    decision = _has_value(raw_ids) or _has_value(raw_external_ids)
    _agent_log(
        "H8",
        "canvas_dag.py:_config_has_file_ids",
        "file id/external id config gate evaluated",
        {
            "raw_ids_type": type(raw_ids).__name__,
            "raw_ids_value": raw_ids,
            "raw_external_ids_type": type(raw_external_ids).__name__,
            "raw_external_ids_value": raw_external_ids,
            "decision": decision,
        },
    )
    return decision


def _end_node_cleanup_tasks(
    nodes: Sequence[Mapping[str, Any]],
    *,
    pred: Dict[str, List[str]],
    executable_ids: Set[str],
    canvas_only_ids: Set[str],
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
        if bool(cfg.get("lookup_full_scan")) and kind not in _QUERY_KINDS:
            errors.append(
                f"{kind} node {node_id!r}: lookup_full_scan is only supported on query nodes"
            )
        if not cfg:
            cfg = {"description": "Post-run cohort RAW cleanup"}
        deps = _resolve_executable_predecessors(
            end_id,
            pred,
            executable_ids=executable_ids,
            canvas_only_ids=canvas_only_ids,
        )
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
    # Dynamic fan-out children are not canvas node kinds but must run locally.
    _extra_local: Dict[str, str] = {
        "fn_etl_file_annotation": "file_annotation",
        "fn_etl_file_annotation_launch": "file_annotation_launch",
        "fn_etl_file_annotation_finalize": "file_annotation_finalize",
        "fn_etl_file_annotation_barrier": "file_annotation_barrier",
    }
    for fn_ext, entry in _extra_local.items():
        if fn_ext not in out:
            out[fn_ext] = (f"{fn_ext}.pipeline", entry)
    return out


def _normalize_mislabeled_fanout_plan_nodes(nodes: List[Any]) -> None:
    """Canvas nodes saved as ``transform`` but configured for fan-out planning."""
    for n in nodes:
        if not isinstance(n, dict):
            continue
        if _node_kind(n) != "transform":
            continue
        data = n.get("data") if isinstance(n.get("data"), dict) else {}
        cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
        if cfg.get("dynamic_fanout_depends_on") or cfg.get("generator_task_id"):
            n["kind"] = "workflow_fanout_plan"


def _validate_json_mapping_config(node_id: str, cfg: Mapping[str, Any]) -> None:
    expr = str(cfg.get("expression") or "").strip()
    if not expr:
        raise CanvasCompileError(
            f"json_mapping node {node_id!r}: expression is required for CDF jsonMapping tasks"
        )
    inp = cfg.get("input")
    if inp is not None and (not isinstance(inp, dict) or isinstance(inp, list)):
        raise CanvasCompileError(
            f"json_mapping node {node_id!r}: input must be a JSON object when set"
        )


def _validate_transform_step_config(
    node_id: str, step: Mapping[str, Any], *, step_suffix: str = ""
) -> None:
    from cdf_fn_common.etl_transform.step_io import validate_transform_step_io

    label = f"{node_id!r}{step_suffix}"
    try:
        validate_transform_step_io(step, context=f"transform node {label}")
    except ValueError as ex:
        raise CanvasCompileError(str(ex)) from ex


def _validate_raw_query_config(node_id: str, cfg: Mapping[str, Any]) -> None:
    source_db = str(cfg.get("source_raw_db") or "").strip()
    source_table = str(
        cfg.get("source_raw_table")
        or cfg.get("source_raw_table_key")
        or ""
    ).strip()
    if not source_db or not source_table:
        raise CanvasCompileError(
            f"query_raw node {node_id!r}: source_raw_db and "
            "source_raw_table/source_raw_table_key are required"
        )


def validate_transform_canvas_config(node_id: str, cfg: Mapping[str, Any]) -> None:
    """Validate transform node config: I/O fields, execution block, handler-specific rules."""
    if cfg.get("enabled") is False:
        return
    from cdf_fn_common.etl_pipeline_steps import (
        EXECUTION_PARALLEL,
        materialize_transform_steps,
        validate_execution_block,
    )
    from cdf_fn_common.etl_property_merge import parse_field_policies
    from cdf_fn_common.etl_transform.row_pipeline import validate_transform_config

    context = f"transform node {node_id!r}"
    validate_execution_block(cfg, context=context)
    mode, steps = materialize_transform_steps(cfg)
    if not steps:
        raise CanvasCompileError(f"{context}: handler_id or steps is required")

    enabled_steps = False
    for index, step in enumerate(steps):
        suffix = f" step[{index}]" if len(steps) > 1 else ""
        if step.get("enabled") is False:
            continue
        enabled_steps = True
        _validate_transform_step_config(node_id, step, step_suffix=suffix)
        try:
            validate_transform_config(step)
        except ValueError as ex:
            raise CanvasCompileError(f"{context}{suffix}: {ex}") from ex

    if not enabled_steps:
        return
    if mode == EXECUTION_PARALLEL:
        try:
            parse_field_policies(cfg)
        except ValueError as ex:
            raise CanvasCompileError(f"{context}: {ex}") from ex


def _reject_removed_mapping_node_kinds(nodes: Sequence[Mapping[str, Any]]) -> None:
    removed = {
        "field_map": "field_map",
        "annotation_map": "annotation_map",
    }
    for n in nodes:
        if not isinstance(n, dict):
            continue
        kind = _node_kind(n)
        if kind not in removed:
            continue
        node_id = str(n.get("id") or kind).strip()
        raise CanvasCompileError(
            f"canvas node {node_id!r} uses removed kind {kind!r}; "
            "use kind 'json_mapping' (CDF jsonMapping task)."
        )


def validate_canvas_dag(canvas: Mapping[str, Any]) -> List[str]:
    """Collect all validation errors without failing fast."""
    errors: List[str] = []
    nodes = list(canvas.get("nodes") or [])
    try:
        _reject_removed_mapping_node_kinds(nodes)
    except CanvasCompileError as ex:
        errors.append(str(ex))
    _normalize_mislabeled_fanout_plan_nodes(nodes)
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

    canvas_only_ids = _canvas_only_node_ids(nodes)
    for n in nodes:
        if not isinstance(n, dict):
            continue
        kind = _node_kind(n)
        if kind in STRUCTURAL_KINDS:
            continue
        if kind not in _KIND_SPEC:
            continue
        node_id = str(n.get("id") or kind).strip()
        data = n.get("data") if isinstance(n.get("data"), dict) else {}
        cfg = data.get("config") if isinstance(data.get("config"), dict) else {}

        if kind == "function_ref" and not str(cfg.get("function_external_id") or "").strip():
            errors.append(f"function_ref node {node_id!r}: function_external_id is required")
        if kind == "transformation_ref" and not str(cfg.get("transformation_external_id") or "").strip():
            errors.append(f"transformation_ref node {node_id!r}: transformation_external_id is required")
        if kind == "subworkflow" and not str(cfg.get("workflow_external_id") or "").strip():
            errors.append(f"subworkflow node {node_id!r}: workflow_external_id is required")
        if kind == "transform":
            try:
                validate_transform_canvas_config(node_id, cfg)
            except CanvasCompileError as ex:
                errors.append(str(ex))
        if kind == "query_raw":
            try:
                _validate_raw_query_config(node_id, cfg)
            except CanvasCompileError as ex:
                errors.append(str(ex))

        deps = _resolve_executable_predecessors(
            node_id,
            pred,
            executable_ids=executable_ids,
            canvas_only_ids=canvas_only_ids,
        )
        try:
            _validate_cohort_consumer_predecessors(node_id, kind, deps, node_by_id)
        except CanvasCompileError as ex:
            errors.append(str(ex))

        if kind == "join":
            try:
                _join_input_task_ids_from_edges(
                    node_id,
                    edges,
                    node_by_id=node_by_id,
                    executable_ids=executable_ids,
                )
            except CanvasCompileError as ex:
                errors.append(str(ex))
        elif kind == "file_annotation":
            try:
                _file_annotation_input_task_ids_from_edges(
                    node_id,
                    edges,
                    node_by_id=node_by_id,
                    executable_ids=executable_ids,
                    require_right=not _config_has_file_ids(cfg),
                )
            except CanvasCompileError as ex:
                errors.append(str(ex))
        elif kind == "workflow_fanout_plan":
            profile = str(cfg.get("fanout_profile") or "file_annotation").strip().lower()
            require_b = profile == "file_annotation" and not _config_has_file_ids(cfg)
            try:
                _dual_input_task_ids_from_edges(
                    node_id,
                    edges,
                    handle_left=FANOUT_PLAN_HANDLE_INPUT_A,
                    handle_right=FANOUT_PLAN_HANDLE_INPUT_B,
                    node_by_id=node_by_id,
                    executable_ids=executable_ids,
                    node_kind_label="workflow_fanout_plan",
                    require_right=require_b,
                )
            except CanvasCompileError as ex:
                errors.append(str(ex))
        elif kind == "json_mapping":
            from cdf_fn_common.etl_annotation_map.kuiper_templates import (
                enrich_json_mapping_config_for_compile,
                is_diagram_mapper_kind,
            )

            enriched = enrich_json_mapping_config_for_compile(cfg)
            try:
                _validate_json_mapping_config(node_id, enriched)
            except CanvasCompileError as ex:
                errors.append(str(ex))
            if is_diagram_mapper_kind(str(enriched.get("mapper_kind") or "")) and len(deps) != 1:
                errors.append(
                    f"json_mapping node {node_id!r} with mapper_kind="
                    f"{enriched.get('mapper_kind')!r} requires exactly one predecessor "
                    f"(typically fanout), got {deps!r}"
                )

    return errors


def compile_canvas_dag(canvas: Mapping[str, Any]) -> Dict[str, Any]:
    """Map executable canvas nodes to compiled_workflow tasks."""
    nodes = list(canvas.get("nodes") or [])
    _reject_removed_mapping_node_kinds(nodes)
    _normalize_mislabeled_fanout_plan_nodes(nodes)
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

    canvas_only_ids = _canvas_only_node_ids(nodes)

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
        if bool(cfg.get("lookup_full_scan")) and kind not in _QUERY_KINDS:
            raise CanvasCompileError(
                f"{kind} node {node_id!r}: lookup_full_scan is only supported on query nodes"
            )
        if kind == "function_ref" and not str(cfg.get("function_external_id") or "").strip():
            raise CanvasCompileError(
                f"function_ref node {node_id!r}: function_external_id is required"
            )
        if kind == "transformation_ref" and not str(
            cfg.get("transformation_external_id") or ""
        ).strip():
            raise CanvasCompileError(
                f"transformation_ref node {node_id!r}: transformation_external_id is required"
            )
        if kind == "subworkflow" and not str(cfg.get("workflow_external_id") or "").strip():
            raise CanvasCompileError(
                f"subworkflow node {node_id!r}: workflow_external_id is required"
            )
        if kind == "transform":
            validate_transform_canvas_config(node_id, cfg)
        if kind == "query_raw":
            _validate_raw_query_config(node_id, cfg)
        deps = _resolve_executable_predecessors(
            node_id,
            pred,
            executable_ids=executable_ids,
            canvas_only_ids=canvas_only_ids,
        )
        _validate_cohort_consumer_predecessors(node_id, kind, deps, node_by_id)
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
        elif kind == "file_annotation":
            entity_tids, files_tid = _file_annotation_input_task_ids_from_edges(
                node_id,
                edges,
                node_by_id=node_by_id,
                executable_ids=executable_ids,
                require_right=not _config_has_file_ids(cfg),
            )
            payload["entities_input_task_id"] = entity_tids[0]
            payload["entities_input_task_ids"] = entity_tids
            if files_tid:
                payload["files_input_task_id"] = files_tid
        elif kind == "workflow_fanout_plan":
            profile = str(cfg.get("fanout_profile") or "file_annotation").strip().lower()
            require_b = profile == "file_annotation" and not _config_has_file_ids(cfg)
            input_a_tid, input_b_tid = _dual_input_task_ids_from_edges(
                node_id,
                edges,
                handle_left=FANOUT_PLAN_HANDLE_INPUT_A,
                handle_right=FANOUT_PLAN_HANDLE_INPUT_B,
                node_by_id=node_by_id,
                executable_ids=executable_ids,
                node_kind_label="workflow_fanout_plan",
                require_right=require_b,
            )
            payload["input_a_task_id"] = input_a_tid
            if input_b_tid:
                payload["input_b_task_id"] = input_b_tid
            payload["fanout_profile"] = profile
        elif kind == "json_mapping":
            from cdf_fn_common.etl_annotation_map.kuiper_templates import (
                enrich_json_mapping_config_for_compile,
                is_diagram_mapper_kind,
            )

            enriched = enrich_json_mapping_config_for_compile(cfg)
            payload["config"] = enriched
            _validate_json_mapping_config(node_id, enriched)
            if is_diagram_mapper_kind(str(enriched.get("mapper_kind") or "")):
                if len(deps) != 1:
                    raise CanvasCompileError(
                        f"json_mapping node {node_id!r} with mapper_kind="
                        f"{enriched.get('mapper_kind')!r} requires exactly one predecessor "
                        f"(typically fanout), got {deps!r}"
                    )
                payload["source_task_id"] = deps[0]
        label = ""
        if isinstance(data.get("label"), str):
            label = str(data.get("label") or "").strip()
        on_failure = str(cfg.get("on_failure") or cfg.get("onFailure") or "").strip()
        if not on_failure and kind == "cdf_task":
            on_failure = "skipTask"
        task_entry: Dict[str, Any] = {
            "id": node_id,
            "function_external_id": _resolved_function_external_id(kind, cfg, fn_ext),
            "executable_kind": exec_kind,
            "task_type": task_type,
            "canvas_node_id": node_id,
            "label": label,
            "depends_on": deps,
            "payload": payload,
        }
        if on_failure:
            task_entry["on_failure"] = on_failure
        tasks.append(task_entry)

    if not any(t.get("executable_kind") == "raw_cleanup" for t in tasks):
        end_cleanups = _end_node_cleanup_tasks(
            nodes,
            pred=pred,
            executable_ids=executable_ids,
            canvas_only_ids=canvas_only_ids,
        )
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
