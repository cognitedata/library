"""Local workflow execution: topological runner over ``compiled_workflow`` tasks (discovery canvas)."""

from __future__ import annotations

import importlib
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from cdf_fn_common.task_runtime import merge_compiled_task_into_data
from cdf_fn_common.workflow_task_policy import (
    ON_FAILURE_SKIP_TASK,
    discovery_task_workflow_policy,
)
from cdf_fn_common.workflow_compile.canvas_dag import discovery_local_pipeline_specs
from cdf_fn_common.workflow_execution_graph import (
    default_execution_graph_path,
    load_execution_graph,
    validate_execution_graph,
)

from .kahn_run_context import KahnRunContext
from .persistence_cohort_snapshot import (
    PERSISTENCE_SNAPSHOT_FUNCTIONS,
    build_persistence_cohort_snapshot,
    parse_handler_summary_message,
)
from .raw_results_attachment import snapshot_raw_results_for_ctx
from .ui_progress import emit_ui_progress, ui_progress_row_counts_from_output

_DISCOVERY_PIPELINES: Dict[str, Tuple[str, str]] = discovery_local_pipeline_specs()


def _discovery_raw_hash_index_getter(ctx: KahnRunContext):
    """
    Return a callable ``(client, raw_db, raw_table, workflow_scope) -> scope_key -> node_id -> hash``.

    Caches one full-table build per distinct (raw_db, raw_table, workflow_scope) for the lifetime
    of *ctx* so parallel ``fn_dm_view_query`` tasks for one workflow share a single RAW scan.
    """

    def getter(
        client: Any, raw_db: str, raw_table: str, workflow_scope: str = ""
    ) -> Dict[str, Dict[str, str]]:
        from cdf_fn_common.incremental_scope import build_latest_hash_index_for_table

        db = str(raw_db or "").strip()
        tbl = str(raw_table or "").strip()
        ws = str(workflow_scope or "").strip()
        if not db or not tbl:
            return {}
        key = (db, tbl, ws)
        with ctx.raw_hash_index_lock:
            if key not in ctx.raw_hash_index_cache:
                ctx.raw_hash_index_cache[key] = build_latest_hash_index_for_table(
                    client, db, tbl, workflow_scope=ws
                )
            return ctx.raw_hash_index_cache[key]

    return getter


def _discovery_cohort_row_index_getter(ctx: KahnRunContext):
    """
    Return a callable ``(client, raw_db, raw_table) -> CohortRowIndex``.

    Caches one full-table build per distinct (raw_db, raw_table) for the lifetime of *ctx*
    so parallel transform / validate tasks share a single RAW scan per node table.
    """

    def getter(client: Any, raw_db: str, raw_table: str) -> Dict[str, Any]:
        from cdf_fn_common.cohort_storage import build_cohort_row_index

        db = str(raw_db or "").strip()
        tbl = str(raw_table or "").strip()
        if not db or not tbl:
            return {}
        key = (db, tbl)
        with ctx.cohort_row_index_lock:
            if key not in ctx.cohort_row_index_cache:
                ctx.cohort_row_index_cache[key] = build_cohort_row_index(client, db, tbl)
            return ctx.cohort_row_index_cache[key]

    return getter


def _discovery_cohort_row_index_invalidator(ctx: KahnRunContext):
    """Drop a cached cohort index so downstream tasks see rows written after the cache was built."""

    def invalidate(raw_db: str, raw_table: str) -> None:
        db = str(raw_db or "").strip()
        tbl = str(raw_table or "").strip()
        if not db or not tbl:
            return
        key = (db, tbl)
        with ctx.cohort_row_index_lock:
            ctx.cohort_row_index_cache.pop(key, None)

    return invalidate


# Persist handler ``data`` and predecessor cohort snapshots for save / inverted-index tasks.
_HANDLER_DATA_SNAPSHOT_FUNCTIONS: frozenset[str] = PERSISTENCE_SNAPSHOT_FUNCTIONS


def discovery_pipeline_spec_for_task(task: Dict[str, Any]) -> Tuple[str, str]:
    """
    Resolve (``importlib`` module path, pipeline entry function) for a compiled workflow task.

    Uses the registry built from :data:`canvas_dag._KIND_FN` only.
    """
    fn_ext = str(task.get("function_external_id") or "").strip()
    if not fn_ext:
        raise RuntimeError(f"compiled_workflow task missing function_external_id: {task!r}")
    spec = _DISCOVERY_PIPELINES.get(fn_ext)
    if spec is not None:
        return spec
    raise RuntimeError(
        f"Unknown discovery pipeline for function_external_id={fn_ext!r} (task {task.get('id')!r}). "
        "Register the executable canvas kind in cdf_fn_common.workflow_compile.canvas_dag._KIND_FN."
    )


def should_validate_macro_execution_graph(compiled_workflow: Optional[Dict[str, Any]]) -> bool:
    """True when the compiled DAG matches ``workflow_template/workflow.execution.graph.yaml``."""
    if not isinstance(compiled_workflow, dict):
        return False
    tasks = compiled_workflow.get("tasks")
    if not isinstance(tasks, list):
        return False
    ids = {str(t.get("id")) for t in tasks if isinstance(t, dict) and t.get("id")}
    gpath = default_execution_graph_path()
    graph = load_execution_graph(gpath)
    return ids == set(graph.nodes)


def validate_execution_graph_at_startup(
    module_root: Path,
    logger: logging.Logger,
    compiled_workflow: Optional[Dict[str, Any]] = None,
) -> None:
    gpath = default_execution_graph_path(module_root)
    graph = load_execution_graph(gpath)
    errs = validate_execution_graph(graph)
    if errs:
        raise RuntimeError(f"Invalid execution graph {gpath}: {'; '.join(errs)}")
    if compiled_workflow is not None and not should_validate_macro_execution_graph(compiled_workflow):
        logger.info(
            "Compiled workflow task set differs from %s (custom canvas); graph file is reference only.",
            gpath.name,
        )
    else:
        logger.info(
            "Execution graph: %s (%d nodes, %d edges)",
            gpath.name,
            len(graph.nodes),
            len(graph.edges),
        )


def _topological_layers(task_ids: Sequence[str], pred_map: Dict[str, Set[str]]) -> List[List[str]]:
    remaining = set(task_ids)
    completed: Set[str] = set()
    layers: List[List[str]] = []
    while remaining:
        ready = sorted(t for t in remaining if pred_map.get(t, set()) <= completed)
        if not ready:
            raise RuntimeError("compiled_workflow task graph has a cycle or invalid dependencies")
        layers.append(ready)
        completed.update(ready)
        remaining -= set(ready)
    return layers


def _task_index(compiled: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for t in compiled.get("tasks") or []:
        if isinstance(t, dict) and t.get("id"):
            out[str(t["id"])] = t
    return out


def _ui_task_progress_fields(ctx: KahnRunContext, task_id: str) -> Dict[str, Any]:
    task = _task_index(ctx.compiled_workflow).get(task_id) or {}
    fn_ext = str(task.get("function_external_id") or "").strip()
    cnv = str(task.get("canvas_node_id") or "").strip()
    pnode = str(task.get("pipeline_node_id") or "").strip()
    out: Dict[str, Any] = {"task_id": task_id}
    if fn_ext:
        out["function_external_id"] = fn_ext
    if cnv:
        out["canvas_node_id"] = cnv
    if pnode and pnode != task_id:
        out["pipeline_node_id"] = pnode
    return out


def _compiled_task_snapshot(ctx: KahnRunContext, task_id: str) -> Dict[str, Any]:
    t = _task_index(ctx.compiled_workflow).get(task_id) or {}
    deps = t.get("depends_on")
    return {
        "task_id": task_id,
        "function_external_id": str(t.get("function_external_id") or ""),
        "depends_on": list(deps) if isinstance(deps, list) else [],
        "canvas_node_id": (str(t.get("canvas_node_id")).strip() or None)
        if t.get("canvas_node_id")
        else None,
        "pipeline_node_id": str(t.get("pipeline_node_id") or "").strip() or None,
    }


def _handler_data_for_run_results(data: Mapping[str, Any]) -> Dict[str, Any]:
    """
    JSON-serializable deep copy of the Cognite-function-style ``data`` payload.

    Stored under ``handler_data_snapshots`` keyed by ``task_id`` for persistence tasks
    (see ``*_discovery_run.json`` persistence.nodes).
    """
    try:
        return json.loads(json.dumps(dict(data), default=str))
    except Exception:
        return {
            "_handler_data_serialization_failed": True,
            "top_level_keys": sorted(str(k) for k in data.keys()),
        }


def _task_input_hint(ctx: KahnRunContext, fn_ext: str) -> Dict[str, Any]:
    sd = ctx.scope_document if isinstance(ctx.scope_document, dict) else {}
    keys = sorted(sd.keys())
    hint: Dict[str, Any] = {"configuration_top_level_keys": keys[:80]}
    if fn_ext in _DISCOVERY_PIPELINES:
        hint["run_id"] = ctx.run_id
    return hint


def _task_output_snapshot(ctx: KahnRunContext, task_id: str) -> Any:
    if str(task_id) in ctx.discovery_task_outputs:
        return ctx.discovery_task_outputs.get(str(task_id))
    return None


def _discovery_branch(ctx: KahnRunContext, task_id: str, merge_lock: Lock, spec: Tuple[str, str]) -> None:
    task = _task_index(ctx.compiled_workflow).get(task_id) or {}
    mod = importlib.import_module(spec[0])
    run_fn = getattr(mod, spec[1])
    data: Dict[str, Any] = {
        "logLevel": "INFO",
        "run_id": ctx.run_id,
        "run_all": bool(getattr(ctx.args, "run_all", False)),
        "configuration": ctx.scope_document,
        "instance_space": ctx.wf_instance_space,
        "compiled_workflow": ctx.compiled_workflow,
        "task_id": task_id,
        "discovery_raw_hash_index_cache": _discovery_raw_hash_index_getter(ctx),
        "discovery_cohort_row_index_cache": _discovery_cohort_row_index_getter(ctx),
        "discovery_cohort_row_index_invalidate": _discovery_cohort_row_index_invalidator(ctx),
    }
    merge_compiled_task_into_data(data)
    deps_raw = task.get("depends_on") if isinstance(task.get("depends_on"), list) else []
    pred_out: Dict[str, Any] = {}
    for d in deps_raw:
        ds = str(d).strip()
        if not ds:
            continue
        snap = ctx.discovery_task_outputs.get(ds)
        if snap is not None:
            pred_out[ds] = snap
            if str(snap.get("status") or "").strip().lower() == "failure":
                raise RuntimeError(
                    f"Predecessor task {ds!r} failed: {snap.get('message') or 'unknown error'}"
                )
    if pred_out:
        data["discovery_predecessor_outputs"] = pred_out
    run_fn(ctx.client, ctx.pipe_logger, data, ctx.cdf_config)
    snap = {"status": data.get("status"), "message": data.get("message")}
    if str(snap.get("status") or "").strip().lower() == "failure":
        raise RuntimeError(str(snap.get("message") or f"Task {task_id!r} failed"))

    def _merge_out() -> None:
        ctx.discovery_task_outputs[str(task_id)] = snap
        fn_ext = str(task.get("function_external_id") or "").strip()
        if fn_ext in _HANDLER_DATA_SNAPSHOT_FUNCTIONS:
            row_limit = int(getattr(ctx.args, "raw_results_rows", 500) or 500)
            _mrs = int(getattr(ctx.args, "raw_results_max_rows_scanned", 0) or 0)
            cohort_snapshot = build_persistence_cohort_snapshot(
                ctx.client,
                data,
                task_id=str(task_id),
                function_external_id=fn_ext,
                row_limit=max(1, row_limit),
                logger=ctx.logger,
                max_raw_rows_scanned=_mrs if _mrs > 0 else None,
            )
            ctx.handler_data_snapshots[str(task_id)] = {
                "task_id": str(task_id),
                "function_external_id": fn_ext,
                "handler_summary": parse_handler_summary_message(snap.get("message")),
                "handler_data": _handler_data_for_run_results(data),
                "cohort_snapshot": cohort_snapshot,
            }

    if merge_lock is not None:
        with merge_lock:
            _merge_out()
    else:
        _merge_out()


def _dispatch_task(ctx: KahnRunContext, task_id: str, merge_lock: Lock) -> None:
    idx = _task_index(ctx.compiled_workflow)
    task = idx.get(task_id)
    if not task:
        raise RuntimeError(f"Unknown task_id in compiled_workflow: {task_id!r}")
    fn_ext = str(task.get("function_external_id") or "")
    try:
        spec = discovery_pipeline_spec_for_task(task)
    except RuntimeError as ex:
        raise RuntimeError(
            f"Unsupported function_external_id for local runner: {fn_ext!r} (task {task_id!r})"
        ) from ex
    if fn_ext == "fn_dm_discovery_raw_cleanup":
        snapshot_raw_results_for_ctx(ctx)
    _discovery_branch(ctx, task_id, merge_lock, spec)


def _local_task_retries(ctx: KahnRunContext, function_external_id: str) -> int:
    """CDF ``retries`` count (not including the first attempt)."""
    raw_arg = getattr(ctx.args, "local_task_retries", None)
    if raw_arg is not None:
        try:
            return max(0, int(raw_arg))
        except (TypeError, ValueError):
            pass
    env = os.environ.get("KEA_LOCAL_TASK_RETRIES", "").strip()
    if env:
        try:
            return max(0, int(env))
        except ValueError:
            pass
    return int(discovery_task_workflow_policy(function_external_id)["retries"])


def _local_retry_delay_sec(attempt: int) -> float:
    """Seconds to wait before retry *attempt* (1-based after first failure)."""
    raw = os.environ.get("KEA_LOCAL_TASK_RETRY_DELAY_SEC", "1").strip()
    try:
        base = max(0.0, float(raw))
    except ValueError:
        base = 1.0
    if base <= 0:
        return 0.0
    return min(base * (2.0 ** max(0, attempt - 1)), 8.0)


def _dispatch_task_tracked(ctx: KahnRunContext, task_id: str, merge_lock: Lock) -> None:
    meta = _compiled_task_snapshot(ctx, task_id)
    fn_ext = str(meta.get("function_external_id") or "")
    policy = discovery_task_workflow_policy(fn_ext)
    retries = _local_task_retries(ctx, fn_ext)
    max_attempts = 1 + retries
    on_failure = str(policy.get("onFailure") or "abortWorkflow")

    emit_ui_progress("task_start", **_ui_task_progress_fields(ctx, task_id))
    t0 = time.perf_counter()
    err: Optional[str] = None
    output_snap: Any = None
    task_status = "succeeded"

    for attempt in range(1, max_attempts + 1):
        err = None
        try:
            _dispatch_task(ctx, task_id, merge_lock)
            output_snap = _task_output_snapshot(ctx, task_id)
            task_status = "succeeded"
            break
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            if attempt < max_attempts:
                delay = _local_retry_delay_sec(attempt)
                ctx.logger.warning(
                    "Task %s (%s) attempt %s/%s failed, retrying in %.1fs: %s",
                    task_id,
                    fn_ext or "?",
                    attempt,
                    max_attempts,
                    delay,
                    err,
                )
                if delay > 0:
                    time.sleep(delay)
                continue
            if on_failure == ON_FAILURE_SKIP_TASK:
                task_status = "completed_with_errors"
                warn = (
                    f"task {task_id} ({fn_ext}) failed after {max_attempts} attempt(s): {err}"
                )
                ctx.pipeline_warnings.append(warn)
                ctx.logger.warning(
                    "Task %s (%s) onFailure=skipTask — continuing pipeline: %s",
                    task_id,
                    fn_ext or "?",
                    err,
                )
                break
            task_status = "failed"
            raise

    dt = time.perf_counter() - t0
    ctx.logger.info(
        "pipeline_task_timing task_id=%s function_external_id=%s duration_sec=%.3f status=%s",
        task_id,
        fn_ext or "?",
        dt,
        task_status,
    )
    ctx.task_timings.append(
        {
            "task_id": task_id,
            "function_external_id": fn_ext or None,
            "duration_sec": round(dt, 6),
        }
    )
    ui_end = _ui_task_progress_fields(ctx, task_id)
    ui_end["status"] = task_status
    if err and task_status != "succeeded":
        _emax = 2000
        ui_end["error"] = err if len(err) <= _emax else err[:_emax] + "…"
    ui_end.update(ui_progress_row_counts_from_output(output_snap))
    emit_ui_progress("task_end", **ui_end)
    rec: Dict[str, Any] = {
        "task_id": task_id,
        "function_external_id": fn_ext or None,
        "compiled_task": meta,
        "input": _task_input_hint(ctx, fn_ext),
        "output": output_snap,
        "duration_sec": round(dt, 6),
        "status": task_status,
    }
    if err and task_status != "succeeded":
        rec["error"] = err
    ctx.local_run_tasks.append(rec)


def run_compiled_workflow_dag(ctx: KahnRunContext) -> None:
    """Execute every task in ``ctx.compiled_workflow`` in topological order; parallelize independent tasks."""
    cw = ctx.compiled_workflow
    tasks_raw = cw.get("tasks") if isinstance(cw, dict) else None
    if not isinstance(tasks_raw, list) or not tasks_raw:
        raise RuntimeError("compiled_workflow.tasks is missing or empty")
    task_ids: List[str] = []
    pred_map: Dict[str, Set[str]] = {}
    for t in tasks_raw:
        if not isinstance(t, dict) or not t.get("id"):
            continue
        tid = str(t["id"])
        task_ids.append(tid)
        deps = t.get("depends_on") if isinstance(t.get("depends_on"), list) else []
        pred_map[tid] = {str(d) for d in deps if d}
    layers = _topological_layers(task_ids, pred_map)
    merge_lock = Lock()
    for layer in layers:
        if len(layer) == 1:
            _dispatch_task_tracked(ctx, layer[0], merge_lock)
        else:
            with ThreadPoolExecutor(max_workers=len(layer)) as ex:
                futs = [ex.submit(_dispatch_task_tracked, ctx, tid, merge_lock) for tid in layer]
                for fut in as_completed(futs):
                    fut.result()
