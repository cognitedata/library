"""Topological executor for compiled ETL workflow DAG."""

from __future__ import annotations

import importlib
import logging
import time
from threading import Lock
from typing import Any, Dict, List, Mapping, MutableMapping, Sequence, Set, Tuple

from cdf_fn_common.etl_predecessor_mode import use_in_memory_predecessors
from cdf_fn_common.workflow_compile.canvas_dag import etl_local_pipeline_specs
from local_runner.ephemeral_transformation import (
    ephemeral_transformation_external_id,
    run_ephemeral_transformation,
)
from local_runner.run_context import disabled_canvas_task_ids
from local_runner.ui_progress import emit_ui_progress, ui_progress_row_counts

_PIPELINES = etl_local_pipeline_specs()

_ETL_RAW_HASH_INDEX_LOCK = Lock()


def _etl_raw_hash_index_getter(shared_data: MutableMapping[str, Any]):
    """
    Return a callable ``(client, raw_db, raw_table, workflow_scope) -> scope_key -> node_id -> hash``.

    Caches one full-table build per distinct (raw_db, raw_table, workflow_scope) for the lifetime
    of *shared_data* so parallel ``fn_etl_view_query`` tasks in one local DAG share a single RAW scan.
    """

    def getter(
        client: Any, raw_db: str, raw_table: str, workflow_scope: str = ""
    ) -> Dict[str, Dict[str, str]]:
        from cdf_fn_common.etl_incremental_scope import build_latest_hash_index_for_table

        db = str(raw_db or "").strip()
        tbl = str(raw_table or "").strip()
        ws = str(workflow_scope or "").strip()
        if not db or not tbl:
            return {}
        key: Tuple[str, str, str] = (db, tbl, ws)
        store = shared_data.setdefault("_etl_raw_hash_index_store", {})
        with _ETL_RAW_HASH_INDEX_LOCK:
            if key not in store:
                store[key] = build_latest_hash_index_for_table(
                    client, db, tbl, workflow_scope=ws
                )
            return store[key]

    return getter


def _ui_task_progress_fields(task: Mapping[str, Any], task_id: str) -> Dict[str, Any]:
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


def _task_end_status(summary: Mapping[str, Any]) -> str:
    raw = str(summary.get("status") or "succeeded").strip().lower()
    if raw in {"failed", "error"}:
        return "failed"
    if raw in {"completed_with_errors", "warning", "skipped"} and summary.get("reason"):
        return "completed_with_errors"
    if raw == "failed":
        return "failed"
    return "succeeded"


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


def _task_config(task: Mapping[str, Any]) -> Dict[str, Any]:
    payload = task.get("payload")
    if isinstance(payload, dict):
        cfg = payload.get("config")
        if isinstance(cfg, dict):
            return dict(cfg)
    return {}


def _run_transformation_task(
    task: Mapping[str, Any],
    *,
    client: Any,
    logger: logging.Logger,
    shared_data: Mapping[str, Any],
    dry_run: bool,
) -> Dict[str, Any]:
    task_id = str(task.get("id") or "").strip()
    cfg = _task_config(task)
    exec_kind = str(task.get("executable_kind") or "").strip()
    run_id = str(shared_data.get("run_id") or "")

    if dry_run or client is None:
        return {
            "status": "skipped",
            "reason": "dry_run",
            "task_id": task_id,
            "executable_kind": exec_kind,
        }

    if exec_kind == "transformation_ref":
        ext = str(cfg.get("transformation_external_id") or "").strip()
        if not ext:
            raise ValueError(f"transformation_external_id required for task {task_id!r}")
        job = client.transformations.run(transformation_external_id=ext, wait=True, timeout=600.0)
        status = str(getattr(job, "status", "") or "").upper()
        if status == "FAILED":
            raise RuntimeError(str(getattr(job, "error", "") or f"transformation {ext!r} failed"))
        return {
            "status": status.lower() if status else "completed",
            "task_id": task_id,
            "transformation_external_id": ext,
            "job_id": getattr(job, "id", None),
        }

    query = str(cfg.get("query") or cfg.get("sql_query") or "").strip()
    destination = cfg.get("destination") if isinstance(cfg.get("destination"), dict) else None
    ext = ephemeral_transformation_external_id(task_id=task_id, run_id=run_id or task_id)
    result = run_ephemeral_transformation(
        client,
        query=query,
        destination=destination,
        external_id=ext,
        log=logger,
    )
    return {"task_id": task_id, **result}


def _run_orchestration_task(
    task: Mapping[str, Any],
    *,
    dry_run: bool,
) -> Dict[str, Any]:
    task_id = str(task.get("id") or "").strip()
    task_type = str(task.get("task_type") or "").strip()
    return {
        "status": "skipped",
        "reason": "orchestration_not_supported_locally",
        "task_id": task_id,
        "task_type": task_type,
        "dry_run": dry_run,
    }


def run_compiled_workflow_dag(
    compiled: Mapping[str, Any],
    *,
    client: Any,
    logger: logging.Logger,
    shared_data: MutableMapping[str, Any],
    dry_run: bool = False,
) -> Dict[str, Any]:
    configuration = shared_data.get("configuration")
    disabled_ids = disabled_canvas_task_ids(configuration) if isinstance(configuration, dict) else set()

    tasks = [t for t in (compiled.get("tasks") or []) if isinstance(t, dict) and t.get("id")]
    task_by_id = {str(t["id"]): t for t in tasks}
    pred_map: Dict[str, Set[str]] = {}
    for t in tasks:
        tid = str(t["id"])
        pred_map[tid] = {str(d) for d in (t.get("depends_on") or []) if str(d).strip()}

    summaries: Dict[str, Any] = {}
    for layer in _topological_layers(list(task_by_id.keys()), pred_map):
        for task_id in layer:
            task = task_by_id[task_id]
            canvas_node_id = str(task.get("canvas_node_id") or task_id).strip()
            if canvas_node_id in disabled_ids:
                logger.info("Skipping disabled canvas node %s (task %s)", canvas_node_id, task_id)
                summaries[task_id] = {
                    "status": "skipped",
                    "reason": "disabled",
                    "task_id": task_id,
                }
                continue

            t0 = time.perf_counter()
            emit_ui_progress("task_start", **_ui_task_progress_fields(task, task_id))
            try:
                task_type = str(task.get("task_type") or "function").strip()
                if task_type == "transformation":
                    logger.info("Running transformation task %s", task_id)
                    summaries[task_id] = _run_transformation_task(
                        task, client=client, logger=logger, shared_data=shared_data, dry_run=dry_run
                    )
                elif task_type != "function":
                    logger.info("Skipping orchestration task %s (%s)", task_id, task_type)
                    summaries[task_id] = _run_orchestration_task(task, dry_run=dry_run)
                else:
                    fn_ext = str(task.get("function_external_id") or "").strip()
                    spec = _PIPELINES.get(fn_ext)
                    if spec is None:
                        raise RuntimeError(f"No pipeline spec for {fn_ext!r}")
                    mod_name, entry = spec
                    mod = importlib.import_module(mod_name)
                    fn = getattr(mod, entry)
                    data: Dict[str, Any] = dict(shared_data)
                    data["task_id"] = task_id
                    data["compiled_task"] = task
                    compiled_wf = shared_data.get("compiled_workflow")
                    if isinstance(compiled_wf, dict):
                        data["compiled_workflow"] = compiled_wf
                    in_memory = use_in_memory_predecessors(data)
                    if in_memory:
                        buffers = shared_data.get("etl_task_row_buffers")
                        if isinstance(buffers, dict):
                            data["etl_task_row_buffers"] = buffers
                        if "_predecessor_rows" in shared_data:
                            data["_predecessor_rows"] = list(shared_data.get("_predecessor_rows") or [])
                        if "_predecessor_index_rows" in shared_data:
                            data["_predecessor_index_rows"] = list(
                                shared_data.get("_predecessor_index_rows") or []
                            )
                    elif not in_memory:
                        cache = shared_data.get("etl_cohort_row_index_cache")
                        if not isinstance(cache, dict):
                            cache = {}
                            shared_data["etl_cohort_row_index_cache"] = cache
                        data["etl_cohort_row_index_cache"] = cache
                    if fn_ext == "fn_etl_view_query":
                        data["etl_raw_hash_index_cache"] = _etl_raw_hash_index_getter(shared_data)
                    logger.info("Running task %s (%s.%s)", task_id, mod_name, entry)
                    summary = fn(fn_ext, data, client, logger)
                    summaries[task_id] = summary
                    if in_memory and "_predecessor_rows" in data:
                        shared_data["_predecessor_rows"] = data["_predecessor_rows"]
                        buffers = shared_data.get("etl_task_row_buffers")
                        if not isinstance(buffers, dict):
                            buffers = {}
                            shared_data["etl_task_row_buffers"] = buffers
                        buffers[task_id] = list(data["_predecessor_rows"])
                    elif in_memory and "_predecessor_index_rows" in data:
                        shared_data["_predecessor_index_rows"] = data["_predecessor_index_rows"]
                        shared_data.pop("_predecessor_rows", None)
                        buffers = shared_data.get("etl_task_row_buffers")
                        if not isinstance(buffers, dict):
                            buffers = {}
                            shared_data["etl_task_row_buffers"] = buffers
                        buffers[task_id] = list(data["_predecessor_index_rows"])
                    elif not in_memory:
                        shared_data.pop("_predecessor_rows", None)
                    if data.get("run_id"):
                        shared_data["run_id"] = data["run_id"]
            except Exception as exc:
                duration_sec = round(time.perf_counter() - t0, 6)
                emit_ui_progress(
                    "task_end",
                    **_ui_task_progress_fields(task, task_id),
                    status="failed",
                    error=f"{type(exc).__name__}: {exc}",
                    duration_sec=duration_sec,
                )
                raise
            else:
                duration_sec = round(time.perf_counter() - t0, 6)
                summary = summaries[task_id]
                if isinstance(summary, dict):
                    summary["duration_sec"] = duration_sec
                ui_end = _ui_task_progress_fields(task, task_id)
                ui_end["status"] = _task_end_status(summary if isinstance(summary, dict) else {})
                ui_end["duration_sec"] = duration_sec
                if isinstance(summary, dict):
                    ui_end.update(ui_progress_row_counts(summary))
                emit_ui_progress("task_end", **ui_end)
    return summaries
