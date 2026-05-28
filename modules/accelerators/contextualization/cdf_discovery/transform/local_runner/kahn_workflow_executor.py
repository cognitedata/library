"""Topological executor for compiled ETL workflow DAG."""

from __future__ import annotations

import importlib
import logging
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, List, Mapping, MutableMapping, Sequence, Set, Tuple

from cdf_fn_common.etl_common import iter_predecessor_rows_for_task
from cdf_fn_common.etl_predecessor_mode import use_in_memory_predecessors
from cdf_fn_common.etl_ui_progress import bind_handler_progress, clear_handler_progress
from cdf_fn_common.workflow_compile.canvas_dag import etl_local_pipeline_specs
from local_runner.dynamic_fanout import run_local_dynamic_fanout
from local_runner.ephemeral_transformation import (
    ephemeral_transformation_external_id,
    run_ephemeral_transformation,
)
from local_runner.json_mapping import run_local_json_mapping_task
from local_runner.parallel import (
    LockedCogniteClient,
    merge_shared_task_result,
    resolve_max_workers,
    run_parallel,
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
    cnv = str(task.get("canvas_node_id") or task_id).strip()
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


def _effective_function_external_id(task: Mapping[str, Any]) -> str:
    fn_ext = str(task.get("function_external_id") or "").strip()
    if fn_ext:
        return fn_ext
    return str(_task_config(task).get("function_external_id") or "").strip()


def _work_item_is_raw_cleanup(work: "_LayerTaskWork") -> bool:
    if work.skipped:
        return False
    task = work.task
    if str(task.get("executable_kind") or "").strip() == "raw_cleanup":
        return True
    return _effective_function_external_id(task) == "fn_etl_raw_cleanup"


def _maybe_run_preview_snapshots_before_cleanup(
    *,
    work_items: Sequence["_LayerTaskWork"],
    shared_data: MutableMapping[str, Any],
    summaries: Mapping[str, Any],
    client: Any,
    dry_run: bool,
    logger: logging.Logger,
) -> None:
    """Snapshot preview nodes while cohort RAW tables still exist (before ``raw_cleanup``)."""
    if dry_run or client is None:
        return
    if not any(_work_item_is_raw_cleanup(w) for w in work_items):
        return
    if shared_data.get("_preview_snapshots_done"):
        return
    configuration = shared_data.get("configuration")
    if not isinstance(configuration, dict):
        return
    from local_runner.preview_nodes import run_canvas_preview_snapshots

    previews = run_canvas_preview_snapshots(
        configuration,
        shared_data,
        client=client,
        task_summaries=summaries,
        dry_run=False,
        log=logger,
    )
    shared_data["_preview_snapshots_done"] = True
    shared_data["_preview_snapshots"] = previews


def _seed_in_memory_predecessors(data: MutableMapping[str, Any], task_id: str) -> None:
    """Populate task-local ``_predecessor_rows`` from completed predecessor buffers."""
    rows: List[Dict[str, Any]] = []
    for cols, props in iter_predecessor_rows_for_task(data, task_id):
        rows.append({"columns": dict(cols), "properties": dict(props)})
    if rows:
        data["_predecessor_rows"] = rows
    else:
        data.pop("_predecessor_rows", None)


def _run_cdf_function_call_task(
    task: Mapping[str, Any],
    *,
    fn_ext: str,
    client: Any,
    logger: logging.Logger,
    shared_data: Mapping[str, Any],
) -> Dict[str, Any]:
    task_id = str(task.get("id") or "").strip()
    configuration = shared_data.get("configuration")
    data: Dict[str, Any] = {
        "task_id": task_id,
        "incremental_change_processing": shared_data.get("incremental_change_processing", True),
        "run_id": str(shared_data.get("run_id") or ""),
        "configuration": configuration if isinstance(configuration, dict) else {},
    }
    logger.info("Calling CDF function %s for task %s", fn_ext, task_id)
    call = client.functions.call(external_id=fn_ext, data=data, wait=True)
    status = str(getattr(call, "status", "") or "").strip().lower()
    if status in {"failed", "error"}:
        raise RuntimeError(
            f"CDF function {fn_ext!r} call failed for task {task_id!r} (status={status!r})"
        )
    return {
        "status": status or "completed",
        "task_id": task_id,
        "function_external_id": fn_ext,
        "function_call_id": getattr(call, "id", None),
    }


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


@dataclass
class _LayerTaskWork:
    task_id: str
    task: Dict[str, Any]
    skipped: bool
    skip_summary: Dict[str, Any] | None


@dataclass
class _LayerTaskResult:
    task_id: str
    summary: Dict[str, Any]
    task_data: Dict[str, Any] | None
    in_memory: bool


def _prepare_task_data(
    task: Mapping[str, Any],
    *,
    task_id: str,
    shared_data: Mapping[str, Any],
    dry_run: bool = False,
) -> Dict[str, Any]:
    data: Dict[str, Any] = dict(shared_data)
    data["task_id"] = task_id
    data["compiled_task"] = task
    if dry_run:
        data["dry_run"] = True
        data.setdefault("local_predecessor_mode", "in_memory")
    compiled_wf = shared_data.get("compiled_workflow")
    if isinstance(compiled_wf, dict):
        data["compiled_workflow"] = compiled_wf
    in_memory = use_in_memory_predecessors(data)
    if in_memory:
        buffers = shared_data.get("etl_task_row_buffers")
        if isinstance(buffers, dict):
            data["etl_task_row_buffers"] = buffers
        _seed_in_memory_predecessors(data, task_id)
        if "_predecessor_index_rows" in shared_data:
            data["_predecessor_index_rows"] = list(shared_data.get("_predecessor_index_rows") or [])
    else:
        cache = shared_data.get("etl_cohort_row_index_cache")
        if isinstance(cache, dict):
            data["etl_cohort_row_index_cache"] = cache
    return data


def _run_single_compiled_task(
    work: _LayerTaskWork,
    *,
    client: Any,
    shared_data: MutableMapping[str, Any],
    summaries: Mapping[str, Any],
    dry_run: bool,
    logger: logging.Logger,
    fanout_max_workers: int | None,
    shared_lock: Lock,
    client_lock: Lock,
) -> _LayerTaskResult:
    task_id = work.task_id
    task = work.task
    if work.skipped:
        assert work.skip_summary is not None
        return _LayerTaskResult(
            task_id=task_id,
            summary=work.skip_summary,
            task_data=None,
            in_memory=False,
        )

    t0 = time.perf_counter()
    emit_ui_progress("task_start", **_ui_task_progress_fields(task, task_id))
    task_data: Dict[str, Any] | None = None
    in_memory = False
    try:
        task_type = str(task.get("task_type") or "function").strip()
        if task_type == "transformation":
            logger.info("Running transformation task %s", task_id)
            summary = _run_transformation_task(
                task, client=client, logger=logger, shared_data=shared_data, dry_run=dry_run
            )
        elif task_type == "dynamic":
            if dry_run or client is None:
                summary = _run_orchestration_task(task, dry_run=dry_run)
            else:
                logger.info("Running dynamic fan-out task %s locally", task_id)
                configuration = shared_data.get("configuration")
                plan_summary = summaries.get(
                    str(_task_config(task).get("generator_task_id") or "fanout_plan")
                )
                child_count = 1
                if isinstance(plan_summary, dict):
                    tasks_list = plan_summary.get("tasks")
                    if isinstance(tasks_list, list):
                        child_count = max(1, len(tasks_list))
                fanout_workers = resolve_max_workers(
                    layer_size=child_count,
                    override=fanout_max_workers,
                    configuration=configuration if isinstance(configuration, dict) else None,
                )
                summary = run_local_dynamic_fanout(
                    task,
                    summaries=summaries,
                    shared_data=shared_data,
                    client=client,
                    logger=logger,
                    max_workers=fanout_workers,
                    client_lock=client_lock,
                    shared_lock=shared_lock,
                )
        elif task_type == "jsonMapping":
            logger.info("Running jsonMapping task %s locally", task_id)
            summary = run_local_json_mapping_task(
                task,
                summaries=summaries,
                shared_data=shared_data,
                client=client,
                logger=logger,
                dry_run=dry_run,
            )
        elif task_type != "function":
            logger.info("Skipping orchestration task %s (%s)", task_id, task_type)
            summary = _run_orchestration_task(task, dry_run=dry_run)
        else:
            fn_ext = _effective_function_external_id(task)
            if not fn_ext:
                raise RuntimeError(f"function_external_id required for task {task_id!r}")
            spec = _PIPELINES.get(fn_ext)
            if spec is None:
                exec_kind = str(task.get("executable_kind") or "").strip()
                if exec_kind == "function_ref":
                    if dry_run or client is None:
                        summary = {
                            "status": "skipped",
                            "reason": "dry_run",
                            "task_id": task_id,
                            "function_external_id": fn_ext,
                        }
                    else:
                        summary = _run_cdf_function_call_task(
                            task,
                            fn_ext=fn_ext,
                            client=client,
                            logger=logger,
                            shared_data=shared_data,
                        )
                else:
                    raise RuntimeError(f"No pipeline spec for {fn_ext!r}")
            else:
                mod_name, entry = spec
                mod = importlib.import_module(mod_name)
                fn = getattr(mod, entry)
                data = _prepare_task_data(
                    task, task_id=task_id, shared_data=shared_data, dry_run=dry_run
                )
                in_memory = use_in_memory_predecessors(data)
                if fn_ext == "fn_etl_view_query":
                    data["etl_raw_hash_index_cache"] = _etl_raw_hash_index_getter(shared_data)
                logger.info("Running task %s (%s.%s)", task_id, mod_name, entry)
                bind_handler_progress(data)
                try:
                    summary = fn(fn_ext, data, client, logger)
                finally:
                    clear_handler_progress()
                task_data = data
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
        if isinstance(summary, dict):
            summary = dict(summary)
            summary["duration_sec"] = duration_sec
        ui_end = _ui_task_progress_fields(task, task_id)
        ui_end["status"] = _task_end_status(summary if isinstance(summary, dict) else {})
        ui_end["duration_sec"] = duration_sec
        if isinstance(summary, dict):
            ui_end.update(ui_progress_row_counts(summary))
        emit_ui_progress("task_end", **ui_end)
    return _LayerTaskResult(
        task_id=task_id,
        summary=summary if isinstance(summary, dict) else {"status": "succeeded", "task_id": task_id},
        task_data=task_data,
        in_memory=in_memory,
    )


def run_compiled_workflow_dag(
    compiled: Mapping[str, Any],
    *,
    client: Any,
    logger: logging.Logger,
    shared_data: MutableMapping[str, Any],
    dry_run: bool = False,
    max_workers: int | None = None,
) -> Dict[str, Any]:
    configuration = shared_data.get("configuration")
    disabled_ids = disabled_canvas_task_ids(configuration) if isinstance(configuration, dict) else set()
    cfg = configuration if isinstance(configuration, dict) else None

    tasks = [t for t in (compiled.get("tasks") or []) if isinstance(t, dict) and t.get("id")]
    task_by_id = {str(t["id"]): t for t in tasks}
    pred_map: Dict[str, Set[str]] = {}
    for t in tasks:
        tid = str(t["id"])
        pred_map[tid] = {str(d) for d in (t.get("depends_on") or []) if str(d).strip()}

    summaries: Dict[str, Any] = {}
    if not isinstance(shared_data.get("etl_cohort_row_index_cache"), dict):
        shared_data["etl_cohort_row_index_cache"] = {}
    client_lock = Lock()
    shared_lock = Lock()
    run_client: Any = client
    if client is not None and not dry_run:
        run_client = LockedCogniteClient(client, client_lock)

    for layer in _topological_layers(list(task_by_id.keys()), pred_map):
        work_items: List[_LayerTaskWork] = []
        for task_id in layer:
            task = task_by_id[task_id]
            canvas_node_id = str(task.get("canvas_node_id") or task_id).strip()
            if canvas_node_id in disabled_ids:
                logger.info("Skipping disabled canvas node %s (task %s)", canvas_node_id, task_id)
                work_items.append(
                    _LayerTaskWork(
                        task_id=task_id,
                        task=task,
                        skipped=True,
                        skip_summary={
                            "status": "skipped",
                            "reason": "disabled",
                            "task_id": task_id,
                        },
                    )
                )
            else:
                work_items.append(
                    _LayerTaskWork(
                        task_id=task_id,
                        task=task,
                        skipped=False,
                        skip_summary=None,
                    )
                )

        _maybe_run_preview_snapshots_before_cleanup(
            work_items=work_items,
            shared_data=shared_data,
            summaries=summaries,
            client=client,
            dry_run=dry_run,
            logger=logger,
        )

        layer_workers = resolve_max_workers(
            layer_size=len(work_items),
            override=max_workers,
            configuration=cfg,
        )

        def _worker(work: _LayerTaskWork) -> _LayerTaskResult:
            return _run_single_compiled_task(
                work,
                client=run_client,
                shared_data=shared_data,
                summaries=summaries,
                dry_run=dry_run,
                logger=logger,
                fanout_max_workers=max_workers,
                shared_lock=shared_lock,
                client_lock=client_lock,
            )

        if layer_workers <= 1:
            results = [_worker(w) for w in work_items]
        else:
            results = run_parallel(work_items, _worker, max_workers=layer_workers)

        for result in results:
            summaries[result.task_id] = result.summary
            if result.task_data is not None:
                merge_shared_task_result(
                    shared_data,
                    shared_lock,
                    task_id=result.task_id,
                    summary=result.summary,
                    data=result.task_data,
                    in_memory=result.in_memory,
                )
    return summaries
