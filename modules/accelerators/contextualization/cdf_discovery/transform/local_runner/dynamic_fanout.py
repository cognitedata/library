"""Run dynamic fan-out child function tasks during local ETL DAG execution."""

from __future__ import annotations

import copy
import importlib
import logging
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, List, Mapping, MutableMapping

from cdf_fn_common.etl_cohort_handoff import write_entity_rows_to_cohort_sink
from cdf_fn_common.etl_cohort_storage import (
    canvas_node_id_for_task,
    iter_cohort_entity_rows,
    node_cohort_table_name,
    require_run_id,
    resolve_base_cohort_table,
)
from cdf_fn_common.etl_discovery_cohort import _props_from_row_columns
from cdf_fn_common.etl_discovery_query_shared import RECORD_KIND_ENTITY
from cdf_fn_common.etl_file_processing_state import resolve_file_workflow_params
from cdf_fn_common.etl_incremental_scope import RECORD_KIND_COLUMN, raw_row_columns
from cdf_fn_common.etl_ui_progress import bind_handler_progress, clear_handler_progress
from cdf_fn_common.workflow_compile.canvas_dag import etl_local_pipeline_specs
from local_runner.parallel import run_parallel
from local_runner.ui_progress import emit_ui_progress


def _task_config(task: Mapping[str, Any]) -> Dict[str, Any]:
    payload = task.get("payload")
    if isinstance(payload, dict):
        cfg = payload.get("config")
        if isinstance(cfg, dict):
            return dict(cfg)
    return {}


def _resolve_child_function_data(
    child_data: Mapping[str, Any],
    shared_data: Mapping[str, Any],
) -> Dict[str, Any]:
    """Replace workflow template placeholders with local run context."""
    out = copy.deepcopy(dict(child_data))
    configuration = shared_data.get("configuration")
    if isinstance(configuration, Mapping):
        raw_cfg = out.get("configuration")
        if raw_cfg is None or (
            isinstance(raw_cfg, str) and "${workflow.input.configuration}" in raw_cfg
        ):
            out["configuration"] = dict(configuration)
    inc = out.get("incremental_change_processing")
    if isinstance(inc, str) and "${workflow.input" in inc:
        out["incremental_change_processing"] = shared_data.get(
            "incremental_change_processing", True
        )
    if not out.get("run_id"):
        out["run_id"] = shared_data.get("run_id")
    return out


def _cohort_entity_rows_from_table(
    client: Any,
    raw_db: str,
    raw_table: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in iter_cohort_entity_rows(client, raw_db, raw_table):
        cols = dict(raw_row_columns(row))
        if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
            continue
        props = _props_from_row_columns(cols)
        rows.append({"columns": cols, "properties": props})
    return rows


def _merge_child_cohort_into_fanout(
    client: Any,
    shared_data: MutableMapping[str, Any],
    *,
    fanout_task_id: str,
    child_task_ids: List[str],
    logger: logging.Logger,
    shared_lock: Lock,
) -> int:
    with shared_lock:
        run_id = require_run_id(shared_data)
        raw_db, base_table = resolve_base_cohort_table(shared_data)
        aggregated: List[Dict[str, Any]] = []
        for child_tid in child_task_ids:
            child_canvas = canvas_node_id_for_task(shared_data, child_tid)
            tbl = node_cohort_table_name(base_table, run_id, child_canvas)
            chunk = _cohort_entity_rows_from_table(client, raw_db, tbl)
            aggregated.extend(chunk)
            logger.info(
                "dynamic_fanout merged %s rows from child %s table %s/%s",
                len(chunk),
                child_tid,
                raw_db,
                tbl,
            )
        if not aggregated:
            return 0
        scope_key = resolve_file_workflow_params(shared_data)["workflow_scope"]
        handoff_data: Dict[str, Any] = dict(shared_data)
        handoff_data["task_id"] = fanout_task_id
        write_entity_rows_to_cohort_sink(
            client,
            handoff_data,
            run_id=run_id,
            scope_key=scope_key,
            task_id=fanout_task_id,
            query_source="file_annotation",
            entity_type="CogniteFile",
            view_space="cdf_cdm",
            view_external_id="CogniteFile",
            view_version="v1",
            rows=aggregated,
            log=logger,
        )
        return len(aggregated)


def _emit_fanout_batch_progress(
    *,
    fanout_task_id: str,
    fanout_canvas: str,
    completed_batches: int,
    total_batches: int,
    progress_lock: Lock,
) -> None:
    with progress_lock:
        emit_ui_progress(
            "task_progress",
            task_id=fanout_task_id,
            canvas_node_id=fanout_canvas,
            progress_current=max(0, int(completed_batches)),
            progress_total=max(0, int(total_batches)),
            progress_label="batches",
        )


@dataclass
class _FanoutChildWork:
    spec: Dict[str, Any]
    batch_index: int
    fn_ext: str
    child_tid: str
    child_data: Dict[str, Any]


@dataclass
class _FanoutChildResult:
    child_tid: str
    fn_ext: str
    batch_index: int
    summary: Dict[str, Any]
    duration_sec: float


def run_local_dynamic_fanout(
    task: Mapping[str, Any],
    *,
    summaries: Mapping[str, Any],
    shared_data: MutableMapping[str, Any],
    client: Any,
    logger: logging.Logger,
    max_workers: int = 1,
    client_lock: Lock | None = None,
    shared_lock: Lock | None = None,
) -> Dict[str, Any]:
    """
    Execute planner ``tasks`` locally and materialize cohort rows on the fan-out node table.
    """
    task_id = str(task.get("id") or "").strip()
    if not task_id:
        raise RuntimeError("dynamic_fanout requires non-empty task id")
    shared_data["task_id"] = task_id
    cfg = _task_config(task)
    generator_id = str(cfg.get("generator_task_id") or "fanout_plan").strip()
    plan_summary = summaries.get(generator_id)
    if not isinstance(plan_summary, dict):
        raise RuntimeError(
            f"dynamic_fanout {task_id!r}: missing planner summary for {generator_id!r}"
        )
    dynamic_specs = plan_summary.get("tasks")
    if not isinstance(dynamic_specs, list):
        dynamic_specs = []

    if not dynamic_specs:
        logger.warning(
            "dynamic_fanout %s: planner produced 0 child tasks (files_pending=%s)",
            task_id,
            plan_summary.get("files_pending"),
        )

    pipelines = etl_local_pipeline_specs()
    fanout_canvas = str(task.get("canvas_node_id") or task_id).strip()
    progress_lock = Lock()
    sink_lock = shared_lock if shared_lock is not None else Lock()

    runnable_work: List[_FanoutChildWork] = []
    for spec in dynamic_specs:
        if not isinstance(spec, dict):
            continue
        params = spec.get("parameters") if isinstance(spec.get("parameters"), dict) else {}
        fn_block = params.get("function") if isinstance(params.get("function"), dict) else {}
        fn_ext = str(fn_block.get("externalId") or "").strip()
        child_data = fn_block.get("data") if isinstance(fn_block.get("data"), dict) else {}
        child_tid = str(child_data.get("task_id") or spec.get("externalId") or "").strip()
        if fn_ext and child_tid:
            runnable_work.append(
                _FanoutChildWork(
                    spec=spec,
                    batch_index=len(runnable_work) + 1,
                    fn_ext=fn_ext,
                    child_tid=child_tid,
                    child_data=dict(child_data),
                )
            )

    total_batches = len(runnable_work)
    _emit_fanout_batch_progress(
        fanout_task_id=task_id,
        fanout_canvas=fanout_canvas,
        completed_batches=0,
        total_batches=total_batches,
        progress_lock=progress_lock,
    )

    completed_counter = {"n": 0}

    def _run_child(work: _FanoutChildWork) -> _FanoutChildResult:
        fn_ext = work.fn_ext
        child_tid = work.child_tid
        pipe = pipelines.get(fn_ext)
        if pipe is None:
            raise RuntimeError(f"No pipeline spec for dynamic child {fn_ext!r}")
        mod_name, entry = pipe
        mod = importlib.import_module(mod_name)
        fn = getattr(mod, entry)
        data: Dict[str, Any] = dict(shared_data)
        data.update(_resolve_child_function_data(work.child_data, shared_data))
        data["task_id"] = child_tid
        data["compiled_workflow"] = shared_data.get("compiled_workflow")
        emit_ui_progress(
            "task_start",
            task_id=child_tid,
            function_external_id=fn_ext,
            canvas_node_id=fanout_canvas,
            pipeline_node_id=child_tid,
        )
        logger.info(
            "dynamic_fanout running child batch %s/%s: %s (%s)",
            work.batch_index,
            total_batches,
            child_tid,
            fn_ext,
        )
        t0 = time.perf_counter()
        bind_handler_progress({**data, "compiled_task": {"function_external_id": fn_ext}})
        try:
            summary = fn(fn_ext, data, client, logger)
            if isinstance(summary, dict):
                summary = dict(summary)
        finally:
            clear_handler_progress()
        duration_sec = round(time.perf_counter() - t0, 6)
        child_summary = summary if isinstance(summary, dict) else {}
        ui_end: Dict[str, Any] = {
            "task_id": child_tid,
            "function_external_id": fn_ext,
            "canvas_node_id": fanout_canvas,
            "pipeline_node_id": child_tid,
            "status": "succeeded",
            "duration_sec": duration_sec,
        }
        ann = child_summary.get("annotation_rows")
        if isinstance(ann, int):
            ui_end["rows_written"] = ann
        emit_ui_progress("task_end", **{k: v for k, v in ui_end.items() if v is not None})
        with progress_lock:
            completed_counter["n"] += 1
            done = completed_counter["n"]
        _emit_fanout_batch_progress(
            fanout_task_id=task_id,
            fanout_canvas=fanout_canvas,
            completed_batches=done,
            total_batches=total_batches,
            progress_lock=progress_lock,
        )
        return _FanoutChildResult(
            child_tid=child_tid,
            fn_ext=fn_ext,
            batch_index=work.batch_index,
            summary=child_summary,
            duration_sec=duration_sec,
        )

    if max_workers <= 1 or total_batches <= 1:
        child_results = [_run_child(w) for w in runnable_work]
    else:
        child_results = run_parallel(runnable_work, _run_child, max_workers=max_workers)

    child_results.sort(key=lambda r: r.batch_index)
    child_summaries: Dict[str, Any] = {r.child_tid: r.summary for r in child_results}
    child_task_ids = [r.child_tid for r in child_results]

    merged_rows = _merge_child_cohort_into_fanout(
        client,
        shared_data,
        fanout_task_id=task_id,
        child_task_ids=child_task_ids,
        logger=logger,
        shared_lock=sink_lock,
    )

    return {
        "status": "ok" if child_task_ids and merged_rows > 0 else (
            "completed_with_errors" if not child_task_ids else "ok"
        ),
        "task_id": task_id,
        "reason": (
            plan_summary.get("reason")
            or ("dynamic_fanout_executed_locally" if child_task_ids else "no_dynamic_children_planned")
        ),
        "children_run": len(child_task_ids),
        "batches_total": total_batches,
        "batches_completed": len(child_task_ids),
        "merged_cohort_rows": merged_rows,
        "child_summaries": child_summaries,
        "files_pending": plan_summary.get("files_pending"),
        "files_skipped_detected": plan_summary.get("files_skipped_detected"),
    }
