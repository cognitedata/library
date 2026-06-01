"""CDF handler: ETL transform stage (fields, templates, handler registry)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_common import (
    _first_nonempty,
    emit_agent_debug_log,
    iter_predecessor_rows,
    require_pipeline_run_key,
    resolve_task_config,
)
from cdf_fn_common.etl_pipeline_steps import materialize_transform_steps
from cdf_fn_common.etl_predecessor_mode import use_in_memory_predecessors
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data
from cdf_fn_common.etl_transform.transform_handlers import resolve_handler_id
from cdf_fn_common.etl_transform.transform_steps import (
    apply_transform_steps_to_props,
    validate_transform_pipeline_config,
)
from cdf_fn_common.etl_transform_orchestration import etl_handle_transform_cohort
from cdf_fn_common.etl_ui_progress import (
    emit_handler_progress_every_n_rows,
    emit_handler_progress_rows_complete,
)


def etl_handle_transform(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    pre_run_id = _first_nonempty(data.get("run_id"), "pre-run")
    # #region agent log
    emit_agent_debug_log(
        run_id=pre_run_id,
        hypothesis_id="H3",
        location="fn_etl_transform/handler.py:44",
        message="transform_entry",
        data={
            "task_id": _first_nonempty(data.get("task_id"), fn_external_id),
            "enabled": bool(cfg.get("enabled", True)) if isinstance(cfg, dict) else False,
            "has_config": bool(cfg),
        },
    )
    # #endregion
    if not cfg:
        raise ValueError("transform task requires non-empty config")
    validate_transform_pipeline_config(cfg)

    if not bool(cfg.get("enabled", True)):
        return {
            "function_external_id": fn_external_id,
            "task_id": _first_nonempty(data.get("task_id"), fn_external_id),
            "status": "skipped",
            "rows_read": 0,
            "rows_written": 0,
            "reason": "disabled",
        }

    if not use_in_memory_predecessors(data, cfg):
        return etl_handle_transform_cohort(fn_external_id, data, client, log)

    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    _, steps = materialize_transform_steps(cfg)
    handler_summary = resolve_handler_id(steps[0]) if steps else ""

    rows_read = 0
    rows_written = 0
    out_rows: list[dict[str, Any]] = []
    for cols, props in iter_predecessor_rows(data):
        rows_read += 1
        emit_handler_progress_every_n_rows(rows_read)
        try:
            for out_props in apply_transform_steps_to_props(props, cfg):
                out_rows.append({"columns": dict(cols), "properties": dict(out_props)})
                rows_written += 1
        except Exception as ex:
            # #region agent log
            emit_agent_debug_log(
                run_id=run_id,
                hypothesis_id="H3",
                location="fn_etl_transform/handler.py:93",
                message="transform_row_exception",
                data={
                    "task_id": task_id,
                    "rows_read": rows_read,
                    "error": str(ex),
                    "handler_summary": handler_summary,
                },
            )
            # #endregion
            if log and hasattr(log, "error"):
                log.error("%s row transform failed: %s", task_id, ex)
            raise

    data["_predecessor_rows"] = out_rows
    emit_handler_progress_rows_complete(rows_read)

    if log and hasattr(log, "info"):
        log.info(
            "%s transform in_memory rows_read=%s rows_written=%s steps=%s",
            fn_external_id,
            rows_read,
            rows_written,
            len(steps),
        )

    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "handler_id": handler_summary,
        "step_count": len(steps),
        "rows_read": rows_read,
        "rows_written": rows_written,
        "run_id": run_id,
        "status": "ok",
        "description": _first_nonempty(cfg.get("description")),
        "predecessor_mode": "in_memory",
    }


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_transform("fn_etl_transform", data, client, log=None)
