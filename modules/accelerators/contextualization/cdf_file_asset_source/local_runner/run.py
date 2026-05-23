"""Run extract → create → write locally and write JSON snapshots."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal

from local_runner.env import load_env
from local_runner.paths import DEFAULT_CONFIG_REL, PIPELINE_STEPS, ensure_import_paths, get_module_root
from local_runner.ui_progress import (
    emit_ui_progress,
    ui_progress_log_forwarding,
    ui_progress_stdio_forwarding,
)

PipelineStep = Literal["extract", "create", "write", "all"]

STEP_FUNCTION_IDS: dict[str, str] = {
    "extract": "fn_dm_extract_assets_by_pattern",
    "create": "fn_dm_create_asset_hierarchy",
    "write": "fn_dm_write_asset_hierarchy",
}


def _ensure_run_logging() -> None:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )


def _import_run_locally(step: PipelineStep):
    ensure_import_paths()
    if step == "extract":
        from fn_dm_extract_assets_by_pattern.handler import run_locally

        return run_locally
    if step == "create":
        from fn_dm_create_asset_hierarchy.handler import run_locally

        return run_locally
    if step == "write":
        from fn_dm_write_asset_hierarchy.handler import run_locally

        return run_locally
    raise ValueError(f"Unknown step: {step}")


def _run_results_dir() -> Path:
    d = get_module_root() / "local_run_results"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_snapshot(
    *,
    step: str,
    result: dict[str, Any],
    workflow: bool = False,
) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    suffix = "workflow_all" if workflow else f"pipeline_{step}"
    path = _run_results_dir() / f"{ts}_{suffix}.json"
    target = "workflow_all" if workflow else f"pipeline_{step}"
    payload = {
        "run_scope": {
            "target": target,
            "config": DEFAULT_CONFIG_REL,
            "step": step if not workflow else None,
        },
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "result": result,
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def _run_step_with_progress(step: str, runner: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    """Execute one step and emit NDJSON progress when ``FAS_UI_PROGRESS_FD`` is set."""
    fn_id = STEP_FUNCTION_IDS.get(step, step)
    emit_ui_progress(
        "task_start",
        task_id=step,
        function_external_id=fn_id,
        workflow_step=step,
    )
    try:
        _ensure_run_logging()
        with ui_progress_stdio_forwarding(), ui_progress_log_forwarding():
            result = runner()
        status = str(result.get("status", "unknown"))
        failed = status != "succeeded"
        emit_ui_progress(
            "task_end",
            task_id=step,
            function_external_id=fn_id,
            workflow_step=step,
            status="failed" if failed else "succeeded",
            error=result.get("message") if failed else None,
        )
        return result
    except Exception as exc:
        emit_ui_progress(
            "task_end",
            task_id=step,
            function_external_id=fn_id,
            workflow_step=step,
            status="failed",
            error=str(exc),
        )
        raise


def run_pipeline_step(step: PipelineStep) -> dict[str, Any]:
    """Run a single pipeline step and persist a JSON snapshot under ``local_run_results/``."""
    if step == "all":
        return run_pipeline_workflow()
    load_env()
    runner = _import_run_locally(step)
    result = _run_step_with_progress(step, runner)
    snapshot = _write_snapshot(step=step, result=result)
    status = str(result.get("status", "unknown"))
    return {
        "status": status,
        "step": step,
        "result": result,
        "snapshot": str(snapshot.relative_to(get_module_root())).replace("\\", "/"),
        "succeeded": status == "succeeded",
    }


def run_pipeline_workflow() -> dict[str, Any]:
    """Run extract, then create, then write; stop on first failure."""
    load_env()
    outcomes: dict[str, Any] = {}
    for step in PIPELINE_STEPS:
        runner = _import_run_locally(step)
        result = _run_step_with_progress(step, runner)
        outcomes[step] = result
        if str(result.get("status")) != "succeeded":
            snapshot = _write_snapshot(step=step, result=result, workflow=True)
            return {
                "status": "failure",
                "failed_step": step,
                "steps": outcomes,
                "snapshot": str(snapshot.relative_to(get_module_root())).replace("\\", "/"),
                "succeeded": False,
            }
    snapshot_path = _run_results_dir() / (
        datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_workflow_all.json"
    )
    payload = {
        "run_scope": {"target": "workflow_all", "config": DEFAULT_CONFIG_REL},
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "result": {"status": "succeeded", "steps": outcomes},
    }
    snapshot_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return {
        "status": "succeeded",
        "steps": outcomes,
        "snapshot": str(snapshot_path.relative_to(get_module_root())).replace("\\", "/"),
        "succeeded": True,
    }
