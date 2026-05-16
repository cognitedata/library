"""Orchestrate the discovery canvas ``compiled_workflow`` DAG for the local CLI."""
import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple, Union

from cdf_fn_common.source_view_filter_build import (
    build_source_view_query_filter,
)
from cdf_fn_common.function_logging import (
    StdlibLoggerAdapter,
)

from .kahn_run_context import KahnRunContext
from .kahn_workflow_executor import (
    run_compiled_workflow_dag,
    validate_execution_graph_at_startup,
)
from .local_run_report import compose_local_run_report_document, write_local_run_report
from .raw_results_attachment import build_raw_results_bundle
from .report import ensure_results_dir
from .ui_progress import ui_progress_log_forwarding
from .workflow_payload import (
    compiled_workflow_for_local_run,
    merged_scope_document_for_local_run,
    scope_document_has_embedded_compiled_workflow,
    workflow_instance_space_for_local,
)

_MODULE_ROOT = Path(__file__).resolve().parent.parent


def _ensure_pipeline_run_id(ctx: KahnRunContext) -> None:
    """Assign one workflow ``run_id`` on *ctx* before tasks execute.

    Each ``_discovery_branch`` seeds ``data["run_id"]`` from ``ctx.run_id``. When that was empty,
    :func:`cdf_fn_common.discovery_query_shared.resolve_run_id` generated a **new** id per task,
    so parallel view-query tasks wrote cohort ``RUN_ID`` values that downstream transform/validate
    steps could not match (``filter_run``), yielding ``rows_read=0`` and no transform rows in RAW.
    """
    if str(getattr(ctx, "run_id", None) or "").strip():
        return
    ctx.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def _config_rel_for_run_results(scope_yaml_path: Union[Path, str]) -> str:
    p = Path(scope_yaml_path).resolve()
    try:
        return str(p.relative_to(_MODULE_ROOT)).replace("\\", "/")
    except ValueError:
        return str(p).replace("\\", "/")


def result_run_scope_dict(scope_yaml_path: Union[Path, str]) -> Dict[str, Any]:
    """Classify a local run for operator UI filtering (env from ``ui.server`` / ``module.py`` subprocess)."""
    raw_target = (os.environ.get("KEA_OPERATOR_RUN_TARGET") or "workflow_local").strip().lower()
    if raw_target not in ("workflow_local", "workflow_template", "workflow_trigger"):
        raw_target = "workflow_local"
    trig = (os.environ.get("KEA_OPERATOR_WORKFLOW_TRIGGER_REL") or "").strip().replace("\\", "/")
    out: Dict[str, Any] = {
        "target": raw_target,
        "config_rel": _config_rel_for_run_results(scope_yaml_path),
    }
    if trig:
        out["workflow_trigger_rel"] = trig
    return out


def _build_dm_filter_from_view_dict(
    view_config: Dict[str, Any], logger: logging.Logger
) -> Any:
    """Same DM filter semantics as ``run_pipeline`` (``HasData`` + configured nodes)."""
    from cognite.client.data_classes.data_modeling.ids import ViewId

    view_space = view_config.get("view_space", "cdf_cdm")
    view_external_id = view_config.get("view_external_id", "CogniteAsset")
    view_version = view_config.get("view_version", "v1")
    view_id = ViewId(
        space=view_space, external_id=view_external_id, version=view_version
    )
    return build_source_view_query_filter(view_id, view_config.get("filters") or [])


class _ViewConfigAdapter:
    """View config for discovery task ``cdf_config`` (root ``source_views`` dict shape)."""

    def __init__(self, d: Dict[str, Any], logger: logging.Logger) -> None:
        self._raw = d
        self._logger = logger
        self.view_space = d.get("view_space", "cdf_cdm")
        self.view_external_id = d.get("view_external_id", "CogniteAsset")
        self.view_version = d.get("view_version", "v1")
        self.instance_space = d.get("instance_space")
        bs = d.get("batch_size") or d.get("limit") or 1000
        self.batch_size = int(bs) if bs else 1000

    @property
    def entity_type(self) -> Any:
        et = self._raw.get("entity_type", "asset")
        if isinstance(et, str):
            return SimpleNamespace(value=et)
        return SimpleNamespace(value=getattr(et, "value", str(et)))

    @property
    def exclude_self_referencing_keys(self) -> Any:
        return self._raw.get("exclude_self_referencing_keys")

    @property
    def include_properties(self) -> List[str]:
        return list(self._raw.get("include_properties") or [])

    def as_view_id(self) -> Any:
        from cognite.client.data_classes.data_modeling.ids import ViewId

        return ViewId(
            space=self.view_space,
            external_id=self.view_external_id,
            version=self.view_version,
        )

    def model_dump(self, **kwargs: Any) -> Dict[str, Any]:
        # Call sites use Pydantic-style ``model_dump(mode="python")``; we only wrap a dict.
        del kwargs
        return dict(self._raw)

    def build_filter(self) -> Any:
        return _build_dm_filter_from_view_dict(self._raw, self._logger)


def _discovery_cdf_config_and_engine(
    source_views: List[Dict[str, Any]],
    logger: logging.Logger,
) -> Tuple[Any, Dict[str, Any]]:
    """Minimal ``cdf_config`` / engine payload for discovery function stubs (no legacy extraction stack)."""
    sv_parsed = [_ViewConfigAdapter(v, logger) for v in source_views]
    cdf_config = SimpleNamespace(
        parameters=SimpleNamespace(),
        data=SimpleNamespace(
            source_views=sv_parsed,
            source_view=None,
            extraction_rules=[],
            validation=None,
            source_tables=[],
            associations=None,
        ),
    )
    return cdf_config, {}


def _log_cli_run_summary(logger: logging.Logger, payload: Dict[str, Any]) -> None:
    """Single end-of-run summary block for the local CLI (discovery DAG only)."""
    lines = [
        "--- Run summary (discovery workflow) ---",
        f"run_id: {payload.get('run_id') or '(none)'}",
        f"tasks_executed: {payload.get('task_timings_count', 0)}",
    ]
    p = (payload.get("paths") or {}).get("discovery")
    if p:
        lines.append(f"Output: discovery_json={p}")
    logger.info("\n".join(lines))


def _write_local_run_report_for_ctx(
    ctx: KahnRunContext,
    results_dir: Path,
    ts: str,
    run_scope: Dict[str, Any],
    logger: logging.Logger,
    discovery_path: Path,
    *,
    dry_run: bool,
    raw_results: Optional[Dict[str, Any]] = None,
) -> None:
    if not getattr(ctx, "local_run_tasks", None):
        return
    try:
        paths = {"discovery": str(discovery_path)}
        outp = results_dir / f"{ts}_local_run_report.json"
        doc = compose_local_run_report_document(
            tasks=list(ctx.local_run_tasks),
            wall_t0=float(ctx.local_run_wall_t0 or 0.0),
            dry_run=dry_run,
            paths=paths,
            raw_results=raw_results,
        )
        write_local_run_report(outp, doc)
        logger.info("✓ Wrote local run report: %s", outp)
    except Exception as exc:
        logger.warning("Could not write local_run_report.json: %s", exc)


def _run_workflow_parity(
    args: argparse.Namespace,
    logger: logging.Logger,
    client: Any,
    source_views: List[Dict[str, Any]],
    scope_yaml_path: Path,
) -> None:
    """Execute ``compiled_workflow`` tasks in topological order (same IR as ``workflow.input``)."""
    pipe_logger: Any = StdlibLoggerAdapter(logger)
    cdf_config, _ = _discovery_cdf_config_and_engine(source_views, logger)

    scope_document = merged_scope_document_for_local_run(scope_yaml_path, source_views)
    wf_instance_space = workflow_instance_space_for_local(
        source_views, getattr(args, "instance_space", None)
    )
    if scope_document_has_embedded_compiled_workflow(scope_document):
        logger.info(
            "Using embedded root-level compiled_workflow from scope; skipping canvas compile."
        )
    compiled_wf = compiled_workflow_for_local_run(scope_document)
    validate_execution_graph_at_startup(_MODULE_ROOT, logger, compiled_wf)

    logger.info(
        "Discovery mode: executing compiled_workflow DAG (registry from canvas_dag._KIND_FN)"
    )

    ctx = KahnRunContext(
        args=args,
        logger=logger,
        client=client,
        pipe_logger=pipe_logger,
        scope_yaml_path=scope_yaml_path,
        scope_document=scope_document,
        wf_instance_space=wf_instance_space,
        source_views=source_views,
        cdf_config=cdf_config,
        compiled_workflow=compiled_wf,
    )
    ctx.local_run_wall_t0 = time.perf_counter()
    _ensure_pipeline_run_id(ctx)

    _timing_mark = len(ctx.task_timings)
    run_compiled_workflow_dag(ctx)
    if len(ctx.task_timings) > _timing_mark:
        parts = [
            f"{t.get('function_external_id') or t.get('task_id')}:{float(t.get('duration_sec', 0)):.2f}s"
            for t in ctx.task_timings[_timing_mark:]
        ]
        logger.info("Pipeline task timings: %s", ", ".join(parts))

    results_dir = ensure_results_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    discovery_path = results_dir / f"{ts}_cdf_discovery_tasks.json"
    run_scope = result_run_scope_dict(scope_yaml_path)
    raw_results: Optional[Dict[str, Any]] = None
    row_limit = int(getattr(args, "raw_results_rows", 500) or 0)
    max_tables = int(getattr(args, "raw_results_max_tables", 30) or 0)
    if client is not None and row_limit > 0 and max_tables > 0:
        try:
            _mrs = int(getattr(args, "raw_results_max_rows_scanned", 0) or 0)
            raw_results = build_raw_results_bundle(
                client,
                ctx.discovery_task_outputs,
                row_limit=row_limit,
                max_tables=max_tables,
                logger=logger,
                run_id=str(ctx.run_id or "").strip() or None,
                max_raw_rows_scanned=_mrs if _mrs > 0 else None,
            )
            if raw_results.get("tables"):
                logger.info(
                    "RAW results attachment: %s table(s), up to %s rows each",
                    len(raw_results["tables"]),
                    row_limit,
                )
        except Exception as exc:
            logger.warning("Could not build RAW results attachment: %s", exc)

    out_doc = {
        "run_scope": run_scope,
        "task_outputs": ctx.discovery_task_outputs,
        "local_run_tasks": ctx.local_run_tasks,
        "handler_data_snapshots": dict(ctx.handler_data_snapshots),
    }
    with discovery_path.open("w", encoding="utf-8") as f:
        json.dump(out_doc, f, indent=2, default=str)
    logger.info("✓ Wrote discovery run snapshot: %s", discovery_path)

    _write_local_run_report_for_ctx(
        ctx,
        results_dir,
        ts,
        run_scope,
        logger,
        discovery_path,
        dry_run=bool(getattr(args, "dry_run", False)),
        raw_results=raw_results,
    )

    _log_cli_run_summary(
        logger,
        {
            "run_id": ctx.run_id,
            "task_timings_count": len(ctx.task_timings),
            "paths": {"discovery": str(discovery_path)},
        },
    )


def run_pipeline(
    args: argparse.Namespace,
    logger: logging.Logger,
    client: Any,
    source_views: List[Dict[str, Any]],
    scope_yaml_path: Optional[Union[Path, str]] = None,
) -> None:
    """Run the discovery canvas DAG (``compiled_workflow``) for the given scope YAML."""
    if not scope_yaml_path:
        raise ValueError(
            "Local runs require a scope YAML path "
            "(module-root workflow.local.config.yaml with --scope default, or --config-path)."
        )
    sp = Path(scope_yaml_path)
    if getattr(args, "run_all", False):
        logger.info(
            "--all: run entire scope (same as workflow input run_all=true)."
        )
    log_fwd_level = logging.DEBUG if getattr(args, "verbose", False) else logging.INFO
    with ui_progress_log_forwarding(log_fwd_level):
        _run_workflow_parity(
            args,
            logger,
            client,
            source_views,
            sp,
        )
