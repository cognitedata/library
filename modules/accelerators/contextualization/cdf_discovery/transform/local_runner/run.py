"""CLI entry: run a built pipeline instance locally."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

import yaml

from local_runner.client import create_cognite_client
from local_runner.env import load_env
from local_runner.kahn_workflow_executor import run_compiled_workflow_dag
from local_runner.paths import ensure_paths, module_root
from local_runner.run_context import (
    apply_incremental_run_scope,
    ensure_shared_run_id,
    merge_pipeline_runtime,
)
from local_runner.ui_progress import ui_progress_log_forwarding


def load_pipeline_instance(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    if not isinstance(doc, dict):
        raise ValueError(f"Pipeline instance must be a mapping: {path}")
    return doc


def ensure_compiled_workflow(doc: Dict[str, Any]) -> Dict[str, Any]:
    compiled = doc.get("compiled_workflow")
    if isinstance(compiled, dict) and compiled.get("tasks"):
        return compiled
    canvas = doc.get("canvas")
    if not isinstance(canvas, dict):
        raise ValueError("Pipeline document has no canvas to compile")
    from cdf_fn_common.workflow_compile.canvas_dag import compile_canvas_dag

    compiled = compile_canvas_dag(canvas)
    doc["compiled_workflow"] = compiled
    return compiled


def run_pipeline_document(
    doc: Mapping[str, Any],
    *,
    dry_run: bool = False,
    predecessor_mode: str | None = None,
    incremental_change_processing: bool = True,
    max_workers: int | None = None,
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """Execute ``compiled_workflow`` from an in-memory pipeline document."""
    ensure_paths()
    working = dict(doc)
    apply_incremental_run_scope(working, incremental_change_processing=incremental_change_processing)
    compiled = ensure_compiled_workflow(working)
    log = logger or logging.getLogger("etl.run")

    client = None if dry_run else create_cognite_client()
    if not dry_run and client is None:
        raise RuntimeError(
            "CDF credentials missing — set COGNITE_API_KEY, OAuth client credentials, "
            "toolkit interactive vars (CDF_PROJECT, CDF_CLUSTER, IDP_TENANT_ID, IDP_CLIENT_ID), "
            "or use --dry-run"
        )

    shared: Dict[str, Any] = {
        "configuration": working,
        "compiled_workflow": compiled,
        "incremental_change_processing": incremental_change_processing,
        "dry_run": dry_run,
    }
    merge_pipeline_runtime(shared, working)
    if not incremental_change_processing:
        shared["incremental"] = False
    from cdf_fn_common.etl_predecessor_mode import MODE_COHORT, MODE_IN_MEMORY, seed_predecessor_mode

    if predecessor_mode:
        seed_predecessor_mode(shared, predecessor_mode)
    else:
        seed_predecessor_mode(shared, MODE_IN_MEMORY if dry_run else MODE_COHORT)
    run_id = ensure_shared_run_id(shared)
    log.info(
        "Local pipeline run_id=%s dry_run=%s predecessor_mode=%s max_workers=%s",
        run_id,
        dry_run,
        shared.get("local_predecessor_mode"),
        max_workers,
    )

    with ui_progress_log_forwarding():
        summaries = run_compiled_workflow_dag(
            compiled,
            client=client,
            logger=log,
            shared_data=shared,
            dry_run=dry_run,
            max_workers=max_workers,
        )
    return {
        "run_id": run_id,
        "dry_run": dry_run,
        "task_summaries": summaries,
    }


def run_pipeline(
    *,
    instance_path: Path,
    dry_run: bool = False,
    predecessor_mode: str | None = None,
    incremental_change_processing: bool = True,
    max_workers: int | None = None,
) -> Dict[str, Any]:
    doc = load_pipeline_instance(instance_path)
    logger = logging.getLogger("etl.run")
    return run_pipeline_document(
        doc,
        dry_run=dry_run,
        predecessor_mode=predecessor_mode,
        incremental_change_processing=incremental_change_processing,
        max_workers=max_workers,
        logger=logger,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a built CDF Discovery ETL pipeline locally")
    target = parser.add_mutually_exclusive_group()
    target.add_argument(
        "--workflow",
        "--instance",
        default=None,
        dest="workflow",
        help="Workflow definition id (YAML under workflow_definitions/instances/)",
    )
    target.add_argument(
        "--template",
        default=None,
        help="Workflow template id (YAML under workflow_definitions/templates/)",
    )
    parser.add_argument(
        "--scope-suffix",
        default="all",
        help="Built scope folder (workflows/<suffix>/etl_<id>.<suffix>.config.yaml)",
    )
    parser.add_argument("--config-dir", type=Path, default=None, help="Override document directory")
    parser.add_argument("--dry-run", action="store_true", help="Skip CDF client; exercise DAG in-memory")
    parser.add_argument(
        "--no-incremental-change-processing",
        dest="incremental_change_processing",
        action="store_false",
        default=True,
        help="Process full scope (disable incremental watermark filter for this run)",
    )
    parser.add_argument(
        "--predecessor-mode",
        choices=["in_memory", "cohort"],
        default=None,
        help="Hand off rows via RAW cohort tables (default for live runs) or in-memory buffer",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Max parallel tasks per DAG layer (default: min(4, layer size); 1=serial)",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    root = module_root()
    load_env(root)
    ensure_paths()
    if args.template:
        tpl_dir = args.config_dir or (root / "workflow_definitions" / "templates")
        inst_path = tpl_dir / f"{args.template}.template.yaml"
        if not inst_path.is_file():
            logging.error("Missing workflow template %s", inst_path)
            return 1
    else:
        workflow_id = args.workflow or "discovery_etl_default"
        scope_suffix = str(args.scope_suffix or "all").strip() or "all"
        if args.config_dir:
            inst_path = args.config_dir
        else:
            built_path = root / "workflows" / scope_suffix / f"etl_{workflow_id}.{scope_suffix}.config.yaml"
            inst_path = root / "workflow_definitions" / "instances" / f"{workflow_id}.yaml"
            if built_path.is_file():
                inst_path = built_path
            elif not inst_path.is_file():
                logging.error(
                    "Missing workflow %s for scope %r — build or save first (tried %s and %s)",
                    workflow_id,
                    scope_suffix,
                    built_path,
                    inst_path,
                )
                return 1
    try:
        payload = run_pipeline(
            instance_path=inst_path,
            dry_run=args.dry_run,
            predecessor_mode=args.predecessor_mode,
            incremental_change_processing=args.incremental_change_processing,
            max_workers=args.max_workers,
        )
    except Exception as ex:
        logging.error("%s: %s", type(ex).__name__, ex)
        return 1
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
