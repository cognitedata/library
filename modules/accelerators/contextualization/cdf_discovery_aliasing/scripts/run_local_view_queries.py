#!/usr/bin/env python3
"""
Run every ``fn_dm_view_query`` task from the local workflow scope (same IR as ``module.py run``).

Prints each task's summary (from handler ``message``) and a sample of cohort rows from the
RAW sink written by that query (``raw_db`` / ``raw_table`` in the summary).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

_MODULE_ROOT = Path(__file__).resolve().parents[1]
_REPO_ROOT = _MODULE_ROOT.parent.parent.parent.parent
for _p in (_REPO_ROOT, _MODULE_ROOT, str(_MODULE_ROOT / "functions")):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from cdf_fn_common.incremental_scope import iter_raw_table_rows_chunked  # noqa: E402
from cdf_fn_common.task_runtime import merge_compiled_task_into_data  # noqa: E402
from cdf_fn_common.function_logging import StdlibLoggerAdapter  # noqa: E402
from local_runner.client import create_cognite_client  # noqa: E402
from local_runner.config_loading import load_discovery_scope  # noqa: E402
from local_runner.paths import ensure_repo_on_path  # noqa: E402
from local_runner.run import _discovery_cdf_config_and_engine  # noqa: E402
from local_runner.workflow_payload import (  # noqa: E402
    compiled_workflow_for_local_run,
    merged_scope_document_for_local_run,
    workflow_instance_space_for_local,
)


def _utc_run_id() -> str:
    from cdf_fn_common.discovery_query_shared import new_pipeline_run_id

    return new_pipeline_run_id()


def _source_view_matches_instance_space(view: Dict[str, Any], wanted: str) -> bool:
    """Same semantics as ``module.py`` ``--instance-space`` filter."""
    wanted_s = wanted.strip()
    if (view.get("instance_space") or "").strip() == wanted_s:
        return True
    for f in view.get("filters") or []:
        if str(f.get("property_scope", "view")).lower() != "node":
            continue
        if f.get("target_property") != "space":
            continue
        op = str(f.get("operator", "")).upper()
        vals = f.get("values")
        if op == "EQUALS":
            vs: List[Any]
            if isinstance(vals, list):
                vs = vals
            elif vals is None:
                continue
            else:
                vs = [vals]
            if any(str(x).strip() == wanted_s for x in vs if x is not None):
                return True
        elif op == "IN" and isinstance(vals, list):
            if wanted_s in {str(x).strip() for x in vals}:
                return True
    return False


def _sample_raw_rows(
    client: Any, raw_db: str, raw_table: str, *, limit: int
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in iter_raw_table_rows_chunked(client, raw_db, raw_table):
        cols = dict(getattr(row, "columns", None) or {})
        out.append({"key": str(getattr(row, "key", "") or ""), "columns": cols})
        if len(out) >= limit:
            break
    return out


def main() -> int:
    ensure_repo_on_path()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger = logging.getLogger("run_local_view_queries")

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--config-path",
        type=str,
        default=None,
        help="Scope YAML (default: module-root workflow.local.config.yaml via --scope default).",
    )
    p.add_argument("--scope", type=str, default="default")
    p.add_argument(
        "--instance-space",
        type=str,
        default=None,
        help="Optional: filter source_views like ``module.py run --instance-space``.",
    )
    p.add_argument(
        "--all",
        dest="run_all",
        action="store_true",
        help="Pass run_all=true on task data (same as ``module.py run --all`` / cohort-wide RAW semantics).",
    )
    p.add_argument(
        "--raw-sample-rows",
        type=int,
        default=25,
        metavar="N",
        help="Max RAW cohort rows to print per query task (default 25).",
    )
    args = p.parse_args()

    from local_runner.env import load_env

    load_env()
    if args.run_all:
        logger.info("--all: run entire scope (run_all=true on view query payloads).")
    client = create_cognite_client()

    scope_yaml_path, source_views = load_discovery_scope(
        logger,
        scope=args.scope,
        config_path=args.config_path,
    )
    if args.instance_space and source_views:
        wanted = str(args.instance_space).strip()
        source_views = [
            v for v in source_views if isinstance(v, dict) and _source_view_matches_instance_space(v, wanted)
        ]

    scope_document = merged_scope_document_for_local_run(scope_yaml_path, source_views)
    wf_instance_space = workflow_instance_space_for_local(source_views, args.instance_space)
    compiled_wf = compiled_workflow_for_local_run(scope_document)
    cdf_config, _ = _discovery_cdf_config_and_engine(source_views, logger)

    tasks = compiled_wf.get("tasks") if isinstance(compiled_wf, dict) else None
    if not isinstance(tasks, list):
        print("compiled_workflow.tasks missing", file=sys.stderr)
        return 2

    run_id = _utc_run_id()
    pipe_logger = StdlibLoggerAdapter(logger)
    view_tasks = [t for t in tasks if isinstance(t, dict) and t.get("function_external_id") == "fn_dm_view_query"]
    if not view_tasks:
        print("No fn_dm_view_query tasks in compiled_workflow.", file=sys.stderr)
        return 1

    from fn_dm_view_query.pipeline import query_view

    results: List[Dict[str, Any]] = []
    for task in view_tasks:
        task_id = str(task.get("id") or "")
        data: Dict[str, Any] = {
            "logLevel": "INFO",
            "run_id": run_id,
            "run_all": bool(args.run_all),
            "configuration": scope_document,
            "instance_space": wf_instance_space,
            "compiled_workflow": compiled_wf,
            "task_id": task_id,
        }
        merge_compiled_task_into_data(data)
        query_view(client, pipe_logger, data, cdf_config)
        summary: Dict[str, Any] = {}
        msg = data.get("message")
        if isinstance(msg, str) and msg.strip():
            try:
                parsed = json.loads(msg)
                if isinstance(parsed, dict):
                    summary = parsed
            except json.JSONDecodeError:
                summary = {"_parse_error": True, "message": msg[:500]}

        raw_db = str(summary.get("raw_db") or "").strip()
        raw_table = str(summary.get("raw_table") or "").strip()
        raw_rows: List[Dict[str, Any]] = []
        err: str | None = None
        if raw_db and raw_table and args.raw_sample_rows > 0:
            try:
                raw_rows = _sample_raw_rows(
                    client, raw_db, raw_table, limit=int(args.raw_sample_rows)
                )
            except Exception as ex:
                err = f"{type(ex).__name__}: {ex}"

        results.append(
            {
                "task_id": task_id,
                "pipeline_node_id": task.get("pipeline_node_id"),
                "status": data.get("status"),
                "summary": summary,
                "raw_sample_row_count": len(raw_rows),
                "raw_sample_rows": raw_rows,
                "raw_read_error": err,
            }
        )

    print(json.dumps(results, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
