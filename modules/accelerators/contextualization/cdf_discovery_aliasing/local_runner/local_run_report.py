"""Structured JSON report for local workflow runs (tasks + end_of_process)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

REPORT_SCHEMA_VERSION = 1

LOCAL_RUN_REPORT_SUFFIX = "_local_run_report.json"
CDF_DISCOVERY_TASKS_SUFFIX = "_cdf_discovery_tasks.json"


def cdf_discovery_tasks_path_for_run_report(local_run_report_path: Path) -> Path:
    """Sibling ``*_cdf_discovery_tasks.json`` for a given ``*_local_run_report.json`` path."""
    name = local_run_report_path.name
    if not name.endswith(LOCAL_RUN_REPORT_SUFFIX):
        raise ValueError(
            f"expected path ending with {LOCAL_RUN_REPORT_SUFFIX!r}, got {local_run_report_path.name!r}"
        )
    stem = name[: -len(LOCAL_RUN_REPORT_SUFFIX)]
    return local_run_report_path.with_name(f"{stem}{CDF_DISCOVERY_TASKS_SUFFIX}")


def build_end_of_process(
    *,
    tasks: List[Dict[str, Any]],
    wall_t0: float,
    dry_run: bool,
    failed_task_id: Optional[str] = None,
    warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    elapsed_ms = int(max(0.0, (time.perf_counter() - wall_t0)) * 1000) if wall_t0 else None
    if failed_task_id:
        status = "failed"
    elif any(t.get("status") == "failed" for t in tasks):
        status = "failed"
    elif dry_run:
        status = "succeeded"
    else:
        status = "succeeded"
    ft: Optional[str] = failed_task_id
    if ft is None:
        for t in tasks:
            if t.get("status") == "failed":
                ft = str(t.get("task_id") or "")
                break
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": status,
        "failed_task_key": ft,
        "elapsed_ms": elapsed_ms,
        "dry_run": bool(dry_run),
        "task_count": len(tasks),
        "warnings": list(warnings or []),
    }


def write_local_run_report(path: Path, document: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(document), indent=2, default=str), encoding="utf-8")


def compose_local_run_report_document(
    *,
    tasks: List[Dict[str, Any]],
    wall_t0: float,
    dry_run: bool,
    paths: Dict[str, Any],
    raw_results: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build the slim ``*_local_run_report.json`` payload.

    Task payloads, ``run_scope``, and handler snapshots stay in ``*_cdf_discovery_tasks.json``
    (see ``paths["discovery"]``). Optional ``raw_results`` (RAW row samples) are attached here
    only, not duplicated in the cdf discovery snapshot.
    """
    eop = build_end_of_process(
        tasks=tasks,
        wall_t0=wall_t0,
        dry_run=dry_run,
    )
    doc: Dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "paths": paths,
        "end_of_process": eop,
    }
    if raw_results and isinstance(raw_results, dict) and raw_results.get("tables"):
        doc["raw_results"] = raw_results
    return doc
