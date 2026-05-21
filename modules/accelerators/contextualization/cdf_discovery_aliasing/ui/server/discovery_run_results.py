"""Discovery local run results API (schema v2, single ``*_discovery_run.json`` artifact)."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, Query

from local_runner.discovery_run_v2 import DISCOVERY_RUN_SCHEMA_VERSION, DISCOVERY_RUN_SUFFIX

_DISCOVERY_RUN_BASENAME = re.compile(rf"^\d{{8}}_\d{{6}}{re.escape(DISCOVERY_RUN_SUFFIX)}$")


def discovery_run_stem_from_path(path: Path) -> str:
    if not path.name.endswith(DISCOVERY_RUN_SUFFIX):
        raise ValueError(f"expected path ending with {DISCOVERY_RUN_SUFFIX!r}")
    return path.name[: -len(DISCOVERY_RUN_SUFFIX)]


def summary_from_discovery_run(data: Dict[str, Any]) -> Dict[str, Any]:
    eop = data.get("end_of_process") if isinstance(data.get("end_of_process"), dict) else {}
    warnings = eop.get("warnings")
    persistence = data.get("persistence") if isinstance(data.get("persistence"), dict) else {}
    pipeline = data.get("pipeline") if isinstance(data.get("pipeline"), dict) else {}
    return {
        "status": eop.get("status"),
        "elapsed_ms": eop.get("elapsed_ms"),
        "task_count": eop.get("task_count") or pipeline.get("task_count"),
        "dry_run": eop.get("dry_run") if "dry_run" in eop else data.get("dry_run"),
        "failed_task_key": eop.get("failed_task_key"),
        "warnings": list(warnings) if isinstance(warnings, list) else [],
        "persistence_node_count": persistence.get("node_count"),
    }


def resolve_discovery_run_path(
    rel: str,
    *,
    run_results_prefix: str,
    run_results_root: Path,
    safe_rel_path,
    rel_under_module,
) -> Path:
    rel_n = rel.strip().replace("\\", "/")
    if not rel_n.startswith(run_results_prefix):
        raise HTTPException(status_code=400, detail="Path must be under local_run_results/")
    path = safe_rel_path(rel_n)
    root_resolved = run_results_root.resolve()
    try:
        path.relative_to(root_resolved)
    except ValueError as e:
        raise HTTPException(status_code=400, detail="Path escapes local_run_results") from e
    if not _DISCOVERY_RUN_BASENAME.match(path.name):
        raise HTTPException(
            status_code=400,
            detail=f"Only YYYYMMDD_HHMMSS{DISCOVERY_RUN_SUFFIX}",
        )
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Discovery run file not found")
    return path


def load_discovery_run_json(path: Path, load_json_object) -> Dict[str, Any]:
    data = load_json_object(path)
    if int(data.get("schema_version") or 0) != DISCOVERY_RUN_SCHEMA_VERSION:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported schema_version (expected {DISCOVERY_RUN_SCHEMA_VERSION})",
        )
    return data


def list_discovery_runs(
    *,
    run_results_root: Path,
    run_results_prefix: str,
    rel_under_module,
    read_json_run_scope,
    run_scope_matches_key,
    load_json_object,
    summary_from_run,
    run_scope_key: Optional[str] = None,
) -> Dict[str, Any]:
    root = run_results_root
    root.mkdir(parents=True, exist_ok=True)
    runs: List[Dict[str, Any]] = []
    if not root.is_dir():
        return {"runs": []}
    scope_filter = (run_scope_key or "").strip()
    for p in sorted(root.glob(f"*{DISCOVERY_RUN_SUFFIX}"), key=lambda x: x.stat().st_mtime, reverse=True):
        if not _DISCOVERY_RUN_BASENAME.match(p.name):
            continue
        if scope_filter:
            rs = read_json_run_scope(p)
            if not run_scope_matches_key(rs, scope_filter):
                continue
        stem_ts = discovery_run_stem_from_path(p)
        entry: Dict[str, Any] = {
            "stem": stem_ts,
            "run_rel": rel_under_module(p),
            "mtime_ms": int(p.stat().st_mtime * 1000),
        }
        try:
            entry.update(summary_from_run(load_json_object(p)))
        except HTTPException:
            pass
        runs.append(entry)
    return {"runs": runs}


def discovery_run_detail_payload(
    path: Path,
    data: Dict[str, Any],
    rel_under_module,
) -> Dict[str, Any]:
    return {
        "run_rel": rel_under_module(path),
        "schema_version": data.get("schema_version"),
        "run_scope": data.get("run_scope"),
        "run_id": data.get("run_id"),
        "dry_run": data.get("dry_run"),
        "end_of_process": data.get("end_of_process"),
        "summary": summary_from_discovery_run(data),
        "pipeline_task_count": (data.get("pipeline") or {}).get("task_count")
        if isinstance(data.get("pipeline"), dict)
        else None,
        "persistence_node_count": (data.get("persistence") or {}).get("node_count")
        if isinstance(data.get("persistence"), dict)
        else None,
    }


def paginate_pipeline_tasks(
    data: Dict[str, Any],
    *,
    offset: int,
    limit: int,
) -> Dict[str, Any]:
    pipeline = data.get("pipeline") if isinstance(data.get("pipeline"), dict) else {}
    tasks = pipeline.get("tasks") if isinstance(pipeline.get("tasks"), list) else []
    total = len(tasks)
    chunk = tasks[offset : offset + limit]
    return {"total": total, "offset": offset, "limit": limit, "items": chunk}


def persistence_nodes_from_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    persistence = data.get("persistence") if isinstance(data.get("persistence"), dict) else {}
    nodes = persistence.get("nodes") if isinstance(persistence.get("nodes"), list) else []
    return [n for n in nodes if isinstance(n, dict)]


def persistence_node_by_task_id(data: Dict[str, Any], task_id: str) -> Dict[str, Any]:
    tid = task_id.strip()
    for node in persistence_nodes_from_data(data):
        if str(node.get("task_id") or "") == tid:
            return node
    raise HTTPException(status_code=404, detail=f"No persistence node for task {tid!r}")


def merged_entities_from_data(data: Dict[str, Any]) -> Dict[str, Any]:
    persistence = data.get("persistence") if isinstance(data.get("persistence"), dict) else {}
    merged = persistence.get("merged_entities")
    return dict(merged) if isinstance(merged, dict) else {}
