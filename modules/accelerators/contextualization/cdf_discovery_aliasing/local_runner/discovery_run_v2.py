"""Discovery local run results schema v2 (single artifact, persistence-first)."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

from .persistence_cohort_snapshot import PERSISTENCE_SNAPSHOT_FUNCTIONS
from .persistence_instances_merge import build_merged_persistence_instances

DISCOVERY_RUN_SCHEMA_VERSION = 2
DISCOVERY_RUN_SUFFIX = "_discovery_run.json"
END_OF_PROCESS_SCHEMA_VERSION = 2


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
    else:
        status = "succeeded"
    ft: Optional[str] = failed_task_id
    if ft is None:
        for t in tasks:
            if t.get("status") == "failed":
                ft = str(t.get("task_id") or "")
                break
    return {
        "schema_version": END_OF_PROCESS_SCHEMA_VERSION,
        "status": status,
        "failed_task_key": ft,
        "elapsed_ms": elapsed_ms,
        "dry_run": bool(dry_run),
        "task_count": len(tasks),
        "warnings": list(warnings or []),
    }

_FN_TO_PERSISTENCE_KIND: Dict[str, str] = {
    "fn_dm_view_save": "view_save",
    "fn_dm_raw_save": "raw_save",
    "fn_dm_classic_save": "classic_save",
    "fn_dm_inverted_index": "inverted_index",
}

_FN_TO_TASK_CATEGORY: Dict[str, str] = {
    "fn_dm_view_query": "query",
    "fn_dm_raw_query": "query",
    "fn_dm_classic_query": "query",
    "fn_dm_sql_query": "query",
    "fn_dm_transform": "transform",
    "fn_dm_validate": "validate",
    "fn_dm_filter": "filter",
    "fn_dm_confidence_filter": "filter",
    "fn_dm_join": "transform",
    "fn_dm_view_save": "persistence",
    "fn_dm_raw_save": "persistence",
    "fn_dm_classic_save": "persistence",
    "fn_dm_inverted_index": "persistence",
    "fn_dm_discovery_raw_cleanup": "cleanup",
}


def _parse_task_message(message: Any) -> Optional[Dict[str, Any]]:
    if isinstance(message, dict):
        return dict(message)
    if isinstance(message, str) and message.strip().startswith("{"):
        try:
            parsed = json.loads(message)
            return dict(parsed) if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def _task_category(function_external_id: str) -> str:
    return _FN_TO_TASK_CATEGORY.get(function_external_id, "other")


def _persistence_kind(function_external_id: str) -> str:
    return _FN_TO_PERSISTENCE_KIND.get(function_external_id, "unknown")


def _output_kind_for_persistence(function_external_id: str) -> str:
    if function_external_id == "fn_dm_inverted_index":
        return "inverted_index_sink"
    if function_external_id == "fn_dm_view_save":
        return "dm_instances_apply"
    if function_external_id == "fn_dm_raw_save":
        return "raw_upsert"
    if function_external_id == "fn_dm_classic_save":
        return "classic_write"
    return "unknown"


def _build_pipeline_tasks(
    *,
    local_run_tasks: List[Dict[str, Any]],
    discovery_task_outputs: Mapping[str, Any],
    compiled_workflow: Optional[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    compiled_index: Dict[str, Dict[str, Any]] = {}
    if isinstance(compiled_workflow, dict):
        for t in compiled_workflow.get("tasks") or []:
            if isinstance(t, dict) and t.get("id"):
                compiled_index[str(t["id"])] = t

    timing_by_id: Dict[str, Dict[str, Any]] = {}
    for rec in local_run_tasks:
        if not isinstance(rec, dict):
            continue
        tid = str(rec.get("task_id") or "").strip()
        if tid:
            timing_by_id[tid] = rec

    task_ids = sorted(set(timing_by_id.keys()) | {str(k) for k in discovery_task_outputs.keys()})
    out: List[Dict[str, Any]] = []
    for tid in task_ids:
        timing = timing_by_id.get(tid) or {}
        compiled = compiled_index.get(tid) or {}
        compiled_task = (
            timing.get("compiled_task") if isinstance(timing.get("compiled_task"), dict) else {}
        )
        fn_ext = str(
            timing.get("function_external_id")
            or compiled.get("function_external_id")
            or compiled_task.get("function_external_id")
            or ""
        ).strip()
        output_entry = discovery_task_outputs.get(tid)
        status = None
        message = None
        if isinstance(output_entry, dict):
            status = output_entry.get("status")
            message = output_entry.get("message")
        if status is None:
            status = timing.get("status")
        parsed = _parse_task_message(message) or _parse_task_message(timing.get("output"))
        out.append(
            {
                "task_id": tid,
                "function_external_id": fn_ext or None,
                "canvas_node_id": compiled.get("canvas_node_id") or compiled_task.get("canvas_node_id"),
                "pipeline_node_id": compiled.get("pipeline_node_id"),
                "depends_on": list(compiled.get("depends_on") or [])
                if isinstance(compiled.get("depends_on"), list)
                else [],
                "category": _task_category(fn_ext),
                "status": status,
                "duration_sec": timing.get("duration_sec"),
                "error": timing.get("error"),
                "output": parsed,
            }
        )
    return out


def _normalize_cohort_rows(rows: Any) -> List[Dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in rows:
        if isinstance(item, dict):
            out.append(
                {
                    "key": str(item.get("key") or ""),
                    "columns": dict(item.get("columns") or {})
                    if isinstance(item.get("columns"), dict)
                    else {},
                }
            )
    return out


def _build_persistence_nodes(
    *,
    handler_data_snapshots: Mapping[str, Any],
    pipeline_tasks: List[Dict[str, Any]],
    compiled_workflow: Optional[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    snap_by_id: Dict[str, Dict[str, Any]] = {}
    for key, entry in handler_data_snapshots.items():
        if isinstance(entry, dict):
            snap_by_id[str(entry.get("task_id") or key)] = entry

    timing_by_id = {str(t["task_id"]): t for t in pipeline_tasks if t.get("task_id")}

    compiled_persistence: List[Dict[str, Any]] = []
    if isinstance(compiled_workflow, dict):
        for t in compiled_workflow.get("tasks") or []:
            if not isinstance(t, dict):
                continue
            fn_ext = str(t.get("function_external_id") or "").strip()
            if fn_ext in PERSISTENCE_SNAPSHOT_FUNCTIONS:
                compiled_persistence.append(t)

    seen: set[str] = set()
    nodes: List[Dict[str, Any]] = []

    def _append_node(task_id: str, fn_ext: str, compiled_task: Mapping[str, Any]) -> None:
        if task_id in seen:
            return
        seen.add(task_id)
        entry = snap_by_id.get(task_id) or {}
        timing = timing_by_id.get(task_id) or {}
        cohort = entry.get("cohort_snapshot") if isinstance(entry.get("cohort_snapshot"), dict) else {}
        pred = cohort.get("predecessor_cohort") if isinstance(cohort.get("predecessor_cohort"), dict) else {}
        inv = (
            cohort.get("inverted_index_persistence")
            if isinstance(cohort.get("inverted_index_persistence"), dict)
            else {}
        )
        handler_summary = entry.get("handler_summary") if isinstance(entry.get("handler_summary"), dict) else {}
        if not handler_summary and timing.get("output"):
            handler_summary = dict(timing["output"])

        label = ""
        payload = compiled_task.get("payload") if isinstance(compiled_task.get("payload"), dict) else {}
        cfg = payload.get("config") if isinstance(payload.get("config"), dict) else {}
        label = str(cfg.get("description") or compiled_task.get("pipeline_node_id") or task_id)

        input_cohort = {
            "predecessor_sources": list(pred.get("predecessor_sources") or []),
            "predecessor_canvas_node_ids": list(pred.get("predecessor_canvas_node_ids") or []),
            "entity_row_count": pred.get("cohort_row_count"),
            "truncated": pred.get("truncated"),
            "row_limit": pred.get("row_limit"),
            "entity_rows": _normalize_cohort_rows(pred.get("cohort_rows")),
            "rows_examined": pred.get("rows_examined"),
            "raw_scan_truncated": pred.get("raw_scan_truncated"),
            "error": pred.get("error"),
        }

        if fn_ext == "fn_dm_inverted_index":
            output_block: Dict[str, Any] = {
                "kind": "inverted_index_sink",
                "raw_db": inv.get("raw_db"),
                "raw_table": inv.get("raw_table"),
                "row_count": inv.get("row_count"),
                "truncated": inv.get("truncated"),
                "row_limit": inv.get("row_limit"),
                "rows_examined": inv.get("rows_examined"),
                "raw_scan_truncated": inv.get("raw_scan_truncated"),
                "index_rows": _normalize_cohort_rows(inv.get("index_rows")),
                "error": inv.get("error"),
            }
        else:
            output_block = {
                "kind": _output_kind_for_persistence(fn_ext),
                "summary": dict(handler_summary),
                "raw_db": handler_summary.get("raw_db") or cohort.get("raw_db"),
                "raw_table": handler_summary.get("raw_table") or cohort.get("raw_table"),
            }

        nodes.append(
            {
                "task_id": task_id,
                "kind": _persistence_kind(fn_ext),
                "function_external_id": fn_ext,
                "label": label,
                "canvas_node_id": compiled_task.get("canvas_node_id"),
                "pipeline_node_id": compiled_task.get("pipeline_node_id"),
                "status": timing.get("status") or entry.get("status"),
                "duration_sec": timing.get("duration_sec"),
                "handler_result": dict(handler_summary),
                "input_cohort": input_cohort,
                "output": output_block,
                "snapshot_present": bool(entry),
            }
        )

    for t in compiled_persistence:
        tid = str(t.get("id") or "").strip()
        fn_ext = str(t.get("function_external_id") or "").strip()
        if tid and fn_ext:
            _append_node(tid, fn_ext, t)

    for tid, entry in sorted(snap_by_id.items()):
        fn_ext = str(entry.get("function_external_id") or "").strip()
        if fn_ext in PERSISTENCE_SNAPSHOT_FUNCTIONS:
            _append_node(tid, fn_ext, {})

    nodes.sort(key=lambda n: (str(n.get("kind") or ""), str(n.get("task_id") or "")))
    return nodes


def compose_discovery_run_document(
    *,
    run_scope: Mapping[str, Any],
    run_id: str,
    dry_run: bool,
    wall_t0: float,
    local_run_tasks: List[Dict[str, Any]],
    discovery_task_outputs: Mapping[str, Any],
    handler_data_snapshots: Mapping[str, Any],
    compiled_workflow: Optional[Mapping[str, Any]] = None,
    raw_table_samples: Optional[Mapping[str, Any]] = None,
    failed_task_id: Optional[str] = None,
    warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build the canonical v2 discovery run JSON document."""
    pipeline_tasks = _build_pipeline_tasks(
        local_run_tasks=local_run_tasks,
        discovery_task_outputs=discovery_task_outputs,
        compiled_workflow=compiled_workflow,
    )
    persistence_nodes = _build_persistence_nodes(
        handler_data_snapshots=handler_data_snapshots,
        pipeline_tasks=pipeline_tasks,
        compiled_workflow=compiled_workflow,
    )
    merged = build_merged_persistence_instances(handler_data_snapshots)

    doc: Dict[str, Any] = {
        "schema_version": DISCOVERY_RUN_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_scope": dict(run_scope),
        "run_id": str(run_id or "").strip(),
        "dry_run": bool(dry_run),
        "end_of_process": build_end_of_process(
            tasks=local_run_tasks,
            wall_t0=wall_t0,
            dry_run=dry_run,
            failed_task_id=failed_task_id,
            warnings=warnings,
        ),
        "pipeline": {
            "task_count": len(pipeline_tasks),
            "tasks": pipeline_tasks,
        },
        "persistence": {
            "node_count": len(persistence_nodes),
            "nodes": persistence_nodes,
            "merged_entities": merged,
        },
    }
    if raw_table_samples and isinstance(raw_table_samples, dict) and raw_table_samples.get("tables"):
        doc["raw_table_samples"] = dict(raw_table_samples)
    return doc


def discovery_run_path_for_timestamp(results_dir: Any, timestamp_stem: str) -> Any:
    from pathlib import Path

    return Path(results_dir) / f"{timestamp_stem}{DISCOVERY_RUN_SUFFIX}"


def write_discovery_run_artifact(
    *,
    results_dir: Any,
    run_scope: Mapping[str, Any],
    run_id: str,
    dry_run: bool,
    wall_t0: float,
    local_run_tasks: List[Dict[str, Any]],
    discovery_task_outputs: Mapping[str, Any],
    handler_data_snapshots: Mapping[str, Any],
    compiled_workflow: Optional[Mapping[str, Any]] = None,
    raw_table_samples: Optional[Mapping[str, Any]] = None,
    failed_task_id: Optional[str] = None,
    warnings: Optional[List[str]] = None,
    timestamp_stem: Optional[str] = None,
) -> Any:
    """Write ``{timestamp}_discovery_run.json``; returns the path written."""
    from pathlib import Path

    stem = (timestamp_stem or "").strip() or datetime.now().strftime("%Y%m%d_%H%M%S")
    discovery_path = Path(results_dir) / f"{stem}{DISCOVERY_RUN_SUFFIX}"
    out_doc = compose_discovery_run_document(
        run_scope=run_scope,
        run_id=str(run_id or "").strip(),
        dry_run=bool(dry_run),
        wall_t0=float(wall_t0 or 0.0),
        local_run_tasks=list(local_run_tasks),
        discovery_task_outputs=discovery_task_outputs,
        handler_data_snapshots=dict(handler_data_snapshots),
        compiled_workflow=compiled_workflow,
        raw_table_samples=raw_table_samples,
        failed_task_id=failed_task_id,
        warnings=warnings,
    )
    with discovery_path.open("w", encoding="utf-8") as f:
        json.dump(out_doc, f, indent=2, default=str)
    return discovery_path


__all__ = [
    "DISCOVERY_RUN_SCHEMA_VERSION",
    "DISCOVERY_RUN_SUFFIX",
    "END_OF_PROCESS_SCHEMA_VERSION",
    "PERSISTENCE_SNAPSHOT_FUNCTIONS",
    "build_end_of_process",
    "compose_discovery_run_document",
    "discovery_run_path_for_timestamp",
    "write_discovery_run_artifact",
]
