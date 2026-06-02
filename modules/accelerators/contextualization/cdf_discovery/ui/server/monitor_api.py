"""FastAPI routes for monitor workflow state dashboards."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query

from ui.server import cdf_browse, transform_registry

router = APIRouter(prefix="/api/monitor", tags=["monitor"])


def _cdf_client():
    from ui.server.main import _cdf_client as main_cdf_client

    return main_cdf_client()


def _to_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "dump"):
        try:
            dumped = value.dump(camel_case=False)
            if isinstance(dumped, dict):
                return dumped
        except Exception:
            pass
    if hasattr(value, "__dict__"):
        raw = {
            key: val
            for key, val in vars(value).items()
            if not key.startswith("_") and not callable(val)
        }
        if raw:
            return raw
    return {}


def _iso_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (int, float)):
        try:
            if value > 1_000_000_000_000:
                return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc).isoformat()
            if value > 1_000_000_000:
                return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    return text or None


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _duration_ms(start_iso: Optional[str], end_iso: Optional[str]) -> Optional[int]:
    start = _parse_iso(start_iso)
    end = _parse_iso(end_iso)
    if start is None or end is None:
        return None
    delta_ms = int((end - start).total_seconds() * 1000)
    if delta_ms < 0:
        return None
    return delta_ms


def _normalize_status(raw: Any) -> str:
    text = str(raw or "").strip().lower()
    if text in {"completed", "complete", "done", "succeeded", "success"}:
        return "succeeded"
    if text in {"failed", "failure", "error", "timed_out", "timeout", "cancelled", "canceled"}:
        return "failed"
    if text in {"running", "in_progress", "inprogress", "started", "queued", "created"}:
        return "running"
    if text in {"skipped"}:
        return "skipped"
    return text or "unknown"


def _task_rows_from_execution(execution: Any) -> List[Dict[str, Any]]:
    payload = _to_dict(execution)
    candidates: list[Any] = [
        payload.get("tasks"),
        payload.get("task_results"),
        payload.get("executed_tasks"),
        payload.get("task_execution_details"),
    ]
    for item in candidates:
        if isinstance(item, list):
            rows: List[Dict[str, Any]] = []
            for row in item:
                row_dict = _to_dict(row) if not isinstance(row, dict) else row
                if row_dict:
                    rows.append(row_dict)
            if rows:
                return rows
    return []


def _task_failure_counts(task_rows: List[Dict[str, Any]]) -> Tuple[int, int]:
    if not task_rows:
        return 0, 0
    failed = 0
    for task in task_rows:
        status = _normalize_status(task.get("status") or task.get("task_status"))
        if status == "failed":
            failed += 1
    return failed, len(task_rows)


def _normalize_cdf_execution(execution: Any, *, detailed: bool = False) -> Dict[str, Any]:
    payload = _to_dict(execution)
    start_iso = _iso_or_none(
        payload.get("start_time")
        or payload.get("created_time")
        or payload.get("started_time")
        or payload.get("createdTime")
    )
    end_iso = _iso_or_none(
        payload.get("end_time")
        or payload.get("finished_time")
        or payload.get("last_updated_time")
        or payload.get("endTime")
    )
    task_rows = _task_rows_from_execution(execution) if detailed else []
    failed_tasks, total_tasks = _task_failure_counts(task_rows)
    run_status = _normalize_status(payload.get("status"))
    reason = (
        payload.get("reason_for_incompletion")
        or payload.get("reason")
        or payload.get("error")
        or payload.get("message")
    )
    return {
        "source": "cdf",
        "run_id": str(
            payload.get("id")
            or payload.get("execution_id")
            or payload.get("external_id")
            or ""
        ).strip(),
        "workflow_id": str(payload.get("workflow_external_id") or "").strip(),
        "workflow_version": str(payload.get("workflow_version") or "").strip() or None,
        "status": run_status,
        "start_time": start_iso,
        "end_time": end_iso,
        "duration_ms": _duration_ms(start_iso, end_iso),
        "failed_tasks": failed_tasks,
        "total_tasks": total_tasks,
        "error_summary": str(reason).strip() if reason else None,
        "tasks": [
            {
                "task_id": str(t.get("external_id") or t.get("id") or "").strip(),
                "status": _normalize_status(t.get("status") or t.get("task_status")),
                "error_summary": str(
                    t.get("reason_for_incompletion") or t.get("error") or ""
                ).strip()
                or None,
            }
            for t in task_rows
        ],
    }


def _latest_status(runs: List[Dict[str, Any]]) -> str:
    if not runs:
        return "unknown"
    sorted_runs = sorted(
        runs,
        key=lambda row: row.get("start_time") or row.get("end_time") or "",
        reverse=True,
    )
    return str(sorted_runs[0].get("status") or "unknown")


def _build_workflow_rows(client: Any, *, run_limit: int = 200) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    workflow_rows: Dict[str, Dict[str, Any]] = {}
    runs_by_workflow: Dict[str, List[Dict[str, Any]]] = {}

    for row in _safe_list_workflows(client, limit=1000):
        workflow_id = str(row.get("external_id") or "").strip()
        if not workflow_id:
            continue
        workflow_rows[workflow_id] = {
            "workflow_id": workflow_id,
            "label": str(row.get("label") or workflow_id),
            "sources": ["cdf"],
        }

    for entry in transform_registry.list_pipeline_tree_entries():
        pipeline_id = str(entry.get("id") or "").strip()
        if not pipeline_id:
            continue
        scope = str(entry.get("scope_suffix") or "").strip()
        try:
            pairing = transform_registry.pipeline_build_pairing(pipeline_id, scope_suffix=scope)
            workflow_id = str(pairing.get("workflow_external_id") or "").strip() or pipeline_id
        except Exception:
            workflow_id = pipeline_id
        existing = workflow_rows.get(workflow_id)
        if existing is None:
            workflow_rows[workflow_id] = {
                "workflow_id": workflow_id,
                "label": str(entry.get("label") or workflow_id),
                "sources": ["local"],
            }
        elif "local" not in existing["sources"]:
            existing["sources"] = [*existing["sources"], "local"]

    for execution in _safe_list_cdf_executions(client, limit=run_limit):
        normalized = _normalize_cdf_execution(execution)
        workflow_id = str(normalized.get("workflow_id") or "").strip()
        if not workflow_id:
            continue
        runs_by_workflow.setdefault(workflow_id, []).append(normalized)
        if workflow_id not in workflow_rows:
            workflow_rows[workflow_id] = {
                "workflow_id": workflow_id,
                "label": workflow_id,
                "sources": ["cdf"],
            }

    rows: List[Dict[str, Any]] = []
    for workflow_id, base in workflow_rows.items():
        runs = runs_by_workflow.get(workflow_id, [])
        run_count = len(runs)
        failed_count = sum(1 for r in runs if r.get("status") == "failed")
        succeeded_count = sum(1 for r in runs if r.get("status") == "succeeded")
        running_count = sum(1 for r in runs if r.get("status") == "running")
        last_run_time = None
        if runs:
            sorted_runs = sorted(
                runs,
                key=lambda row: row.get("start_time") or row.get("end_time") or "",
                reverse=True,
            )
            latest = sorted_runs[0]
            last_run_time = latest.get("start_time") or latest.get("end_time")
        rows.append(
            {
                **base,
                "run_count": run_count,
                "failed_count": failed_count,
                "succeeded_count": succeeded_count,
                "running_count": running_count,
                "failure_rate": (failed_count / run_count) if run_count else 0.0,
                "latest_status": _latest_status(runs),
                "last_run_time": last_run_time,
                "degraded": failed_count > 0 and running_count > 0,
            }
        )
    rows.sort(key=lambda row: str(row.get("label") or row.get("workflow_id") or "").lower())
    all_runs = [run for runs in runs_by_workflow.values() for run in runs]
    return rows, all_runs


def _safe_list_workflows(client: Any, *, limit: int) -> List[Dict[str, Any]]:
    try:
        return cdf_browse.list_workflows(client, limit=limit)
    except Exception:
        return []


def _safe_list_cdf_executions(client: Any, *, limit: int) -> List[Any]:
    workflows_api = getattr(client, "workflows", None)
    executions_api = getattr(workflows_api, "executions", None)
    list_fn = getattr(executions_api, "list", None)
    if list_fn is None:
        return []
    try:
        return list(list_fn(limit=limit))
    except Exception:
        # SDK 7.x requires one of workflow_version_ids / created_time_* / statuses.
        pass

    attempts: List[Dict[str, Any]] = [
        {"limit": limit, "created_time_start": 0},
        {
            "limit": limit,
            "statuses": [
                "running",
                "completed",
                "failed",
                "timed_out",
                "terminated",
                "cancelled",
            ],
        },
    ]
    for kwargs in attempts:
        try:
            return list(list_fn(**kwargs))
        except Exception:
            continue
    return []


def _safe_retrieve_cdf_execution_detail(client: Any, execution_id: str, fallback: Any) -> Any:
    workflows_api = getattr(client, "workflows", None)
    executions_api = getattr(workflows_api, "executions", None)
    retrieve_fn = getattr(executions_api, "retrieve_detailed", None)
    if retrieve_fn is None:
        return fallback
    try:
        return retrieve_fn(execution_id)
    except Exception:
        return fallback


def _safe_list_workflow_triggers(client: Any, *, limit: int) -> List[Any]:
    workflows_api = getattr(client, "workflows", None)
    triggers_api = getattr(workflows_api, "triggers", None)
    list_fn = getattr(triggers_api, "list", None)
    if list_fn is None:
        return []
    try:
        return list(list_fn(limit=limit))
    except Exception:
        return []


def _extract_cron_expression(trigger_rule: Any) -> Optional[str]:
    if isinstance(trigger_rule, str):
        text = trigger_rule.strip()
        return text or None
    if not isinstance(trigger_rule, dict):
        return None
    direct = (
        trigger_rule.get("cron_expression")
        or trigger_rule.get("cronExpression")
        or trigger_rule.get("expression")
    )
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    schedule = trigger_rule.get("schedule")
    if isinstance(schedule, dict):
        nested = (
            schedule.get("cron_expression")
            or schedule.get("cronExpression")
            or schedule.get("expression")
        )
        if isinstance(nested, str) and nested.strip():
            return nested.strip()
    return None


def _build_pipeline_refs_by_workflow() -> Dict[str, List[Dict[str, str]]]:
    out: Dict[str, List[Dict[str, str]]] = {}
    for entry in transform_registry.list_registry_entries():
        pipeline_id = str(entry.get("id") or "").strip()
        if not pipeline_id:
            continue
        scope = str(entry.get("scope_suffix") or "").strip()
        try:
            pairing = transform_registry.pipeline_build_pairing(pipeline_id, scope_suffix=scope)
            workflow_id = str(pairing.get("workflow_external_id") or "").strip()
        except Exception:
            workflow_id = ""
        if not workflow_id:
            continue
        out.setdefault(workflow_id, []).append(
            {
                "pipeline_id": pipeline_id,
                "pipeline_label": str(entry.get("label") or pipeline_id),
                "scope_suffix": scope,
            }
        )
    return out


def _avg_duration_ms_for_runs(
    runs: List[Dict[str, Any]],
    *,
    now_utc: datetime,
    lookback_days: int,
) -> Optional[int]:
    cutoff = now_utc - timedelta(days=lookback_days)
    values: List[int] = []
    for run in runs:
        start = _parse_iso(run.get("start_time"))
        if start is None or start < cutoff:
            continue
        duration = run.get("duration_ms")
        if isinstance(duration, (int, float)) and duration >= 0:
            values.append(int(duration))
    if not values:
        return None
    return int(sum(values) / len(values))


@router.get("/workflow-state/summary")
def workflow_state_summary(
    run_limit: int = Query(200, ge=1, le=2000),
) -> Dict[str, Any]:
    client = _cdf_client()
    workflow_rows, all_runs = _build_workflow_rows(client, run_limit=run_limit)
    status_counts = {
        "running": 0,
        "succeeded": 0,
        "failed": 0,
        "unknown": 0,
    }
    for row in workflow_rows:
        status = str(row.get("latest_status") or "unknown")
        if status not in status_counts:
            status_counts["unknown"] += 1
        else:
            status_counts[status] += 1
    return {
        "workflow_count": len(workflow_rows),
        "run_count": len(all_runs),
        "running_workflows": status_counts["running"],
        "succeeded_workflows": status_counts["succeeded"],
        "failed_workflows": status_counts["failed"],
        "degraded_workflows": sum(1 for row in workflow_rows if row.get("degraded")),
        "status_counts": status_counts,
    }


@router.get("/workflow-state/workflows")
def workflow_state_workflows(
    run_limit: int = Query(200, ge=1, le=2000),
) -> Dict[str, Any]:
    client = _cdf_client()
    rows, _ = _build_workflow_rows(client, run_limit=run_limit)
    return {"workflows": rows}


@router.get("/workflow-state/workflows/{workflow_id}")
def workflow_state_workflow_detail(
    workflow_id: str,
    runs_limit: int = Query(20, ge=1, le=200),
) -> Dict[str, Any]:
    workflow_id_s = (workflow_id or "").strip()
    if not workflow_id_s:
        raise HTTPException(status_code=400, detail="workflow_id is required")
    client = _cdf_client()
    rows, _ = _build_workflow_rows(client, run_limit=500)
    workflow = next((row for row in rows if row["workflow_id"] == workflow_id_s), None)
    if workflow is None:
        raise HTTPException(status_code=404, detail=f"Workflow not found: {workflow_id_s}")

    detailed_runs: List[Dict[str, Any]] = []
    for execution in _safe_list_cdf_executions(client, limit=500):
        normalized = _normalize_cdf_execution(execution)
        if normalized.get("workflow_id") != workflow_id_s:
            continue
        run_id = str(normalized.get("run_id") or "").strip()
        detailed = _safe_retrieve_cdf_execution_detail(client, run_id, execution) if run_id else execution
        detailed_runs.append(_normalize_cdf_execution(detailed, detailed=True))
        if len(detailed_runs) >= runs_limit:
            break

    detailed_runs.sort(
        key=lambda row: row.get("start_time") or row.get("end_time") or "",
        reverse=True,
    )
    task_status_counts: Dict[str, int] = {}
    for run in detailed_runs:
        for task in run.get("tasks") or []:
            status = str(task.get("status") or "unknown")
            task_status_counts[status] = task_status_counts.get(status, 0) + 1

    return {
        "workflow": workflow,
        "runs": detailed_runs,
        "task_status_counts": task_status_counts,
    }


@router.get("/schedules")
def monitor_schedules(
    lookback_days: int = Query(7, ge=1, le=30),
    executions_limit: int = Query(1000, ge=1, le=5000),
    trigger_limit: int = Query(1000, ge=1, le=5000),
) -> Dict[str, Any]:
    client = _cdf_client()
    now_utc = datetime.now(timezone.utc)
    runs_by_workflow: Dict[str, List[Dict[str, Any]]] = {}
    for execution in _safe_list_cdf_executions(client, limit=executions_limit):
        normalized = _normalize_cdf_execution(execution)
        workflow_id = str(normalized.get("workflow_id") or "").strip()
        if not workflow_id:
            continue
        runs_by_workflow.setdefault(workflow_id, []).append(normalized)
    for run_list in runs_by_workflow.values():
        run_list.sort(
            key=lambda row: row.get("start_time") or row.get("end_time") or "",
            reverse=True,
        )

    pipeline_refs = _build_pipeline_refs_by_workflow()
    rows: List[Dict[str, Any]] = []
    for trigger in _safe_list_workflow_triggers(client, limit=trigger_limit):
        payload = _to_dict(trigger)
        trigger_rule = payload.get("trigger_rule") or payload.get("triggerRule")
        cron = _extract_cron_expression(trigger_rule)
        if not cron:
            continue
        workflow_id = str(payload.get("workflow_external_id") or payload.get("workflowExternalId") or "").strip()
        if not workflow_id:
            continue
        workflow_version = str(payload.get("workflow_version") or payload.get("workflowVersion") or "").strip() or None
        trigger_id = str(payload.get("external_id") or payload.get("id") or "").strip() or workflow_id
        runs = runs_by_workflow.get(workflow_id, [])
        refs = pipeline_refs.get(workflow_id, [])
        entity_type = "pipeline" if refs else "workflow"
        entity_label = refs[0]["pipeline_label"] if refs else workflow_id
        rows.append(
            {
                "trigger_id": trigger_id,
                "workflow_id": workflow_id,
                "workflow_version": workflow_version,
                "cron_expression": cron,
                "entity_type": entity_type,
                "entity_label": entity_label,
                "pipeline_refs": refs,
                "recent_runs_count": sum(
                    1
                    for run in runs
                    if (_parse_iso(run.get("start_time")) or now_utc) >= now_utc - timedelta(days=lookback_days)
                ),
                "avg_runtime_ms_7d": _avg_duration_ms_for_runs(
                    runs, now_utc=now_utc, lookback_days=lookback_days
                ),
                "last_run_time": runs[0].get("start_time") if runs else None,
                "last_status": runs[0].get("status") if runs else "unknown",
            }
        )

    rows.sort(key=lambda row: (row.get("entity_type") != "pipeline", str(row.get("entity_label") or "").lower()))
    return {"lookback_days": lookback_days, "schedules": rows}
