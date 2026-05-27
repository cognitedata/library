"""Complete CDF workflow orchestration tasks (isAsyncComplete functions)."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional


def orchestration_task_id_from_data(data: Mapping[str, Any]) -> Optional[int]:
    raw = data.get("cogniteOrchestrationTaskId") or data.get("cognite_orchestration_task_id")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def complete_workflow_task(
    client: Any,
    task_id: int,
    *,
    status: str = "Completed",
    output: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Mark an async workflow function task complete.

    Uses Workflows API when available on the client.
    """
    output = output or {}
    workflows = getattr(client, "workflows", None)
    if workflows is None:
        raise RuntimeError("Cognite client has no workflows API")
    tasks_api = getattr(workflows, "tasks", None)
    if tasks_api is not None and hasattr(tasks_api, "complete"):
        tasks_api.complete(task_id, status=status, output=output)
        return
    if hasattr(workflows, "task_complete"):
        workflows.task_complete(task_id, status=status, output=output)
        return
    raise RuntimeError(
        "Could not find workflows.tasks.complete on CogniteClient; verify cognite-sdk version"
    )


def fail_workflow_task(
    client: Any,
    task_id: int,
    *,
    error_message: str,
    output: Optional[Dict[str, Any]] = None,
) -> None:
    out = dict(output or {})
    out["error"] = error_message
    complete_workflow_task(client, task_id, status="Failed", output=out)
