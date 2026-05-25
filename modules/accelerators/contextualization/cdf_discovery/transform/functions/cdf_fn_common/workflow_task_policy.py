"""Per-function WorkflowVersion task policy defaults."""

from __future__ import annotations


def discovery_task_workflow_policy(function_external_id: str) -> dict[str, int | str]:
    _ = function_external_id
    return {"retries": 3, "onFailure": "abortWorkflow"}
