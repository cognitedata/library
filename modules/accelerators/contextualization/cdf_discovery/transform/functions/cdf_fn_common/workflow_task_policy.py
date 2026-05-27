"""Per-function WorkflowVersion task policy defaults."""

from __future__ import annotations

from typing import Any

_POLICIES: dict[str, dict[str, Any]] = {
    "fn_etl_raw_cleanup": {
        "retries": 3,
        "onFailure": "skipTask",
        "timeout": 1800,
        "isAsyncComplete": False,
    },
    "fn_etl_file_annotation": {
        "retries": 2,
        "onFailure": "skipTask",
        "timeout": 3600,
        "isAsyncComplete": True,
    },
    "fn_etl_workflow_fanout_plan": {
        "retries": 3,
        "onFailure": "abortWorkflow",
        "timeout": 1800,
        "isAsyncComplete": False,
    },
}


def discovery_task_workflow_policy(function_external_id: str) -> dict[str, Any]:
    ext = str(function_external_id or "").strip()
    if ext in _POLICIES:
        return dict(_POLICIES[ext])
    return {"retries": 3, "onFailure": "abortWorkflow", "timeout": 7200, "isAsyncComplete": False}
