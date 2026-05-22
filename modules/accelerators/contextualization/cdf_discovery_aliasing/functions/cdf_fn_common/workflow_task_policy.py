"""CDF Data Workflow task retry / onFailure policy (shared by codegen and local runner)."""

from __future__ import annotations

from typing import Literal, TypedDict

DEFAULT_TASK_RETRIES = 3
DEFAULT_ON_FAILURE: Literal["abortWorkflow", "skipTask"] = "abortWorkflow"
ON_FAILURE_SKIP_TASK: Literal["skipTask"] = "skipTask"

SKIP_TASK_FUNCTIONS = frozenset({"fn_dm_discovery_raw_cleanup"})


class DiscoveryTaskWorkflowPolicy(TypedDict):
    retries: int
    onFailure: Literal["abortWorkflow", "skipTask"]


def discovery_task_workflow_policy(function_external_id: str) -> DiscoveryTaskWorkflowPolicy:
    """Return ``retries`` and ``onFailure`` for a discovery function external id."""
    fn = str(function_external_id or "").strip()
    on_failure: Literal["abortWorkflow", "skipTask"] = (
        ON_FAILURE_SKIP_TASK if fn in SKIP_TASK_FUNCTIONS else DEFAULT_ON_FAILURE
    )
    return DiscoveryTaskWorkflowPolicy(
        retries=DEFAULT_TASK_RETRIES,
        onFailure=on_failure,
    )
