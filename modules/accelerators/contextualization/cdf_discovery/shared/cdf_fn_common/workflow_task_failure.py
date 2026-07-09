"""Resolve CDF workflow ``onFailure`` for canvas IR tasks and local execution."""

from __future__ import annotations

from typing import Any, Mapping, Optional


def _normalized_on_failure(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def resolve_task_on_failure(
    task: Mapping[str, Any],
    *,
    function_external_id: str | None = None,
) -> str:
    """``skipTask`` or ``abortWorkflow`` (CDF workflow task failure policy)."""
    explicit = _normalized_on_failure(task.get("on_failure")) or _normalized_on_failure(
        task.get("onFailure")
    )
    if explicit:
        return explicit

    payload = task.get("payload")
    cfg: Mapping[str, Any] = {}
    if isinstance(payload, dict):
        nested = payload.get("config")
        if isinstance(nested, dict):
            cfg = nested
    cfg_failure = _normalized_on_failure(cfg.get("on_failure")) or _normalized_on_failure(
        cfg.get("onFailure")
    )
    if cfg_failure:
        return cfg_failure

    fn_ext = (function_external_id or str(task.get("function_external_id") or "")).strip()
    if fn_ext:
        from cdf_fn_common.workflow_task_policy import discovery_task_workflow_policy

        return str(discovery_task_workflow_policy(fn_ext)["onFailure"])

    task_type = str(task.get("task_type") or "").strip()
    if task_type in {"dynamic", "subworkflow"}:
        return "abortWorkflow"
    return "skipTask"


def abort_workflow_on_task_failure(on_failure: str) -> bool:
    return str(on_failure or "").strip() == "abortWorkflow"
