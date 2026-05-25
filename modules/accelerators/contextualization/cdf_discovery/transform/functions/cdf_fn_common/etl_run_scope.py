"""Resolve All vs Incremental scope for pipeline runs and query nodes."""

from __future__ import annotations

from typing import Any, Mapping

QUERY_SCOPE_MODES = frozenset({"inherit", "all", "incremental"})


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def pipeline_parameters(data: Mapping[str, Any]) -> dict[str, Any]:
    configuration = _as_dict(data.get("configuration"))
    params = configuration.get("parameters")
    if isinstance(params, dict):
        return dict(params)
    top = data.get("parameters")
    return dict(top) if isinstance(top, dict) else {}


def incremental_change_processing_enabled(
    data: Mapping[str, Any],
    cfg: Mapping[str, Any] | None = None,
) -> bool:
    """True when cross-run incremental state should be read or written."""
    if bool(data.get("incremental_change_processing")):
        return True
    if bool(data.get("incremental")):
        return True
    params = pipeline_parameters(data)
    if bool(params.get("incremental_change_processing")):
        return True
    if bool(params.get("incremental")):
        return True
    task_cfg = _as_dict(cfg)
    if bool(task_cfg.get("incremental_change_processing")):
        return True
    return False


def resolve_effective_incremental_change_processing(
    data: Mapping[str, Any],
    cfg: Mapping[str, Any] | None = None,
) -> bool:
    """True when this query should use incremental listing (not full scope)."""
    task_cfg = _as_dict(cfg)
    mode = str(task_cfg.get("query_scope_mode") or task_cfg.get("scope_mode") or "inherit").strip().lower()
    if mode == "all":
        return False
    if mode == "incremental":
        return True
    if "incremental_change_processing" in data:
        return bool(data.get("incremental_change_processing"))
    params = pipeline_parameters(data)
    if "incremental_change_processing" in params:
        return bool(params.get("incremental_change_processing"))
    return False


def incremental_listing_narrowed(
    data: Mapping[str, Any],
    cfg: Mapping[str, Any] | None = None,
) -> bool:
    """True when listing should use watermark filter and may skip unchanged rows."""
    return incremental_change_processing_enabled(data, cfg) and resolve_effective_incremental_change_processing(
        data, cfg
    )


def incremental_skip_unchanged(
    data: Mapping[str, Any],
    cfg: Mapping[str, Any],
    *,
    listing_narrowed: bool,
) -> bool:
    if not listing_narrowed:
        return False
    if "incremental_skip_unchanged" in cfg:
        return bool(cfg.get("incremental_skip_unchanged"))
    params = pipeline_parameters(data)
    return bool(params.get("incremental_skip_unchanged", True))


def resolve_workflow_scope(data: Mapping[str, Any]) -> str:
    params = pipeline_parameters(data)
    ws = str(params.get("workflow_scope") or "").strip()
    if ws:
        return ws
    configuration = _as_dict(data.get("configuration"))
    return str(configuration.get("id") or "").strip()
