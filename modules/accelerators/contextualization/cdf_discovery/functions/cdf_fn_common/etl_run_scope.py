"""Resolve All vs Incremental scope for pipeline runs and query nodes."""

from __future__ import annotations

from typing import Any, Mapping

QUERY_SCOPE_MODES = frozenset({"inherit", "all", "incremental"})


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"", "false", "0", "no", "off"}
    return bool(value)


def pipeline_parameters(data: Mapping[str, Any]) -> dict[str, Any]:
    configuration = _as_dict(data.get("configuration"))
    params = configuration.get("parameters")
    if isinstance(params, dict):
        return dict(params)
    return {}


def is_lookup_full_scan(cfg: Mapping[str, Any] | None = None) -> bool:
    task_cfg = _as_dict(cfg)
    return bool(task_cfg.get("lookup_full_scan"))


def incremental_change_processing_enabled(
    data: Mapping[str, Any],
    cfg: Mapping[str, Any] | None = None,
) -> bool:
    """True when cross-run incremental state should be read or written."""
    if is_lookup_full_scan(cfg):
        return False
    params = pipeline_parameters(data)
    if _as_bool(params.get("incremental_change_processing")):
        return True
    task_cfg = _as_dict(cfg)
    if _as_bool(task_cfg.get("incremental_change_processing")):
        return True
    return False


def resolve_query_scope_mode(cfg: Mapping[str, Any] | None = None) -> str:
    task_cfg = _as_dict(cfg)
    mode = str(task_cfg.get("query_scope_mode") or "inherit").strip().lower()
    if mode in QUERY_SCOPE_MODES:
        return mode
    return "inherit"


def resolve_effective_incremental_change_processing(
    data: Mapping[str, Any],
    cfg: Mapping[str, Any] | None = None,
) -> bool:
    """True when this query should use incremental listing (not full scope)."""
    if is_lookup_full_scan(cfg):
        return False
    mode = resolve_query_scope_mode(cfg)
    if mode == "all":
        return False
    if mode == "incremental":
        return True
    if "incremental_change_processing" in data:
        return _as_bool(data.get("incremental_change_processing"))
    params = pipeline_parameters(data)
    if "incremental_change_processing" in params:
        return _as_bool(params.get("incremental_change_processing"))
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
        return _as_bool(cfg.get("incremental_skip_unchanged"))
    params = pipeline_parameters(data)
    return _as_bool(params.get("incremental_skip_unchanged", True))


def resolve_workflow_scope(data: Mapping[str, Any]) -> str:
    params = pipeline_parameters(data)
    ws = str(params.get("workflow_scope") or "").strip()
    if ws:
        return ws
    configuration = _as_dict(data.get("configuration"))
    return str(configuration.get("id") or "").strip()
