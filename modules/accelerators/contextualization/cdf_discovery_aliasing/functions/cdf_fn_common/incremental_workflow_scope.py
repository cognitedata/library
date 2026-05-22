"""Resolve ``workflow_scope`` for cross-run incremental state (multi-workflow isolation)."""

from __future__ import annotations

from typing import Any, Dict, Mapping

DEFAULT_LOCAL_WORKFLOW_SCOPE = "workflow_local"


def resolve_workflow_scope(
    configuration: Mapping[str, Any],
    ke_params: Mapping[str, Any],
) -> str:
    """
    Partition key for incremental watermarks / hashes (parallel deployed workflows).

    Order: ``key_extraction.config.parameters.workflow_scope`` → ``configuration.scope.id``
    → :data:`DEFAULT_LOCAL_WORKFLOW_SCOPE`.
    """
    ws = str(ke_params.get("workflow_scope") or "").strip()
    if ws:
        return ws
    scope = configuration.get("scope")
    if isinstance(scope, dict):
        sid = str(scope.get("id") or "").strip()
        if sid:
            return sid
    return DEFAULT_LOCAL_WORKFLOW_SCOPE


def resolve_source_view_fingerprint(
    ke_params: Mapping[str, Any],
    *,
    scope_key: str,
) -> str:
    """FDM-aligned view disambiguation; default ``scope_key`` when unset."""
    fp = str(ke_params.get("source_view_fingerprint") or "").strip()
    return fp if fp else str(scope_key or "").strip()
