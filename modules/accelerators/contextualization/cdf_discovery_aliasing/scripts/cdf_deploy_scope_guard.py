"""Shared rules: only scoped ``workflows/<suffix>/`` artifacts may be deployed or run remotely.

Operator **local** / **template** documents (``workflow.local.*``, ``workflow_template/``) are not
deploy targets; block reserved ``scope_suffix`` values that would mirror those names.
"""

from __future__ import annotations

import re
from pathlib import Path

# Lowercased keys: suffixes that must not be used as a ``workflows/<suffix>/`` deploy leaf.
_RESERVED_SCOPE_SUFFIX_KEYS = frozenset(
    {
        "local",
        "template",
        "workflow_local",
        "workflow_template",
        "workflowlocal",
        "workflowtemplate",
    }
)


def _suffix_normalization_key(suffix: str) -> str:
    return suffix.strip().lower().replace("-", "_")


def assert_scope_suffix_deployable(suffix: str) -> None:
    """Raise ``ValueError`` if ``suffix`` is reserved or empty."""
    raw = suffix.strip()
    if not raw:
        raise ValueError("scope_suffix must be non-empty")
    if _suffix_normalization_key(raw) in _RESERVED_SCOPE_SUFFIX_KEYS:
        raise ValueError(
            f"scope_suffix {raw!r} is reserved (local/template workflow documents are not deployable). "
            "Use a scoped leaf under workflows/<suffix>/ from the hierarchy build."
        )


_TRIGGER_NAME_RE = re.compile(
    r"^.+\.WorkflowTrigger\.ya?ml$",
    re.IGNORECASE,
)


def parse_scope_suffix_from_workflow_trigger_rel(rel: str) -> str:
    """``workflows/<suffix>/…WorkflowTrigger.yaml`` → ``suffix``. Raises ``ValueError``."""
    parts = rel.strip().replace("\\", "/").split("/")
    parts = [p for p in parts if p]
    if len(parts) < 3 or parts[0] != "workflows":
        raise ValueError(
            "workflow_trigger_rel must be a module-relative path workflows/<suffix>/…WorkflowTrigger.yaml"
        )
    if not _TRIGGER_NAME_RE.match(parts[-1]):
        raise ValueError("workflow_trigger_rel must end with .WorkflowTrigger.yaml or .WorkflowTrigger.yml")
    return parts[1]


def assert_workflow_trigger_rel_matches_suffix(rel: str, scope_suffix: str) -> None:
    """Raises ``ValueError`` if ``rel`` does not point under ``workflows/<scope_suffix>/``."""
    parsed = parse_scope_suffix_from_workflow_trigger_rel(rel)
    if parsed != scope_suffix.strip():
        raise ValueError(
            f"workflow_trigger_rel folder {parsed!r} does not match scope_suffix {scope_suffix.strip()!r}"
        )


def assert_trigger_path_under_module(module_root: Path, rel: str) -> Path:
    """Resolve ``rel`` under ``module_root``; raise ``ValueError`` if escape or wrong prefix."""
    if ".." in rel.split("/") or rel.startswith(("/", "\\")):
        raise ValueError("Invalid workflow_trigger_rel path")
    p = (module_root / rel).resolve()
    try:
        p.relative_to(module_root.resolve())
    except ValueError as e:
        raise ValueError("workflow_trigger_rel escapes module root") from e
    parts = p.relative_to(module_root).parts
    if len(parts) < 3 or parts[0] != "workflows":
        raise ValueError("workflow_trigger_rel must live under workflows/")
    return p
