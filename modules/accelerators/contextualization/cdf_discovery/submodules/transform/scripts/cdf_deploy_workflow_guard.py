"""Rules for deployable workflow artifacts under ``workflows/`` (flat or ``workflows/<suffix>/``)."""

from __future__ import annotations

import re
from pathlib import Path

_RESERVED_SCOPE_SUFFIX_KEYS = frozenset(
    {
        "local",
        "template",
        "workflow_local",
        "workflow_template",
        "workflowlocal",
        "workflowtemplate",
        "all",
    }
)

_TRIGGER_NAME_RE = re.compile(
    r"^.+\.WorkflowTrigger\.ya?ml$",
    re.IGNORECASE,
)


def _suffix_normalization_key(suffix: str) -> str:
    return suffix.strip().lower().replace("-", "_")


def assert_scope_suffix_deployable(suffix: str) -> None:
    raw = suffix.strip()
    if not raw:
        raise ValueError("scope_suffix must be non-empty when using a workflows/<suffix>/ folder")
    if _suffix_normalization_key(raw) in _RESERVED_SCOPE_SUFFIX_KEYS:
        raise ValueError(
            f"scope_suffix {raw!r} is reserved. Use a built scope folder under workflows/<suffix>/."
        )


def workflows_root_from_trigger(trigger_path: Path) -> Path:
    p = trigger_path.resolve()
    for ancestor in (p.parent, p.parent.parent):
        if ancestor.name == "workflows":
            return ancestor
    raise ValueError(f"WorkflowTrigger must live under workflows/: {trigger_path}")


def scope_suffix_from_trigger(trigger_path: Path) -> str:
    """``''`` for flat ``workflows/etl_*.WorkflowTrigger.yaml``; else the single subfolder name."""
    wroot = workflows_root_from_trigger(trigger_path)
    rel = trigger_path.parent.resolve().relative_to(wroot.resolve())
    if rel.parts == ():
        return ""
    if len(rel.parts) == 1:
        suffix = rel.parts[0]
        assert_scope_suffix_deployable(suffix)
        return suffix
    raise ValueError(
        f"Unsupported workflows layout (expected workflows/<file> or workflows/<suffix>/<file>): {trigger_path}"
    )


def assert_trigger_path_under_module(module_root: Path, rel: str) -> Path:
    if ".." in rel.split("/") or rel.startswith(("/", "\\")):
        raise ValueError("Invalid workflow_trigger_rel path")
    p = (module_root / rel).resolve()
    try:
        p.relative_to(module_root.resolve())
    except ValueError as e:
        raise ValueError("workflow_trigger_rel escapes module root") from e
    parts = p.relative_to(module_root).parts
    if len(parts) < 2 or parts[0] != "workflows":
        raise ValueError("workflow_trigger_rel must live under workflows/")
    if not _TRIGGER_NAME_RE.match(parts[-1]):
        raise ValueError("workflow_trigger_rel must end with .WorkflowTrigger.yaml")
    return p
