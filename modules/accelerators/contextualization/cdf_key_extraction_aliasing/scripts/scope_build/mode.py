"""scope_build_mode from default.config.yaml (main.py --build / build_scopes.py)."""

from __future__ import annotations

from typing import Literal

ScopeBuildMode = Literal["trigger_only", "full"]

_TRIGGER_ONLY: ScopeBuildMode = "trigger_only"
_FULL: ScopeBuildMode = "full"


def scope_build_mode_from_doc(doc: dict) -> ScopeBuildMode:
    """Return ``trigger_only`` (default) or ``full`` from top-level ``scope_build_mode``."""
    raw = doc.get("scope_build_mode", _TRIGGER_ONLY)
    if raw is None or str(raw).strip() == "":
        return _TRIGGER_ONLY
    s = str(raw).strip().lower().replace("-", "_")
    if s in ("trigger_only", "triggeronly"):
        return _TRIGGER_ONLY
    if s == "full":
        return _FULL
    raise ValueError(
        "scope_build_mode must be 'trigger_only' or 'full' "
        f"(got {raw!r}); see default.config.yaml"
    )


def scoped_workflow_external_id(workflow_base: str, suffix: str) -> str:
    """CDF workflow external id for one leaf in ``full`` mode."""
    return f"{workflow_base}.{suffix}"
