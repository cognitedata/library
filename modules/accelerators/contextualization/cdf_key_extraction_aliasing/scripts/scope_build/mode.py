"""Workflow layout is always scoped (``workflows/<suffix>/`` per hierarchy leaf)."""

from __future__ import annotations

from typing import Literal

ScopeBuildMode = Literal["full"]


def scope_build_mode_from_doc(doc: dict) -> ScopeBuildMode:
    """Return ``full``. Reject removed ``trigger_only`` / unknown values."""
    raw = doc.get("scope_build_mode")
    if raw is None or str(raw).strip() == "":
        return "full"
    s = str(raw).strip().lower().replace("-", "_")
    if s in ("trigger_only", "triggeronly"):
        raise ValueError(
            "scope_build_mode 'trigger_only' is no longer supported; remove the key "
            "(scoped workflows under workflows/<suffix>/ are the only layout)."
        )
    if s == "full":
        return "full"
    raise ValueError(
        f"scope_build_mode must be 'full' or omitted (got {raw!r}); see default.config.yaml"
    )


def scoped_workflow_external_id(workflow_base: str, suffix: str) -> str:
    """CDF workflow external id for one leaf."""
    return f"{workflow_base}.{suffix}"
