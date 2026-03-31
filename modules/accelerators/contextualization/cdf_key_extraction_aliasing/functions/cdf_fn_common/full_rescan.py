"""Resolve effective ``full_rescan`` from parameters and optional function ``data``."""

from __future__ import annotations

from typing import Any, Dict, Optional


def resolve_full_rescan(parameters: Any, data: Optional[Dict[str, Any]] = None) -> bool:
    """
    Run-level ``data["full_rescan"]`` overrides ``parameters.full_rescan`` when the key is present.

    ``parameters`` may be a Pydantic model or any object with a ``full_rescan`` attribute.
    """
    base = bool(getattr(parameters, "full_rescan", False))
    if data is not None and "full_rescan" in data:
        return bool(data["full_rescan"])
    return base
