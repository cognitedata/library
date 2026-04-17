"""Resolve effective ``run_all`` from parameters and optional function ``data``."""

from __future__ import annotations

from typing import Any, Dict, Optional


def resolve_run_all(parameters: Any, data: Optional[Dict[str, Any]] = None) -> bool:
    """
    Run-level ``data["run_all"]`` overrides ``parameters.run_all`` when the key is present.

    ``parameters`` may be a Pydantic model or any object with a ``run_all`` attribute.
    """
    base = bool(getattr(parameters, "run_all", False))
    if data is not None and "run_all" in data:
        return bool(data["run_all"])
    return base
