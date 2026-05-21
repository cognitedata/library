"""Normalize ``source_view_index`` from scope / canvas refs."""

from __future__ import annotations

from typing import Any, Optional


def coerce_association_source_view_index(idx: Any) -> Optional[int]:
    if idx is None or isinstance(idx, bool):
        return None
    if isinstance(idx, int):
        return int(idx)
    if isinstance(idx, float) and float(idx).is_integer():
        return int(idx)
    if isinstance(idx, str):
        s = idx.strip()
        if not s:
            return None
        try:
            v = float(s)
        except (TypeError, ValueError):
            return None
        if v.is_integer():
            return int(v)
    return None
