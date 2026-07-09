"""Incremental watermark filter helpers for instances.query (Phase 2)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional


def build_watermark_range_filter(
    property_path: list[str],
    *,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> Any:
    """Return a DM Range filter on *property_path* when bounds are set."""
    from cognite.client import data_modeling as dm

    if since is None and until is None:
        return None
    if since is not None and until is not None:
        return dm.filters.Range(property_path, gte=since.isoformat(), lt=until.isoformat())
    if since is not None:
        return dm.filters.Range(property_path, gte=since.isoformat())
    return dm.filters.Range(property_path, lt=until.isoformat())
