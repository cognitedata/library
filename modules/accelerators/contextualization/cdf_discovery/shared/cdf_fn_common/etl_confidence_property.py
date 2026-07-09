"""Parallel score property names for discovery cohort value lists."""

from __future__ import annotations


def confidence_property_key(value_field: str) -> str:
    """Return ``{value_field}_confidence`` (e.g. ``aliases`` → ``aliases_confidence``)."""
    vf = str(value_field or "aliases").strip() or "aliases"
    return f"{vf}_confidence"
