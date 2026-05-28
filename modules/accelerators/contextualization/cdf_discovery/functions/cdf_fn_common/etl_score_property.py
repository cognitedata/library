"""Parallel score property names for discovery cohort value lists."""

from __future__ import annotations


def score_property_key(value_field: str) -> str:
    """Return ``{value_field}_score`` (e.g. ``aliases`` → ``aliases_score``)."""
    vf = str(value_field or "aliases").strip() or "aliases"
    return f"{vf}_score"
