"""Dot-path reads on nested dict/list structures (cohort ``properties``, JSON blobs)."""

from __future__ import annotations

from typing import Any


def get_property_path(obj: Any, path: str) -> Any:
    """
    Resolve ``path`` like ``tags.0`` or ``a.b`` on dict/list trees.

    Mirrors the path walker used by transform ``parse_json_extract`` handlers.
    """
    cur = obj
    normalized = str(path or "").strip().lstrip("$").lstrip(".")
    if not normalized:
        return cur
    for part in normalized.split("."):
        if not part:
            continue
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list) and part.isdigit():
            idx = int(part)
            cur = cur[idx] if 0 <= idx < len(cur) else None
        else:
            return None
    return cur


def get_value_by_property_path(entity_props: Any, field_name: str) -> Any:
    """Resolve *field_name* on *entity_props* (alias for :func:`get_property_path`)."""
    return get_property_path(entity_props, field_name)
