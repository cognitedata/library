"""
Dot-path reads on view property bags (nested dicts and JSON string leaves).

Used by key extraction when ``source_fields[].field_name`` uses dotted paths
to reach nested DM properties or JSON serialized in a string field.
"""

from __future__ import annotations

import json
from typing import Any, Optional


def get_value_by_property_path(
    root: Any,
    path: str,
    *,
    parse_json_strings: bool = True,
) -> Optional[Any]:
    """
    Resolve ``path`` (dot-separated segments) against ``root``.

    Each ``.`` starts a new segment; there is no escape for literal dots in
    property names (use a single top-level key without ``.`` in the path).

    - Walks ``dict`` keys in order.
    - If the current value is a ``str`` and more segments remain, tries
      ``json.loads`` (after strip) and continues on the parsed value so a
      property that stores JSON can be traversed (e.g. ``meta.tag`` when
      ``meta`` is ``'{"tag":"P-1"}'``).

    Returns ``None`` if any segment is missing or JSON parsing fails.
    """
    if not path or not isinstance(path, str):
        return None
    parts = path.split(".")
    value: Any = root
    i = 0
    while i < len(parts):
        part = parts[i]
        if isinstance(value, dict) and part in value:
            value = value[part]
            i += 1
            continue
        if parse_json_strings and isinstance(value, str) and i < len(parts):
            s = value.strip()
            if not s:
                return None
            try:
                value = json.loads(s)
            except (json.JSONDecodeError, TypeError, ValueError):
                return None
            continue
        return None
    return value
