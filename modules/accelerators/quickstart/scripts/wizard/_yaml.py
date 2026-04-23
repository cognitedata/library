"""
YAML utility functions for the Quickstart DP setup wizard.

Uses a lightweight line-based parser — no external YAML library required —
to locate, read, and mutate individual fields while preserving comments,
indentation, and surrounding content.
"""
from __future__ import annotations

import re
from collections.abc import Sequence

from ._constants import _YAML_LINE_RE


# Path building

def yaml_key_match(line: str) -> tuple[int, str] | None:
    """Return (indent_level, key) for a YAML key line, or None."""
    m = re.match(r"^(\s*)([A-Za-z0-9_]+):", line)
    if not m:
        return None
    return len(m.group(1)), m.group(2)


def build_yaml_paths(lines: Sequence[str]) -> dict[tuple[str, ...], int]:
    """Build a mapping of dotted-key-path → line-index for the given YAML lines."""
    key_line_map: dict[tuple[str, ...], int] = {}
    stack: list[tuple[int, str]] = []
    for idx, line in enumerate(lines):
        parsed = yaml_key_match(line)
        if not parsed:
            continue
        indent, key = parsed
        while stack and indent <= stack[-1][0]:
            stack.pop()
        current_path = tuple(k for _, k in stack) + (key,)
        key_line_map[current_path] = idx
        stack.append((indent, key))
    return key_line_map


# Value read / write

def get_yaml_current_value(
    lines: Sequence[str],
    path: tuple[str, ...],
    key_line_map: dict[tuple[str, ...], int],
) -> str | None:
    """Return the raw current value for a YAML path, or None if absent/empty."""
    idx = key_line_map.get(path)
    if idx is None:
        return None
    m = _YAML_LINE_RE.match(lines[idx].rstrip("\n"))
    if not m:
        return None
    val = m.group(2).strip()
    return val if val else None


def set_yaml_line_value(line: str, value: str) -> str:
    """Replace the value on a single YAML line, preserving any trailing comment."""
    m = _YAML_LINE_RE.match(line.rstrip("\n"))
    if not m:
        raise ValueError(f"Cannot set value for YAML line: {line!r}")
    comment_part = m.group(3) or ""
    if comment_part.strip().startswith("#"):
        comment_part = f" {comment_part.strip()}"
    return f"{m.group(1)}{value}{comment_part}\n"


def _extract_yaml_value(line: str) -> str:
    """Extract the value portion from a YAML key: value line, or return stripped line."""
    m = _YAML_LINE_RE.match(line.rstrip("\n"))
    return m.group(2).strip() if m else line.strip()


def _strip_yaml_quotes(value: str) -> str:
    """Remove a single layer of surrounding YAML quotes (single or double) from a value."""
    for q in ('"', "'"):
        if len(value) >= 2 and value.startswith(q) and value.endswith(q):
            return value[1:-1]
    return value


def quote_yaml_string(value: str) -> str:
    """Wrap a plain string in double quotes, escaping backslashes and inner quotes."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def set_yaml_value_by_path(
    lines: list[str],
    path: tuple[str, ...],
    value: str,
    key_line_map: dict[tuple[str, ...], int] | None = None,
) -> tuple[str, str] | None:
    """
    Set a YAML value by dotted path.

    Returns (old_value, new_value) if the path was found, None otherwise.
    old_value / new_value use ``"<not set>"`` when the field was empty.
    """
    if key_line_map is None:
        key_line_map = build_yaml_paths(lines)
    idx = key_line_map.get(path)
    if idx is None:
        return None
    old_line = lines[idx]
    new_line = set_yaml_line_value(old_line, value)
    lines[idx] = new_line
    old_val = _extract_yaml_value(old_line) or "<not set>"
    new_val = _extract_yaml_value(new_line) or "<not set>"
    return old_val, new_val


def set_target_view_filter_values(
    lines: list[str],
    desired_value: str,
    key_line_map: dict[tuple[str, ...], int] | None = None,
) -> tuple[str, str] | None:
    """
    Update the first list item under targetViewFilterValues.

    Returns (old_value, new_value) if found, None otherwise.
    """
    if key_line_map is None:
        key_line_map = build_yaml_paths(lines)
    base_path = (
        "variables", "modules", "accelerators", "contextualization",
        "cdf_entity_matching", "targetViewFilterValues",
    )
    idx = key_line_map.get(base_path)
    if idx is None:
        return None
    if idx + 1 >= len(lines):
        return None
    list_line = lines[idx + 1]
    list_match = re.match(r"^(\s*)-\s*(.*)\s*$", list_line)
    if not list_match:
        return None
    indent = list_match.group(1)
    old_val = list_match.group(2).strip() or "<not set>"
    lines[idx + 1] = f"{indent}- {desired_value}\n"
    return old_val, desired_value

