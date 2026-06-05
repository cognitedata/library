"""
Line-preserving YAML patcher for the setup wizard.

Updates existing scalar values and inline lists in a YAML file,
preserving comments, blank lines, and unrelated indentation.

Only updates or deletes existing keys—does not add new ones.
"""

from __future__ import annotations

import re

# ── Internal helpers ───────────────────────────────────────────────────────────

def _remove_block_items(lines: list[str], start: int, parent_indent: int) -> bool:
    """Remove consecutive block-sequence lines (``- …``) after *start* that are
    indented deeper than *parent_indent*.  Returns ``True`` if anything removed."""
    to_remove: list[int] = []
    j = start
    while j < len(lines):
        stripped = lines[j].strip()
        if not stripped or stripped.startswith("#"):
            j += 1
            continue
        item_indent = len(lines[j]) - len(lines[j].lstrip())
        if stripped.startswith("-") and item_indent >= parent_indent:
            to_remove.append(j)
            j += 1
        else:
            break
    for k in reversed(to_remove):
        lines.pop(k)
    return bool(to_remove)


# ── Public API ─────────────────────────────────────────────────────────────────

def find_line(lines: list[str], dotted_path: str) -> int | None:
    """Return the index of the line that owns the leaf key, or ``None``."""
    parts = dotted_path.split(".")
    depth = 0
    section_indent: list[int] = [-1]  # stack of indents for each open section

    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(stripped)
        key_match = re.match(r'^([A-Za-z0-9_\-\.]+)\s*:', stripped)
        if not key_match:
            continue
        key = key_match.group(1)

        # Pop sections we have exited (current indent ≤ section opener's indent).
        while indent <= section_indent[-1] and depth > 0:
            section_indent.pop()
            depth -= 1

        if depth < len(parts) - 1:
            if key == parts[depth] and indent > section_indent[-1]:
                section_indent.append(indent)
                depth += 1
        elif depth == len(parts) - 1:
            if key == parts[depth] and indent > section_indent[-1]:
                return i

    return None


def get_value(lines: list[str], dotted_path: str) -> str | None:
    """Return the unquoted scalar value at *dotted_path*, or ``None`` if not found."""
    idx = find_line(lines, dotted_path)
    if idx is None:
        return None
    m = re.match(r'^(\s*[^:]+:\s*)(.*?)(\s*#.*)?$', lines[idx].rstrip())
    if m:
        return m.group(2).strip().strip('"').strip("'") or None
    return None


def set_value(lines: list[str], dotted_path: str, new_val: str) -> tuple[str | None, bool]:
    """Replace the scalar (or inline-list) value at *dotted_path* in-place.

    When the existing key owns a block-sequence below it (lines starting with
    ``-`` at deeper indentation), those lines are removed and *new_val* is
    written inline on the key line.

    Returns:
        (old_value_str, changed) — *changed* is ``False`` when the value was
        already equal to *new_val* and no block items were present.
    """
    idx = find_line(lines, dotted_path)
    if idx is None:
        return None, False

    line = lines[idx]
    m = re.match(r'^(\s*[^:]+:\s*)(.*?)(\s*(#.*)?)$', line.rstrip())
    if not m:
        return None, False

    prefix   = m.group(1).rstrip() + " "  # ensure exactly one space after the colon
    old_raw  = m.group(2).strip()
    comment  = m.group(4) or ""
    old_val  = old_raw.strip('"').strip("'")

    key_indent = len(line) - len(line.lstrip())

    # Remove any block-sequence items owned by this key.
    block_removed = _remove_block_items(lines, idx + 1, key_indent)

    suffix = f"  {comment}" if comment else ""
    lines[idx] = f"{prefix}{new_val}{suffix}\n"

    changed = block_removed or (old_val != new_val.strip('"').strip("'"))
    return old_val or None, changed


def delete_key(lines: list[str], dotted_path: str) -> bool:
    """Remove the line at *dotted_path* and any owned block-sequence items.

    Returns ``True`` if the key line was found and removed.
    """
    idx = find_line(lines, dotted_path)
    if idx is None:
        return False
    key_indent = len(lines[idx]) - len(lines[idx].lstrip())
    _remove_block_items(lines, idx + 1, key_indent)
    lines.pop(idx)
    return True
