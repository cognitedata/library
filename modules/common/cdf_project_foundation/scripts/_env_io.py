"""Read and write helpers for ``.env`` files used by the setup wizard."""

from __future__ import annotations

from pathlib import Path


def parse_env_file(path: Path) -> tuple[list[str], dict[str, str], dict[str, int]]:
    """Parse a .env file.

    Returns:
        lines      — raw file lines (mutable; pass to upsert_env / write back)
        values     — mapping of key → unquoted value for every non-comment entry
        key_idx    — mapping of key → line index (for in-place updates)
    """
    lines: list[str] = []
    values: dict[str, str] = {}
    key_idx: dict[str, int] = {}
    if path.exists():
        lines = path.read_text().splitlines(keepends=True)
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                k, _, v = stripped.partition("=")
                values[k.strip()] = v.strip().strip('"').strip("'")
                key_idx[k.strip()] = i
    return lines, values, key_idx


def upsert_env(
    lines: list[str],
    key_idx: dict[str, int],
    key: str,
    value: str,
) -> None:
    """Insert or replace a KEY="value" entry in *lines*, updating *key_idx* in place."""
    entry = f'{key}={value}\n'
    if key in key_idx:
        lines[key_idx[key]] = entry
    else:
        # Ensure a blank separator before a new variable block.
        if lines and lines[-1].strip():
            lines.append("\n")
        key_idx[key] = len(lines)
        lines.append(entry)
