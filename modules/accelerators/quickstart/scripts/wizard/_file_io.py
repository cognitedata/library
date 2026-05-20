"""
File I/O helpers for the Quickstart DP setup wizard.

Covers timestamped backups, line-based file reads/writes, and .env file
parsing / mutation.
"""
from __future__ import annotations

import datetime
import shutil
from collections.abc import Sequence
from pathlib import Path


def ensure_backup(path: Path) -> Path:
    """Create a timestamped backup of *path* on every call.  Returns the backup path.

    For dotfiles such as `.env` (which have no conventional extension and may be
    auto-discovered by AI tooling if they keep the leading dot), the backup is
    written as ``qs_backup_<timestamp>.env`` in the same directory so that it is
    not mistaken for an active secrets file.

    All other files receive the standard ``<name>.<ext>.bak.<timestamp>`` suffix.
    """
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    if path.name.startswith(".") and path.suffix == "":
        # Dotfile with no extension, e.g. ".env"  →  qs_backup_<ts>.env
        stem = path.name.lstrip(".")          # "env"
        backup_path = path.parent / f"qs_backup_{ts}.{stem}"
    else:
        backup_path = path.with_suffix(path.suffix + f".bak.{ts}")
    shutil.copy2(path, backup_path)
    return backup_path


def read_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines(keepends=True)


def write_lines(path: Path, lines: Sequence[str]) -> None:
    path.write_text("".join(lines), encoding="utf-8")


def parse_env_file(path: Path) -> tuple[list[str], dict[str, str], dict[str, int]]:
    """Parse a .env file into (lines, values_dict, key_to_line_index).

    Returns empty structures when the file does not exist.
    """
    if not path.exists():
        return [], {}, {}
    lines = read_lines(path)
    values: dict[str, str] = {}
    key_to_line: dict[str, int] = {}
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        values[key] = value.rstrip("\n")
        key_to_line[key] = idx
    return lines, values, key_to_line


def upsert_env_var(
    lines: list[str],
    key_to_line: dict[str, int],
    key: str,
    value: str,
) -> None:
    """Update an existing key or append a new ``KEY=value`` line."""
    new_line = f"{key}={value}\n"
    if key in key_to_line:
        lines[key_to_line[key]] = new_line
    else:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] = lines[-1] + "\n"
        lines.append(new_line)
        key_to_line[key] = len(lines) - 1
