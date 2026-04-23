"""
SQL transformation helper for the Quickstart DP setup wizard.

Handles switching ``asset.Transformation.sql`` from COMMON MODE to
FILE_ANNOTATION MODE by commenting out the COMMON block and uncommenting
the FILE_ANNOTATION block.
"""
from __future__ import annotations

from pathlib import Path

from ._constants import (
    SQL_COMMON_BLOCK_ANCHOR,
    SQL_COMMON_MODE_MARKER,
    SQL_FILE_ANNOTATION_BLOCK_ANCHOR,
    SQL_FILE_ANNOTATION_MODE_MARKER,
)
from ._file_io import ensure_backup, read_lines, write_lines


def enable_file_annotation_mode(sql_path: Path, skip_backup: bool = False) -> bool:
    """Switch asset.Transformation.sql to FILE_ANNOTATION mode.

    Returns ``True`` if the file was changed, ``False`` if it was already in
    FILE_ANNOTATION mode (idempotent).

    Pass ``skip_backup=True`` when the caller has already created a backup
    (e.g. the wizard takes all backups upfront before any writes).

    Raises ``RuntimeError`` if the expected mode markers or SQL blocks cannot
    be found in the file.
    """
    lines = read_lines(sql_path)
    updated = False

    common_marker_idx = next(
        (i for i, t in enumerate(lines) if SQL_COMMON_MODE_MARKER in t), None
    )
    file_marker_idx = next(
        (i for i, t in enumerate(lines) if SQL_FILE_ANNOTATION_MODE_MARKER in t), None
    )
    if (
        common_marker_idx is None
        or file_marker_idx is None
        or common_marker_idx >= file_marker_idx
    ):
        raise RuntimeError("Could not find expected mode markers in asset.Transformation.sql")

    common_sql_start = next(
        (
            i for i in range(common_marker_idx, file_marker_idx)
            if SQL_COMMON_BLOCK_ANCHOR in lines[i]
        ),
        None,
    )
    if common_sql_start is None:
        raise RuntimeError("Could not find COMMON MODE SQL block in asset.Transformation.sql")

    # Comment out every active (non-comment) line in the COMMON block.
    for i in range(common_sql_start, file_marker_idx):
        stripped = lines[i].strip()
        if not stripped or stripped.startswith("--"):
            continue
        indent = len(lines[i]) - len(lines[i].lstrip(" "))
        lines[i] = (" " * indent) + "-- " + lines[i].lstrip(" ")
        updated = True

    file_sql_start = next(
        (
            i for i in range(file_marker_idx, len(lines))
            if SQL_FILE_ANNOTATION_BLOCK_ANCHOR in lines[i]
        ),
        None,
    )
    if file_sql_start is None:
        raise RuntimeError(
            "Could not find FILE_ANNOTATION MODE SQL block in asset.Transformation.sql"
        )

    # Uncomment the FILE_ANNOTATION block if it is still commented out.
    file_block_commented = lines[file_sql_start].lstrip().startswith("--")
    if file_block_commented:
        for i in range(file_sql_start, len(lines)):
            line = lines[i]
            stripped = line.lstrip()
            if stripped.startswith("-- "):
                prefix_len = len(line) - len(stripped)
                lines[i] = (" " * prefix_len) + stripped[3:]
                updated = True
            elif stripped.startswith("--"):
                prefix_len = len(line) - len(stripped)
                lines[i] = (" " * prefix_len) + stripped[2:]
                updated = True

    if updated:
        if not skip_backup:
            ensure_backup(sql_path)
        write_lines(sql_path, lines)
    return updated
