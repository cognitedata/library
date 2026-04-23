"""Shared pytest fixtures for the QS DP setup wizard tests."""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest

# Import SQL marker constants so the generated fixture always stays in sync
# with the production code — no static SQL file needed.
from wizard._constants import (
    SQL_COMMON_BLOCK_ANCHOR,
    SQL_COMMON_MODE_MARKER,
    SQL_FILE_ANNOTATION_BLOCK_ANCHOR,
    SQL_FILE_ANNOTATION_MODE_MARKER,
)

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "qs_dp"

# Minimal SQL content that satisfies enable_file_annotation_mode():
# - contains both mode markers
# - contains the COMMON block anchor (active, uncommented)
# - contains the FILE_ANNOTATION block anchor (inactive, commented out)
_SQL_CONTENT = (
    f"-- {SQL_COMMON_MODE_MARKER}\n"
    f"{SQL_COMMON_BLOCK_ANCHOR}\n"
    f"    SELECT 1\n"
    f");\n"
    f"\n"
    f"-- {SQL_FILE_ANNOTATION_MODE_MARKER}\n"
    f"-- {SQL_FILE_ANNOTATION_BLOCK_ANCHOR}\n"
    f"--     SELECT 1\n"
    f"-- );\n"
)


@pytest.fixture()
def fixture_root() -> Path:
    """Path to the canonical fixture repo (read-only — tests that write use tmp_fixture_root)."""
    return FIXTURE_ROOT


@pytest.fixture()
def tmp_fixture_root(tmp_path: Path) -> Path:
    """
    A writable copy of the fixture repo in a pytest tmp_path.
    Tests that call main() or mutate files should use this.
    """
    dest = tmp_path / "qs_dp"
    shutil.copytree(FIXTURE_ROOT, dest)
    return dest


@pytest.fixture()
def fixture_config_lines() -> list[str]:
    return (FIXTURE_ROOT / "config.dev.yaml").read_text(encoding="utf-8").splitlines(keepends=True)


@pytest.fixture()
def tmp_sql_path(tmp_path: Path) -> Path:
    """Writable SQL file generated from the production marker constants."""
    path = tmp_path / "asset.Transformation.sql"
    path.write_text(_SQL_CONTENT, encoding="utf-8")
    return path
