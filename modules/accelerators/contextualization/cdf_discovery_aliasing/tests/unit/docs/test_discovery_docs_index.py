"""Ensure canonical documentation files exist."""

from __future__ import annotations

from pathlib import Path

import pytest


def _module_root() -> Path:
    return Path(__file__).resolve().parents[3]


@pytest.mark.parametrize(
    "relative",
    [
        "docs/README.md",
        "docs/MODULE_SPECIFICATION.md",
        "README.md",
    ],
)
def test_canonical_docs_exist(relative: str) -> None:
    path = _module_root() / relative
    assert path.is_file(), f"Missing: {path}"


def test_readme_documents_ui_launch() -> None:
    text = (_module_root() / "README.md").read_text(encoding="utf-8")
    assert "module.py ui" in text
    assert "## Operator UI" in text
