"""Ensure canonical documentation files exist."""

from __future__ import annotations

from pathlib import Path

import pytest


def _module_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "relative",
    [
        "docs/README.md",
        "docs/MODULE_SPECIFICATION.md",
        "docs/guides/howto_operator_ui.md",
        "README.md",
    ],
)
def test_canonical_docs_exist(relative: str) -> None:
    assert (_module_root() / relative).is_file()


def test_readme_documents_ui_launch() -> None:
    text = (_module_root() / "README.md").read_text(encoding="utf-8")
    assert "module.py ui" in text
