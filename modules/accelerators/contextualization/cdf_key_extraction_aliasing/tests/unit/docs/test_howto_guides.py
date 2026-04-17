"""Ensure operational how-to guides stay present and minimally complete."""

from __future__ import annotations

from pathlib import Path

import pytest


def _module_root() -> Path:
    # tests/unit/docs/test_howto_guides.py -> module root (cdf_key_extraction_aliasing/)
    return Path(__file__).resolve().parents[3]


@pytest.mark.parametrize(
    ("name", "needles"),
    [
        (
            "howto_quickstart.md",
            [
                "# Quickstart",
                "## Prerequisites",
                ".env",
                "PYTHONPATH",
                "## Run `module.py run`",
                "tests/results/",
            ],
        ),
        (
            "howto_scoped_deployment.md",
            [
                "# Scoped deployment",
                "aliasing_scope_hierarchy",
                "## 3. Build commands",
                "WorkflowTrigger",
                "input.configuration",
                "cdf deploy",
            ],
        ),
        (
            "howto_config_yaml.md",
            [
                "# How to build configuration with YAML",
                "## 1. Know the three layers",
                "workflow.local.config.yaml",
                "module.py build",
                "module.py run",
            ],
        ),
        (
            "howto_config_ui.md",
            [
                "# How to build configuration with the UI",
                "## 2. Start the API and the frontend",
                "module.py ui",
                "CDF_KEY_EXTRACTION_ALIASING_ROOT",
                "POST /api/run",
                "run_all",
            ],
        ),
    ],
)
def test_howto_guide_exists_with_expected_sections(name: str, needles: list[str]) -> None:
    path = _module_root() / "docs" / "guides" / name
    assert path.is_file(), f"Missing guide: {path}"
    text = path.read_text(encoding="utf-8")
    for fragment in needles:
        assert fragment in text, f"{name} should mention {fragment!r}"


def test_howto_custom_handlers_related_links_quickstart_and_scoped() -> None:
    path = _module_root() / "docs" / "guides" / "howto_custom_handlers.md"
    text = path.read_text(encoding="utf-8")
    assert "howto_quickstart.md" in text
    assert "howto_scoped_deployment.md" in text
