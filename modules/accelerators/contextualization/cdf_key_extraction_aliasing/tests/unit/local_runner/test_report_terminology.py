"""Guardrails for user-facing pipeline terminology in local_runner/report.py."""

from __future__ import annotations

from pathlib import Path

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.local_runner import (
    report as report_mod,
)


def test_workflow_overview_step_uses_reference_index_not_catalog() -> None:
    src = Path(report_mod.__file__).read_text(encoding="utf-8")
    assert "5. **Reference Index**" in src
    assert "Reference Catalog" not in src
    assert "fn_dm_reference_index" in src
    assert "workflow_diagram.png" not in src
    assert "workflow_diagram.md" in src
    assert "future implementation" not in src
