"""Guardrails for user-facing pipeline terminology in local_runner/report.py."""

from __future__ import annotations

import sys
from pathlib import Path

_MOD = Path(__file__).resolve().parents[3]
if str(_MOD) not in sys.path:
    sys.path.insert(0, str(_MOD))

from local_runner import (  # noqa: E402
    report as report_mod,
)


def test_workflow_overview_mentions_discovery_stages() -> None:
    src = Path(report_mod.__file__).read_text(encoding="utf-8")
    assert "Discovery" in src or "discovery" in src
    assert "workflow_diagram.png" not in src
    assert "workflow_diagram.md" in src
