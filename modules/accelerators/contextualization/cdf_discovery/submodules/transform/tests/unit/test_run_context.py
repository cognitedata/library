"""Tests for local run context helpers."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from local_runner.run_context import apply_incremental_run_scope


def test_apply_incremental_run_scope_enables_processing() -> None:
    doc: dict = {"parameters": {"incremental": False, "incremental_change_processing": False}}
    apply_incremental_run_scope(doc, incremental_change_processing=True)
    assert doc["parameters"]["incremental_change_processing"] is True
    assert doc["parameters"]["incremental_skip_unchanged"] is True


def test_apply_incremental_run_scope_disables_processing_for_full_scope() -> None:
    doc: dict = {"parameters": {"incremental_change_processing": True, "incremental": True}}
    apply_incremental_run_scope(doc, incremental_change_processing=False)
    assert doc["parameters"]["incremental_change_processing"] is False
    assert doc["parameters"]["incremental"] is False
