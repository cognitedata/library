"""Cohort RAW handoff emits canvas task_progress every 10,000 rows."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common import etl_ui_progress  # noqa: E402
from cdf_fn_common.etl_cohort_handoff import write_entity_rows_to_cohort_sink  # noqa: E402


def test_write_entity_rows_emits_progress_every_10k(monkeypatch) -> None:
    emitted: list[int] = []

    captured: list[tuple[int, int | None]] = []

    def capture(current: int, *, total=None, label=None, force: bool = False) -> None:
        captured.append((current, total))

    monkeypatch.setattr(etl_ui_progress, "emit_handler_progress", capture)
    monkeypatch.setattr(
        "cdf_fn_common.etl_discovery_query_shared.create_table_if_not_exists",
        lambda *a, **k: None,
    )

    client = MagicMock()
    rows = [
        {
            "columns": {"node_instance_id": f"sp:{i}", "external_id": f"E{i}"},
            "properties": {"name": f"n{i}"},
        }
        for i in range(501)
    ]
    data: dict = {"task_id": "q1", "run_id": "run-1", "config": {}}
    etl_ui_progress.bind_handler_progress(data)
    summary = write_entity_rows_to_cohort_sink(
        client,
        data,
        run_id="run-1",
        scope_key="default",
        task_id="q1",
        query_source="view",
        entity_type="Asset",
        view_space="cdf_cdm",
        view_external_id="Asset",
        view_version="v1",
        rows=rows,
    )
    etl_ui_progress.clear_handler_progress()

    assert summary["rows_written"] == 501
    assert captured == [(501, 501)]


def test_write_entity_rows_uses_configured_write_batch_size(monkeypatch) -> None:
    captured: list[tuple[int, int | None]] = []

    def capture(current: int, *, total=None, label=None, force: bool = False) -> None:
        captured.append((current, total))

    monkeypatch.setattr(etl_ui_progress, "emit_handler_progress", capture)
    monkeypatch.setattr(
        "cdf_fn_common.etl_discovery_query_shared.create_table_if_not_exists",
        lambda *a, **k: None,
    )

    client = MagicMock()
    rows = [
        {
            "columns": {"node_instance_id": f"sp:{i}", "external_id": f"E{i}"},
            "properties": {"name": f"n{i}"},
        }
        for i in range(5)
    ]
    data: dict = {
        "task_id": "q1",
        "run_id": "run-1",
        "config": {},
        "configuration": {"parameters": {"cohort_write_batch_size": 2}},
    }
    etl_ui_progress.bind_handler_progress(data)
    summary = write_entity_rows_to_cohort_sink(
        client,
        data,
        run_id="run-1",
        scope_key="default",
        task_id="q1",
        query_source="view",
        entity_type="Asset",
        view_space="cdf_cdm",
        view_external_id="Asset",
        view_version="v1",
        rows=rows,
    )
    etl_ui_progress.clear_handler_progress()

    assert summary["rows_written"] == 5
    assert captured == [(2, 5), (4, 5), (5, 5)]
