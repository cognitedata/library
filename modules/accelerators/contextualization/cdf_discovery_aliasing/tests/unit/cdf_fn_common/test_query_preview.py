"""Unit tests for discovery query preview helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cdf_fn_common.query_preview import (
    run_classic_query_preview,
    run_raw_query_preview,
    run_view_query_preview,
)


def test_run_view_query_preview_requires_view_external_id() -> None:
    with pytest.raises(ValueError, match="view_external_id"):
        run_view_query_preview(MagicMock(), {})


def test_run_raw_query_preview_requires_db_and_table() -> None:
    with pytest.raises(ValueError, match="raw_db"):
        run_raw_query_preview(MagicMock(), {"raw_db": "db"})


def test_run_classic_query_preview_maps_rows(monkeypatch) -> None:
    item = MagicMock()
    item.external_id = "asset-1"
    item.dump.return_value = {"name": "Pump A"}

    client = MagicMock()
    client.assets.list.return_value = [item]

    out = run_classic_query_preview(client, {"resource_type": "assets"}, limit=10)
    assert out["row_count"] == 1
    assert out["columns"]
    assert out["items"][0]["external_id"] == "asset-1"


def test_run_raw_query_preview_filters_entity_rows(monkeypatch) -> None:
    row = MagicMock()
    row.key = "k1"
    row.columns = {"RECORD_KIND": "entity", "EXTERNAL_ID": "e1", "RUN_ID": "run-a"}

    monkeypatch.setattr(
        "cdf_fn_common.query_preview.iter_raw_table_rows_chunked",
        lambda *_a, **_k: [row],
    )

    out = run_raw_query_preview(
        MagicMock(),
        {"raw_db": "db", "raw_table_key": "tbl"},
        limit=50,
    )
    assert out["row_count"] == 1
    assert out["items"][0]["EXTERNAL_ID"] == "e1"

    out_filtered = run_raw_query_preview(
        MagicMock(),
        {"raw_db": "db", "raw_table_key": "tbl", "source_run_id": "other"},
        limit=50,
    )
    assert out_filtered["row_count"] == 0
