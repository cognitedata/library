"""Unit tests for watermark incremental index builds."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from inverted_index.index_build_watermark import (
    read_watermark,
    run_watermark_incremental_build,
    write_watermark,
)


def test_read_watermark_returns_none_when_missing() -> None:
    client = MagicMock()
    client.raw.rows.retrieve.side_effect = Exception("not found")
    assert read_watermark(client, {"raw_database": "db", "state_table": "t"}) is None


def test_write_and_read_watermark_roundtrip() -> None:
    client = MagicMock()
    stored: dict = {}

    def _insert(*, db_name, table_name, row):
        stored.update(row)

    def _retrieve(db, table, key):
        if key not in stored:
            raise Exception("missing")
        return MagicMock(columns=stored[key])

    client.raw.rows.insert.side_effect = _insert
    client.raw.rows.retrieve.side_effect = _retrieve

    ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    with patch("inverted_index.index_build_watermark.create_table_if_not_exists"):
        write_watermark(client, ts, {"raw_database": "db", "state_table": "t"})
        loaded = read_watermark(client, {"raw_database": "db", "state_table": "t"})

    assert loaded == ts


def test_run_watermark_incremental_build_invokes_both_builds() -> None:
    client = MagicMock()
    runtime = {
        "source_index_config": {
            "enabled": True,
            "watermark": {
                "enabled": True,
                "raw_database": "db",
                "state_table": "index_build_state",
                "initial_lookback_seconds": 60,
            },
        },
        "storage_config": {"backend": "raw", "raw": {"database": "db"}},
        "index_field_config": [],
        "annotation_index_config": {},
        "scope_config": {},
        "instance_spaces": None,
    }
    with patch(
        "inverted_index.index_build_watermark.read_watermark",
        return_value=None,
    ), patch(
        "inverted_index.index_build_watermark.build_metadata_index",
        return_value={"processed": 1},
    ) as meta, patch(
        "inverted_index.index_build_watermark.build_diagram_annotation_index",
        return_value={"processed": 2},
    ) as ann, patch(
        "inverted_index.index_build_watermark.write_watermark",
    ) as write_wm, patch(
        "inverted_index.index_build_watermark.get_storage_adapter",
        return_value=MagicMock(),
    ):
        result = run_watermark_incremental_build(
            client,
            dry_run=False,
            runtime_config=runtime,
        )

    meta.assert_called_once()
    ann.assert_called_once()
    write_wm.assert_called_once()
    assert result["status"] == "ok"
    assert result["metadata"]["processed"] == 1
    assert result["annotations"]["processed"] == 2
    assert "watermark_before" in result
    assert "watermark_after" in result
