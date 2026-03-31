"""Unit tests for RAW extraction store skip policy and row kind helpers."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.pipeline import (  # noqa: E402
    EXTRACTION_STATUS_COLUMN,
    EXTRACTION_STATUS_EMPTY,
    EXTRACTION_STATUS_FAILED,
    EXTRACTION_STATUS_SUCCESS,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RECORD_KIND_RUN,
    _entity_row_should_skip_listing,
    _is_run_summary_row,
    _read_entity_keys_to_exclude,
)


class _FakeRow:
    def __init__(self, key: str, columns: dict):
        self.key = key
        self.columns = columns


class TestRunSummaryAndSkipPolicy(unittest.TestCase):
    def test_is_run_summary_row(self):
        self.assertTrue(
            _is_run_summary_row({RECORD_KIND_COLUMN: RECORD_KIND_RUN})
        )
        self.assertFalse(_is_run_summary_row({RECORD_KIND_COLUMN: RECORD_KIND_ENTITY}))
        self.assertFalse(_is_run_summary_row({}))

    def test_successful_only_missing_status_not_skipped(self):
        self.assertFalse(_entity_row_should_skip_listing("successful_only", {}))
        self.assertFalse(
            _entity_row_should_skip_listing(
                "successful_only", {RECORD_KIND_COLUMN: RECORD_KIND_ENTITY}
            )
        )

    def test_successful_only_success_and_empty(self):
        self.assertTrue(
            _entity_row_should_skip_listing(
                "successful_only",
                {EXTRACTION_STATUS_COLUMN: EXTRACTION_STATUS_SUCCESS},
            )
        )
        self.assertTrue(
            _entity_row_should_skip_listing(
                "successful_only",
                {EXTRACTION_STATUS_COLUMN: EXTRACTION_STATUS_EMPTY},
            )
        )

    def test_successful_only_failed_not_skipped(self):
        self.assertFalse(
            _entity_row_should_skip_listing(
                "successful_only",
                {EXTRACTION_STATUS_COLUMN: EXTRACTION_STATUS_FAILED},
            )
        )

    def test_none_never_skips(self):
        self.assertFalse(_entity_row_should_skip_listing("none", {}))
        self.assertFalse(
            _entity_row_should_skip_listing(
                "none",
                {EXTRACTION_STATUS_COLUMN: EXTRACTION_STATUS_SUCCESS},
            )
        )

    def test_run_row_never_skip_listing(self):
        self.assertFalse(
            _entity_row_should_skip_listing(
                "successful_only",
                {RECORD_KIND_COLUMN: RECORD_KIND_RUN},
            )
        )

    def test_read_entity_keys_to_exclude_filters_run_rows(self):
        client = MagicMock()
        batches = [
            [
                _FakeRow("run_ts", {RECORD_KIND_COLUMN: RECORD_KIND_RUN}),
                _FakeRow("ent_ok", {EXTRACTION_STATUS_COLUMN: EXTRACTION_STATUS_SUCCESS}),
            ],
            [
                _FakeRow(
                    "ent_fail",
                    {EXTRACTION_STATUS_COLUMN: EXTRACTION_STATUS_FAILED},
                ),
            ],
        ]
        client.raw.rows = MagicMock(
            side_effect=lambda *args, **kwargs: iter(batches)
        )
        out = _read_entity_keys_to_exclude(
            client, "db", "tbl", "successful_only", 5000
        )
        self.assertEqual(sorted(out), ["ent_ok"])

    def test_read_entity_keys_to_exclude_ignores_legacy_rows_without_status(self):
        client = MagicMock()
        batches = [
            [
                _FakeRow("legacy", {"NAME": ["x"]}),
                _FakeRow("done", {EXTRACTION_STATUS_COLUMN: EXTRACTION_STATUS_SUCCESS}),
            ],
        ]
        client.raw.rows = MagicMock(
            side_effect=lambda *args, **kwargs: iter(batches)
        )
        out = _read_entity_keys_to_exclude(
            client, "db", "tbl", "successful_only", 1000
        )
        self.assertEqual(sorted(out), ["done"])

    def test_read_entity_keys_to_exclude_none_policy(self):
        client = MagicMock()
        client.raw.rows = MagicMock()
        out = _read_entity_keys_to_exclude(client, "db", "tbl", "none", 1000)
        self.assertEqual(out, [])
        client.raw.rows.assert_not_called()


if __name__ == "__main__":
    unittest.main()
