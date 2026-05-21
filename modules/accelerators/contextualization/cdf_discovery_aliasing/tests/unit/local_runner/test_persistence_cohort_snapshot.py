"""Unit tests for persistence cohort snapshots at save / inverted-index tasks."""

from __future__ import annotations

import sys
from pathlib import Path
_MODULE = Path(__file__).resolve().parents[2]
if str(_MODULE) not in sys.path:
    sys.path.insert(0, str(_MODULE))

from local_runner.persistence_cohort_snapshot import (  # noqa: E402
    build_persistence_cohort_snapshot,
    parse_handler_summary_message,
)


def test_parse_handler_summary_message() -> None:
    assert parse_handler_summary_message('{"rows_read": 3}') == {"rows_read": 3}
    assert parse_handler_summary_message("") == {}


def test_build_snapshot_without_client() -> None:
    data = {
        "run_id": "run_1",
        "task_id": "save_a",
        "compiled_workflow": {
            "tasks": [
                {
                    "id": "save_a",
                    "function_external_id": "fn_dm_view_save",
                    "depends_on": [],
                    "config": {"view_external_id": "CogniteAsset"},
                }
            ]
        },
    }
    snap = build_persistence_cohort_snapshot(
        None,
        data,
        task_id="save_a",
        function_external_id="fn_dm_view_save",
    )
    assert snap["run_id"] == "run_1"
    assert snap["predecessor_cohort"]["cohort_row_count"] == 0
    assert snap["predecessor_cohort"]["error"] == "no_client"


def test_build_inverted_index_snapshot_without_client() -> None:
    data = {
        "run_id": "run_1",
        "task_id": "ii_a",
        "configuration": {"inverted_index_raw_table": "idx_tbl"},
        "compiled_workflow": {
            "tasks": [
                {
                    "id": "ii_a",
                    "function_external_id": "fn_dm_inverted_index",
                    "depends_on": [],
                }
            ]
        },
    }
    snap = build_persistence_cohort_snapshot(
        None,
        data,
        task_id="ii_a",
        function_external_id="fn_dm_inverted_index",
    )
    assert "inverted_index_persistence" in snap
    assert snap["inverted_index_persistence"]["error"] == "no_client"
