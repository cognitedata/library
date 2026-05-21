"""Tests for merged persistence instances in local run results."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_MODULE = Path(__file__).resolve().parents[2]
if str(_MODULE) not in sys.path:
    sys.path.insert(0, str(_MODULE))

from local_runner.persistence_instances_merge import (  # noqa: E402
    build_merged_persistence_instances,
)


def _cohort_row(key: str, nid: str, props: dict) -> dict:
    return {
        "key": key,
        "columns": {
            "NODE_INSTANCE_ID": nid,
            "SCOPE_KEY": "default",
            "EXTERNAL_ID": nid.split(":")[-1],
            "PROPERTIES_JSON": json.dumps(props),
        },
    }


def test_merge_unions_save_and_index_predecessor_cohorts() -> None:
    snaps = {
        "save_1": {
            "task_id": "save_1",
            "function_external_id": "fn_dm_view_save",
            "handler_summary": {"instances_applied": 1},
            "cohort_snapshot": {
                "predecessor_cohort": {
                    "cohort_rows": [
                        _cohort_row("default:inst-1", "sp:inst-1", {"aliases": ["P-101"]}),
                    ],
                },
            },
        },
        "ii_1": {
            "task_id": "ii_1",
            "function_external_id": "fn_dm_inverted_index",
            "cohort_snapshot": {
                "predecessor_cohort": {
                    "cohort_rows": [
                        _cohort_row("default:inst-1", "sp:inst-1", {"indexKey": ["P-101"]}),
                    ],
                },
                "inverted_index_persistence": {
                    "raw_db": "db_discovery",
                    "raw_table": "discovery_inverted_index",
                    "index_rows": [
                        {
                            "key": "metadata:P-101",
                            "columns": {"INDEX_KIND": "metadata", "LOOKUP_KEY": "P-101"},
                        },
                    ],
                },
            },
        },
    }
    merged = build_merged_persistence_instances(snaps)
    assert merged["instance_count"] == 1
    assert merged["cohort_rows_ingested"] == 2
    assert merged["inverted_index_sink_row_count"] == 1
    inst = merged["instances"][0]
    assert inst["instance_key"] == "sp:inst-1"
    assert inst["properties"]["aliases"] == ["P-101"]
    assert inst["properties"]["indexKey"] == ["P-101"]
    assert len(inst["contributions"]) == 2
    assert merged["inverted_index_sink_rows"][0]["task_id"] == "ii_1"


def test_merge_empty_snapshots() -> None:
    merged = build_merged_persistence_instances({})
    assert merged["instance_count"] == 0
    assert merged["instances"] == []
