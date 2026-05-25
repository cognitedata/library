"""Unit tests for ETL build_index and inverted-index persistence."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
FUNCS = ROOT / "functions"
if str(FUNCS) not in sys.path:
    sys.path.insert(0, str(FUNCS))

from cdf_fn_common.etl_build_index_orchestration import etl_handle_build_index  # noqa: E402
from cdf_fn_common.etl_inverted_index import (  # noqa: E402
    INDEX_KIND_COLUMN,
    LOOKUP_KEY_COLUMN,
    POSTINGS_JSON_COLUMN,
    build_inverted_index_rows,
    merge_postings,
    normalize_lookup_key,
    parse_index_kinds_config,
)
from cdf_fn_common.etl_save_apply import etl_persist_inverted_index_save  # noqa: E402


def test_parse_index_kinds_config_metadata_index_key() -> None:
    pairs = parse_index_kinds_config({"index_kinds": {"metadata": ["indexKey"]}})
    assert pairs == [("metadata", "indexKey")]


def test_build_inverted_index_rows_shape() -> None:
    pending = {
        ("metadata", "p-101a"): [
            {
                "instance_space": "sp",
                "external_id": "ext1",
                "source_property": "indexKey",
                "run_id": "run1",
                "confidence": 0.9,
            }
        ]
    }
    rows = build_inverted_index_rows(
        pending=pending,
        run_id="run1",
        canvas_node_id="build_idx",
    )
    assert len(rows) == 1
    assert rows[0]["key"] == "metadata:p-101a"
    cols = rows[0]["columns"]
    assert cols[INDEX_KIND_COLUMN] == "metadata"
    assert cols[LOOKUP_KEY_COLUMN] == "p-101a"
    assert cols["RECORD_KIND"] == "index_posting"
    postings = json.loads(cols[POSTINGS_JSON_COLUMN])
    assert len(postings) == 1
    assert postings[0]["confidence"] == 0.9


@patch("cdf_fn_common.etl_build_index_orchestration._flush_rows")
@patch("cdf_fn_common.etl_build_index_orchestration.resolve_query_sink", return_value=("db", "tbl"))
def test_build_index_in_memory(_sink: MagicMock, _flush: MagicMock) -> None:
    data = {
        "task_id": "bi1",
        "run_id": "run1",
        "local_predecessor_mode": "in_memory",
        "_predecessor_rows": [
            {
                "columns": {
                    "EXTERNAL_ID": "ext1",
                    "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
                },
                "properties": {"indexKey": ["TAG-1"], "instance_space": "sp"},
            }
        ],
        "config": {"index_kinds": {"metadata": ["indexKey"]}},
    }
    summary = etl_handle_build_index("fn_etl_build_index", data, None, None)
    assert summary["index_rows_written"] == 1
    assert len(data["_predecessor_index_rows"]) == 1
    assert data["_predecessor_index_rows"][0]["key"] == "metadata:tag-1"


@patch("cdf_fn_common.etl_save_apply.persist_inverted_index_rows", return_value=1)
@patch("cdf_fn_common.etl_save_apply._iter_index_rows_for_save")
def test_persist_inverted_index_save(mock_rows: MagicMock, mock_persist: MagicMock) -> None:
    mock_rows.return_value = [
        {
            "key": "metadata:tag-1",
            "columns": {
                "INDEX_KIND": "metadata",
                "LOOKUP_KEY": "tag-1",
                "POSTINGS_JSON": "[]",
            },
        }
    ]
    client = MagicMock()
    data = {
        "task_id": "save1",
        "run_id": "run1",
        "config": {
            "source_raw_db": "db_discovery",
            "source_raw_table_key": "discovery_inverted_index",
        },
    }
    summary = etl_persist_inverted_index_save("fn_etl_raw_save", data, client, None)
    assert summary["save_kind"] == "inverted_index"
    assert summary["rows_written"] == 1
    mock_persist.assert_called_once()
