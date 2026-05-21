"""Unit tests for discovery inverted index."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.discovery_inverted_index import (  # noqa: E402
    INDEX_KIND_COLUMN,
    LOOKUP_KEY_COLUMN,
    POSTINGS_JSON_COLUMN,
    merge_postings,
    normalize_lookup_key,
    parse_index_kinds_config,
    run_discovery_inverted_index,
)
from cdf_fn_common.discovery_query_shared import resolve_inverted_index_sink  # noqa: E402


def test_parse_index_kinds_config_empty() -> None:
    assert parse_index_kinds_config({}) == []
    assert parse_index_kinds_config({"index_kinds": {}}) == []


def test_parse_index_kinds_config_metadata_index_key() -> None:
    pairs = parse_index_kinds_config(
        {"index_kinds": {"metadata": ["indexKey"]}}
    )
    assert pairs == [("metadata", "indexKey")]


def test_normalize_lookup_key_casefold() -> None:
    assert normalize_lookup_key("  P-101A  ") == "p-101a"


def test_merge_postings_replaces_same_run() -> None:
    existing = [
        {
            "instance_space": "sp",
            "external_id": "a1",
            "source_property": "indexKey",
            "run_id": "r1",
            "confidence": 0.5,
        }
    ]
    incoming = [
        {
            "instance_space": "sp",
            "external_id": "a1",
            "source_property": "indexKey",
            "run_id": "r1",
            "confidence": 0.9,
        }
    ]
    merged = merge_postings(existing, incoming)
    assert len(merged) == 1
    assert merged[0]["confidence"] == 0.9


def test_resolve_inverted_index_sink_defaults() -> None:
    db, tbl = resolve_inverted_index_sink(
        {
            "persistence": {"raw_db": "db_discovery", "raw_table": "discovery_state"},
            "config": {},
        }
    )
    assert db == "db_discovery"
    assert tbl == "discovery_state_inverted_index"


@patch("cdf_fn_common.discovery_inverted_index.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.discovery_inverted_index.iter_predecessor_instance_props")
@patch("cdf_fn_common.discovery_inverted_index._load_existing_postings", return_value=[])
@patch("cdf_fn_common.discovery_inverted_index._flush_rows")
def test_indexes_index_key_only(
    _flush: MagicMock,
    _load: MagicMock,
    mock_iter: MagicMock,
    _pred: MagicMock,
) -> None:
    row = MagicMock()
    row.key = "k1"
    row.columns = {
        "RECORD_KIND": "entity",
        "RUN_ID": "run1",
        "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        "VIEW_SPACE": "cdf_cdm",
        "VIEW_EXTERNAL_ID": "CogniteAsset",
        "VIEW_VERSION": "v1",
        "ENTITY_TYPE": "asset",
        "PROPERTIES_JSON": json.dumps(
            {
                "indexKey": ["TAG-1"],
                "aliases": ["alias-should-not-index"],
                "instance_space": "sp",
            }
        ),
    }
    from cdf_fn_common.discovery_cohort import _props_from_row_columns

    cols = dict(row.columns)
    mock_iter.return_value = [(cols, _props_from_row_columns(cols))]
    client = MagicMock()
    data = {
        "task_id": "ii1",
        "run_id": "run1",
        "compiled_workflow": {
            "tasks": [
                {"task_id": "ii1", "canvas_node_id": "cf_ii", "depends_on": ["pred"]},
                {"task_id": "pred", "canvas_node_id": "fl", "depends_on": []},
            ]
        },
        "config": {
            "index_kinds": {"metadata": ["indexKey"]},
            "inverted_index_raw_table": "discovery_inverted_index",
        },
    }
    summary = run_discovery_inverted_index("fn_dm_inverted_index", data, client, None)
    assert summary["inverted_writes"] == 1
    _flush.assert_called_once()
    uploaded = _flush.call_args[0][3]
    assert len(uploaded) == 1
    cols = uploaded[0]["columns"]
    assert cols[INDEX_KIND_COLUMN] == "metadata"
    assert cols[LOOKUP_KEY_COLUMN] == normalize_lookup_key("TAG-1")
    postings = json.loads(cols[POSTINGS_JSON_COLUMN])
    assert len(postings) == 1
    assert postings[0]["source_property"] == "indexKey"
    assert postings[0]["external_id"] == "ext1"


@patch("cdf_fn_common.discovery_inverted_index.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.discovery_inverted_index.iter_predecessor_instance_props")
@patch("cdf_fn_common.discovery_inverted_index._load_existing_postings", return_value=[])
@patch("cdf_fn_common.discovery_inverted_index._flush_rows")
def test_does_not_index_empty_index_key_string(
    _flush: MagicMock,
    _load: MagicMock,
    mock_iter: MagicMock,
    _pred: MagicMock,
) -> None:
    row = MagicMock()
    row.key = "k1"
    row.columns = {
        "RECORD_KIND": "entity",
        "RUN_ID": "run1",
        "NODE_INSTANCE_ID": "sp:ext1",
        "EXTERNAL_ID": "ext1",
        "VIEW_SPACE": "cdf_cdm",
        "VIEW_EXTERNAL_ID": "CogniteFile",
        "VIEW_VERSION": "v1",
        "ENTITY_TYPE": "file",
        "PROPERTIES_JSON": json.dumps(
            {
                "indexKey": "",
                "aliases": ["file.pdf"],
                "instance_space": "sp",
            }
        ),
    }
    from cdf_fn_common.discovery_cohort import _props_from_row_columns

    cols = dict(row.columns)
    mock_iter.return_value = [(cols, _props_from_row_columns(cols))]
    summary = run_discovery_inverted_index(
        "fn_dm_inverted_index",
        {
            "task_id": "ii1",
            "run_id": "run1",
            "compiled_workflow": {
                "tasks": [
                    {"task_id": "ii1", "canvas_node_id": "cf_ii", "depends_on": ["pred"]},
                    {"task_id": "pred", "canvas_node_id": "fl", "depends_on": []},
                ]
            },
            "config": {"index_kinds": {"metadata": ["indexKey"]}},
        },
        MagicMock(),
        None,
    )
    assert summary["inverted_writes"] == 0
    assert _flush.call_args[0][3] == []


@patch("cdf_fn_common.discovery_inverted_index.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.discovery_inverted_index.iter_predecessor_instance_props")
@patch("cdf_fn_common.discovery_inverted_index._load_existing_postings", return_value=[])
@patch("cdf_fn_common.discovery_inverted_index._flush_rows")
def test_does_not_index_aliases_when_index_key_missing(
    _flush: MagicMock,
    _load: MagicMock,
    mock_iter: MagicMock,
    _pred: MagicMock,
) -> None:
    row = MagicMock()
    row.key = "k1"
    row.columns = {
        "RECORD_KIND": "entity",
        "RUN_ID": "run1",
        "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        "VIEW_SPACE": "cdf_cdm",
        "VIEW_EXTERNAL_ID": "CogniteAsset",
        "VIEW_VERSION": "v1",
        "ENTITY_TYPE": "asset",
        "PROPERTIES_JSON": json.dumps(
            {"aliases": ["TAG-FROM-ALIAS"], "instance_space": "sp"}
        ),
    }
    from cdf_fn_common.discovery_cohort import _props_from_row_columns

    cols = dict(row.columns)
    mock_iter.return_value = [(cols, _props_from_row_columns(cols))]
    summary = run_discovery_inverted_index(
        "fn_dm_inverted_index",
        {
            "task_id": "ii1",
            "run_id": "run1",
            "compiled_workflow": {
                "tasks": [
                    {"task_id": "ii1", "canvas_node_id": "cf_ii", "depends_on": ["pred"]},
                    {"task_id": "pred", "canvas_node_id": "fl", "depends_on": []},
                ]
            },
            "config": {
                "index_kinds": {"metadata": ["indexKey"]},
                "inverted_index_raw_table": "discovery_inverted_index",
            },
        },
        MagicMock(),
        None,
    )
    assert summary["inverted_writes"] == 0
    assert summary["postings"] == 0
    _flush.assert_called_once()
    assert _flush.call_args[0][3] == []


@patch("cdf_fn_common.discovery_inverted_index.iter_predecessor_raw_locations", return_value=[])
def test_skips_when_no_index_kinds(_pred: MagicMock) -> None:
    summary = run_discovery_inverted_index(
        "fn_dm_inverted_index",
        {"task_id": "ii", "config": {}},
        MagicMock(),
        None,
    )
    assert summary["status"] == "skipped"
    assert summary["reason"] == "no_index_kinds_configured"
    assert summary["inverted_writes"] == 0
