"""Unit tests for discovery inverted index."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
_DISCOVERY_ROOT = _MODULE_ROOT.parent / "cdf_discovery"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))
os.environ.setdefault(
    "CDF_INVERTED_INDEX_ROOT",
    str(_DISCOVERY_ROOT / "inverted_index"),
)

from cdf_fn_common.discovery_inverted_index import (  # noqa: E402
    merge_postings,
    normalize_lookup_key,
    parse_index_kinds_config,
    run_discovery_inverted_index,
)


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


@patch("cdf_fn_common.discovery_inverted_index.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.discovery_inverted_index.iter_predecessor_instance_props")
@patch("cdf_fn_common.discovery_inverted_index.persist_index_entries_via_adapter")
def test_indexes_index_key_via_adapter(
    mock_persist: MagicMock,
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
    mock_persist.return_value = {"entries_created": 1, "entries_updated": 0}
    client = MagicMock()
    data = {
        "task_id": "ii1",
        "run_id": "run1",
        "configuration": {
            "scope": {"path": [{"level": "site", "id": "SITE_01"}]},
        },
        "compiled_workflow": {
            "tasks": [
                {"task_id": "ii1", "canvas_node_id": "cf_ii", "depends_on": ["pred"]},
                {"task_id": "pred", "canvas_node_id": "fl", "depends_on": []},
            ]
        },
        "config": {
            "index_kinds": {"metadata": ["indexKey"]},
            "index_storage_backend": "raw",
            "index_raw_database": "db_contextualization_idx",
            "scope": {
                "enabled": True,
                "levels": ["site"],
                "scope_key_template": "site:{site}",
                "fallback_scope_key": "global",
            },
        },
    }
    summary = run_discovery_inverted_index("fn_dm_inverted_index", data, client, None)
    assert summary["entries_created"] == 1
    assert summary["storage_backend"] == "raw"
    assert summary["match_scope_key"] == "site:SITE_01"
    mock_persist.assert_called_once()
    entries = mock_persist.call_args[0][1]
    assert len(entries) == 1
    assert entries[0]["source_type"] == "asset_metadata"
    assert entries[0]["reference_external_id"] == "ext1"


@patch("cdf_fn_common.discovery_inverted_index.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.discovery_inverted_index.iter_predecessor_instance_props")
@patch("cdf_fn_common.discovery_inverted_index.persist_index_entries_via_adapter")
def test_does_not_index_empty_index_key_string(
    mock_persist: MagicMock,
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
    mock_persist.return_value = {"entries_created": 0, "entries_updated": 0}
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
                "index_storage_backend": "raw",
            },
        },
        MagicMock(),
        None,
    )
    assert summary["postings"] == 0
    mock_persist.assert_called_once()
    assert mock_persist.call_args[0][1] == []


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
    assert summary["entries_created"] == 0
