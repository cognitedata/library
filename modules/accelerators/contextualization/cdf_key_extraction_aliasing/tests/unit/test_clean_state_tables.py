"""Tests for cdf_fn_common.clean_state_tables and reference_index_naming."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.clean_state_tables import (
    _collect_db_table_pairs_from_scope_doc,
    clean_state_tables_from_scope_yaml,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.reference_index_naming import (
    reference_index_raw_table_from_key_extraction_table,
)


@pytest.mark.parametrize(
    "key_table,expected",
    [
        ("", "reference_index"),
        ("key_extraction_state", "reference_index"),
        ("site_key_extraction_state", "site_reference_index"),
        ("custom", "custom_reference_index"),
    ],
)
def test_reference_index_raw_table_from_key_extraction_table(key_table, expected):
    assert reference_index_raw_table_from_key_extraction_table(key_table) == expected


def test_collect_pairs_key_extraction_only():
    doc = {
        "key_extraction": {
            "config": {
                "parameters": {
                    "raw_db": "db_ke",
                    "raw_table_key": "key_extraction_state",
                }
            }
        }
    }
    pairs = _collect_db_table_pairs_from_scope_doc(doc)
    assert pairs == [
        ("db_ke", "key_extraction_state"),
        ("db_ke", "reference_index"),
    ]


def test_collect_pairs_custom_reference_index_override():
    doc = {
        "key_extraction": {
            "config": {
                "parameters": {
                    "raw_db": "db_ke",
                    "raw_table_key": "key_extraction_state",
                    "reference_index_raw_db": "db_other",
                    "reference_index_raw_table": "custom_ref_idx",
                }
            }
        }
    }
    pairs = _collect_db_table_pairs_from_scope_doc(doc)
    assert pairs == [
        ("db_ke", "key_extraction_state"),
        ("db_other", "custom_ref_idx"),
    ]


def test_collect_pairs_includes_aliasing_tables():
    doc = {
        "key_extraction": {
            "config": {
                "parameters": {
                    "raw_db": "db_ke",
                    "raw_table_key": "key_extraction_state",
                }
            }
        },
        "aliasing": {
            "config": {
                "parameters": {
                    "raw_db": "db_al",
                    "raw_table_state": "tag_aliasing_state",
                    "raw_table_aliases": "default_aliases",
                }
            }
        },
    }
    pairs = _collect_db_table_pairs_from_scope_doc(doc)
    assert pairs == [
        ("db_ke", "key_extraction_state"),
        ("db_ke", "reference_index"),
        ("db_al", "tag_aliasing_state"),
        ("db_al", "default_aliases"),
    ]


def test_clean_state_tables_from_scope_yaml_invokes_delete(tmp_path):
    doc = {
        "key_extraction": {
            "config": {
                "parameters": {
                    "raw_db": "db_ke",
                    "raw_table_key": "key_extraction_state",
                }
            }
        }
    }
    p = tmp_path / "scope.yaml"
    p.write_text(yaml.safe_dump(doc), encoding="utf-8")

    client = MagicMock()
    log = MagicMock()
    out = clean_state_tables_from_scope_yaml(client, log, p)

    assert len(out) == 2
    assert client.raw.tables.delete.call_count == 2
    calls = [c.args for c in client.raw.tables.delete.call_args_list]
    assert ("db_ke", ["key_extraction_state"]) in calls
    assert ("db_ke", ["reference_index"]) in calls
