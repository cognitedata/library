"""Unit tests for discovery_raw_purge helpers."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.discovery_raw_purge import (  # noqa: E402
    collect_discovery_raw_tables,
    delete_cohort_keys_for_run,
    run_discovery_raw_cleanup_action,
    truncate_raw_tables,
)


def test_collect_discovery_raw_tables_merges_scope_and_tasks() -> None:
    scope = {
        "key_extraction": {
            "config": {
                "parameters": {"raw_db": "db_a", "raw_table_key": "t_main"},
            }
        },
        "aliasing": {"config": {"parameters": {"raw_db": "db_a", "raw_table": "t_alias"}}},
    }
    cw = {
        "tasks": [
            {
                "persistence": {
                    "raw_db": "db_b",
                    "raw_table_key": "t_sink",
                }
            }
        ]
    }
    tables = collect_discovery_raw_tables(scope, cw)
    assert tables == [("db_a", "t_alias"), ("db_a", "t_main"), ("db_b", "t_sink")]


def test_collect_uses_configuration_branch_when_present() -> None:
    scope = {
        "configuration": {
            "key_extraction": {
                "config": {"parameters": {"raw_db": "x", "raw_table_key": "y"}},
            },
            "aliasing": {
                "config": {"parameters": {"raw_db": "x", "raw_table_key": "y"}},
            },
        },
        "key_extraction": {
            "config": {"parameters": {"raw_db": "ignored", "raw_table_key": "ignored"}},
        },
    }
    assert collect_discovery_raw_tables(scope) == [("x", "y")]


def test_truncate_raw_tables_dry_run_no_delete() -> None:
    client = MagicMock()
    out = truncate_raw_tables(client, [("d", "t")], dry_run=True)
    assert out["action"] == "truncate_tables"
    assert out["tables"][0]["dry_run"] is True
    client.raw.tables.delete.assert_not_called()


def test_truncate_raw_tables_calls_delete() -> None:
    client = MagicMock()
    out = truncate_raw_tables(client, [("d", "t")], dry_run=False)
    client.raw.tables.delete.assert_called_once_with("d", "t")
    assert out["tables"][0]["deleted"] is True


@patch("cdf_fn_common.discovery_raw_purge.iter_inter_node_raw_rows_for_filter_run")
def test_delete_cohort_keys_batches_and_respects_dry_run(mock_iter) -> None:
    keys = [f"run1:{i}" for i in range(5)]

    def _rows(*_a, **_k):
        for k in keys:
            yield SimpleNamespace(key=k)

    mock_iter.side_effect = _rows
    client = MagicMock()
    out = delete_cohort_keys_for_run(
        client,
        [("db", "tbl")],
        "run1",
        delete_batch_size=2,
        strict_key_prefix_only=True,
        dry_run=False,
    )
    assert out["action"] == "delete_run_cohort_keys"
    assert out["run_id"] == "run1"
    assert client.raw.rows.delete.call_count == 3
    assert out["tables"][0]["rows_deleted_estimate"] == 5


@patch("cdf_fn_common.discovery_raw_purge.iter_inter_node_raw_rows_for_filter_run")
def test_delete_cohort_keys_dry_run_skips_delete(mock_iter) -> None:
    mock_iter.return_value = [SimpleNamespace(key="run1:x")]
    client = MagicMock()
    out = delete_cohort_keys_for_run(
        client,
        [("db", "tbl")],
        "run1",
        delete_batch_size=100,
        strict_key_prefix_only=True,
        dry_run=True,
    )
    client.raw.rows.delete.assert_not_called()
    assert out["tables"][0]["dry_run"] is True
    assert out["tables"][0]["rows_deleted_estimate"] == 1


def test_run_discovery_raw_cleanup_action_truncate() -> None:
    client = MagicMock()
    scope = {
        "key_extraction": {
            "config": {"parameters": {"raw_db": "d", "raw_table_key": "t"}},
        }
    }
    out = run_discovery_raw_cleanup_action(
        client,
        scope_document=scope,
        compiled_workflow=None,
        run_id="r1",
        action="truncate_tables",
        dry_run=True,
    )
    assert out["action"] == "truncate_tables"
    client.raw.tables.delete.assert_not_called()


def test_run_discovery_raw_cleanup_action_override_tables() -> None:
    client = MagicMock()
    out = run_discovery_raw_cleanup_action(
        client,
        scope_document={},
        compiled_workflow=None,
        run_id="r1",
        action="truncate_tables",
        raw_tables_override=[{"raw_db": "a", "raw_table": "b"}],
        dry_run=False,
    )
    client.raw.tables.delete.assert_called_once_with("a", "b")
    assert out["action"] == "truncate_tables"


def test_run_discovery_raw_cleanup_unknown_action() -> None:
    client = MagicMock()
    out = run_discovery_raw_cleanup_action(
        client,
        scope_document={"key_extraction": {"config": {"parameters": {"raw_db": "d", "raw_table_key": "t"}}}},
        compiled_workflow=None,
        run_id="r1",
        action="nope",
    )
    assert "error" in out
