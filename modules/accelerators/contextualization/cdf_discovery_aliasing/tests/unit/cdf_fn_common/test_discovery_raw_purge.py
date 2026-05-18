"""Unit tests for discovery_raw_purge helpers."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.discovery_raw_purge import (  # noqa: E402
    collect_discovery_raw_tables,
    collect_inter_node_cohort_tables,
    delete_cohort_keys_for_run,
    purge_inter_node_cohort_tables,
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


def test_collect_inter_node_cohort_tables_excludes_aliasing_and_inverted_index() -> None:
    scope = {
        "key_extraction": {
            "config": {
                "parameters": {"raw_db": "db_ke", "raw_table_key": "discovery_state"},
            }
        },
        "aliasing": {
            "config": {
                "parameters": {"raw_db": "db_alias", "raw_table": "asset_aliases"},
            }
        },
    }
    cw = {
        "tasks": [
            {
                "executor_kind": "inverted_index",
                "persistence": {
                    "raw_db": "db_discovery",
                    "raw_table_key": "discovery_state",
                    "inverted_index_raw_table": "discovery_inverted_index",
                },
            },
            {
                "executor_kind": "transform",
                "persistence": {
                    "raw_db": "db_discovery",
                    "raw_table_key": "transform_sink",
                },
            },
            {
                "executor_kind": "save_view",
                "persistence": {"raw_db": "db_x", "raw_table_key": "should_not_add"},
            },
        ]
    }
    tables = collect_inter_node_cohort_tables(scope, cw)
    assert ("db_ke", "discovery_state") in tables
    assert ("db_discovery", "transform_sink") in tables
    assert ("db_alias", "asset_aliases") not in tables
    assert ("db_discovery", "discovery_inverted_index") not in tables
    assert ("db_x", "should_not_add") not in tables


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
    assert collect_inter_node_cohort_tables(scope) == [("x", "y")]


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


def _pipeline_rid(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%S.%f") + "Z-000000000000"


@patch("cdf_fn_common.discovery_raw_purge.iter_raw_table_rows_chunked")
def test_purge_inter_node_deletes_current_and_stale(mock_iter) -> None:
    now = datetime.now(timezone.utc)
    current = _pipeline_rid(now)
    stale = _pipeline_rid(now - timedelta(hours=100))
    fresh_other = _pipeline_rid(now - timedelta(hours=1))
    rows = [
        SimpleNamespace(key=f"{current}:sk:n1", columns={"RUN_ID": current, "RECORD_KIND": "entity"}),
        SimpleNamespace(key=f"{stale}:sk:n2", columns={"RUN_ID": stale, "RECORD_KIND": "entity"}),
        SimpleNamespace(
            key=f"{fresh_other}:sk:n3",
            columns={"RUN_ID": fresh_other, "RECORD_KIND": "entity"},
        ),
        SimpleNamespace(key="scope_wm_abc", columns={"RECORD_KIND": "watermark"}),
    ]
    mock_iter.return_value = iter(rows)
    client = MagicMock()
    out = purge_inter_node_cohort_tables(
        client,
        [("db", "tbl")],
        current,
        retention_hours=72,
        purge_stale=True,
        dry_run=False,
    )
    deleted_keys = [c[0][2] for c in client.raw.rows.delete.call_args_list]
    flat = [k for batch in deleted_keys for k in batch]
    assert f"{current}:sk:n1" in flat
    assert f"{stale}:sk:n2" in flat
    assert f"{fresh_other}:sk:n3" not in flat
    assert "scope_wm_abc" not in flat
    assert out["purge_stale"] is True
    assert out["tables"][0]["rows_deleted_estimate"] == 2


@patch("cdf_fn_common.discovery_raw_purge.delete_cohort_keys_for_run")
def test_purge_stale_false_delegates_to_current_run_only(mock_delete) -> None:
    mock_delete.return_value = {"action": "delete_run_cohort_keys", "run_id": "r1", "tables": []}
    client = MagicMock()
    out = purge_inter_node_cohort_tables(
        client,
        [("db", "tbl")],
        "r1",
        purge_stale=False,
    )
    mock_delete.assert_called_once()
    assert out["purge_stale"] is False


@patch("cdf_fn_common.discovery_raw_purge.collect_inter_node_cohort_tables")
@patch("cdf_fn_common.discovery_raw_purge.purge_inter_node_cohort_tables")
def test_run_discovery_raw_cleanup_default_uses_cohort_collector(
    mock_purge, mock_collect
) -> None:
    mock_collect.return_value = [("d", "t")]
    mock_purge.return_value = {"action": "delete_run_cohort_keys", "run_id": "r1", "tables": []}
    client = MagicMock()
    scope = {"key_extraction": {"config": {"parameters": {"raw_db": "d", "raw_table_key": "t"}}}}
    run_discovery_raw_cleanup_action(
        client,
        scope_document=scope,
        compiled_workflow=None,
        run_id="r1",
        action="delete_run_cohort_keys",
        retention_hours=72,
        purge_stale=True,
    )
    mock_collect.assert_called_once()
    mock_purge.assert_called_once()
    assert mock_purge.call_args.kwargs["retention_hours"] == 72
    assert mock_purge.call_args.kwargs["purge_stale"] is True


@patch("cdf_fn_common.discovery_raw_purge.collect_discovery_raw_tables")
def test_run_discovery_raw_cleanup_action_truncate(mock_collect) -> None:
    mock_collect.return_value = [("d", "t")]
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
    mock_collect.assert_called_once()
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
