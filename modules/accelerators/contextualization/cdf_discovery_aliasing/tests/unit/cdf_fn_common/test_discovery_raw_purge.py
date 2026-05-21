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
    delete_all_run_node_cohort_tables,
    delete_run_cohort_tables,
    purge_discovery_raw_baseline,
    purge_inter_node_cohort_tables,
    purge_stale_run_tables,
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


def test_collect_inter_node_cohort_tables_returns_base_only() -> None:
    scope = {
        "key_extraction": {
            "config": {
                "parameters": {"raw_db": "db_ke", "raw_table_key": "discovery_state"},
            }
        },
    }
    assert collect_inter_node_cohort_tables(scope, {}) == [("db_ke", "discovery_state")]


@patch("cdf_fn_common.discovery_raw_purge.list_run_cohort_tables")
def test_delete_run_cohort_tables_drops_all_node_tables(mock_list) -> None:
    mock_list.return_value = [
        "discovery_state__run1__tr",
        "discovery_state__run1__vq",
    ]
    client = MagicMock()
    out = delete_run_cohort_tables(client, "db", "run1", base_table="discovery_state", dry_run=False)
    assert out["action"] == "delete_run_cohort_tables"
    assert out["tables_deleted"] == 2
    assert client.raw.tables.delete.call_count == 2


@patch("cdf_fn_common.discovery_raw_purge.delete_run_cohort_tables")
@patch("cdf_fn_common.discovery_raw_purge.purge_stale_run_tables")
def test_purge_inter_node_deletes_run_tables_and_sweeps_stale(
    mock_stale, mock_delete
) -> None:
    mock_delete.return_value = {"action": "delete_run_cohort_tables", "tables_deleted": 1}
    mock_stale.return_value = {"tables": []}
    client = MagicMock()
    out = purge_inter_node_cohort_tables(
        client,
        [("db", "discovery_state")],
        "run1",
        purge_stale=True,
    )
    mock_delete.assert_called_once()
    mock_stale.assert_called_once()
    assert out["purge_stale"] is True


def test_truncate_raw_tables_calls_delete() -> None:
    client = MagicMock()
    out = truncate_raw_tables(client, [("d", "t")], dry_run=False)
    client.raw.tables.delete.assert_called_once_with("d", "t")
    assert out["tables"][0]["deleted"] is True


def test_purge_stale_run_tables_deletes_old_prefix_tables() -> None:
    now = datetime.now(timezone.utc)
    stale = now - timedelta(hours=100)
    run_seg = stale.strftime("%Y%m%dT%H%M%S.%f") + "Z-000000000000"
    client = MagicMock()
    client.raw.tables.list.return_value = [
        SimpleNamespace(name=f"discovery_state__{run_seg}__tr"),
        SimpleNamespace(name="discovery_state"),
    ]
    out = purge_stale_run_tables(client, "db", retention_hours=72, dry_run=False)
    assert len(out["tables"]) >= 1


@patch("cdf_fn_common.discovery_raw_purge.collect_inter_node_cohort_tables")
@patch("cdf_fn_common.discovery_raw_purge.purge_inter_node_cohort_tables")
def test_run_discovery_raw_cleanup_default(mock_purge, mock_collect) -> None:
    mock_collect.return_value = [("d", "discovery_state")]
    mock_purge.return_value = {"action": "delete_run_cohort_tables"}
    run_discovery_raw_cleanup_action(
        MagicMock(),
        scope_document={"key_extraction": {"config": {"parameters": {"raw_db": "d", "raw_table_key": "t"}}}},
        compiled_workflow=None,
        run_id="r1",
        action="delete_run_cohort_keys",
    )
    mock_collect.assert_called_once()
    mock_purge.assert_called_once()


def test_delete_all_run_node_cohort_tables_deletes_prefixed_tables() -> None:
    client = MagicMock()
    client.raw.tables.list.return_value = [
        SimpleNamespace(name="discovery_state"),
        SimpleNamespace(name="discovery_state__20260101T120000.000000Z-abc__tr"),
        SimpleNamespace(name="discovery_inverted_index"),
        SimpleNamespace(name="other_table"),
    ]
    out = delete_all_run_node_cohort_tables(
        client, "db_discovery", base_table="discovery_state", dry_run=False
    )
    assert out["tables_deleted"] == 1
    client.raw.tables.delete.assert_called_once_with(
        "db_discovery", "discovery_state__20260101T120000.000000Z-abc__tr"
    )


@patch("cdf_fn_common.discovery_raw_purge.delete_all_run_node_cohort_tables")
@patch("cdf_fn_common.discovery_raw_purge.truncate_raw_tables")
@patch("cdf_fn_common.discovery_raw_purge.collect_discovery_raw_tables")
def test_purge_discovery_raw_baseline(mock_collect, mock_trunc, mock_delete_all) -> None:
    mock_collect.return_value = [("db_discovery", "discovery_state")]
    mock_trunc.return_value = {"action": "truncate_tables", "tables": []}
    mock_delete_all.return_value = {"action": "delete_all_run_node_cohort_tables", "tables_deleted": 2}
    out = purge_discovery_raw_baseline(MagicMock(), {}, None, dry_run=False)
    assert out["action"] == "purge_discovery_raw_baseline"
    mock_delete_all.assert_called_once()
