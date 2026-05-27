"""Unit tests for ETL RAW cleanup (etl_raw_purge)."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.etl_cohort_storage import list_run_cohort_tables  # noqa: E402
from cdf_fn_common.etl_incremental_scope import incremental_state_table_name  # noqa: E402
from cdf_fn_common.etl_raw_purge import (  # noqa: E402
    collect_etl_cohort_bases,
    delete_run_cohort_tables,
    purge_inter_node_cohort_tables,
    purge_stale_run_tables,
    run_etl_raw_cleanup_action,
)
from fn_etl_raw_cleanup.handler import etl_handle_raw_cleanup  # noqa: E402


def test_collect_etl_cohort_bases_merges_parameters_and_tasks() -> None:
    scope = {
        "parameters": {"raw_db": "etl_staging", "raw_table_key": "cohort"},
        "key_extraction": {
            "config": {
                "parameters": {"raw_db": "db_a", "raw_table_key": "t_main"},
            }
        },
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
    tables = collect_etl_cohort_bases(scope, cw)
    assert ("db_a", "t_main") in tables
    assert ("db_b", "t_sink") in tables
    assert ("etl_staging", "cohort") in tables


def test_incremental_table_not_matched_by_run_cohort_prefix() -> None:
    inc = incremental_state_table_name("discovery_state")
    assert inc == "discovery_state__incremental"
    client = MagicMock()
    client.raw.tables.list.return_value = [
        SimpleNamespace(name="discovery_state__abc123def456__tr"),
        SimpleNamespace(name=inc),
        SimpleNamespace(name="discovery_state__abc123def456__vq"),
    ]
    listed = list_run_cohort_tables(client, "db", "abc123def456", base_table="discovery_state")
    assert inc not in listed


@patch("cdf_fn_common.etl_raw_purge.list_run_cohort_tables")
def test_delete_run_cohort_tables_drops_all_node_tables(mock_list) -> None:
    mock_list.return_value = [
        "cohort__run1__tr",
        "cohort__run1__vq",
    ]
    client = MagicMock()
    out = delete_run_cohort_tables(client, "db", "run1", base_table="cohort", dry_run=False)
    assert out["action"] == "delete_run_cohort_tables"
    assert out["tables_deleted"] == 2
    assert client.raw.tables.delete.call_count == 2


@patch("cdf_fn_common.etl_raw_purge.delete_run_cohort_tables")
@patch("cdf_fn_common.etl_raw_purge.purge_stale_run_tables")
def test_purge_inter_node_loops_bases(mock_stale, mock_delete) -> None:
    mock_delete.return_value = {"action": "delete_run_cohort_tables", "tables_deleted": 1}
    mock_stale.return_value = {"tables": []}
    client = MagicMock()
    out = purge_inter_node_cohort_tables(
        client,
        [("db1", "cohort"), ("db2", "state")],
        "run1",
        purge_stale=True,
    )
    assert mock_delete.call_count == 2
    assert mock_stale.call_count == 2
    assert out["tables_deleted"] == 2
    assert out["purge_stale"] is True


def test_purge_stale_run_tables_deletes_old_prefix_tables() -> None:
    now = datetime.now(timezone.utc)
    stale = now - timedelta(hours=100)
    run_seg = stale.strftime("%Y%m%dT%H%M%S.%f") + "Z-000000000000"
    client = MagicMock()
    client.raw.tables.list.return_value = [
        SimpleNamespace(name=f"cohort__{run_seg}__tr"),
        SimpleNamespace(name="cohort"),
    ]
    out = purge_stale_run_tables(client, "db", base_table="cohort", retention_hours=72, dry_run=False)
    assert len(out["tables"]) >= 1


@patch("cdf_fn_common.etl_raw_purge.collect_etl_cohort_bases")
@patch("cdf_fn_common.etl_raw_purge.purge_inter_node_cohort_tables")
def test_run_etl_raw_cleanup_action(mock_purge, mock_collect) -> None:
    mock_collect.return_value = [("d", "cohort")]
    mock_purge.return_value = {"action": "delete_run_cohort_keys", "tables_deleted": 0}
    run_etl_raw_cleanup_action(
        MagicMock(),
        scope_document={"parameters": {"raw_db": "d", "raw_table_key": "cohort"}},
        compiled_workflow=None,
        run_id="r1",
    )
    mock_collect.assert_called_once()
    mock_purge.assert_called_once()


@patch("fn_etl_raw_cleanup.handler.run_etl_raw_cleanup_action")
def test_handler_returns_ok(mock_action) -> None:
    mock_action.return_value = {"action": "delete_run_cohort_keys", "tables_deleted": 2}
    data = {
        "run_id": "20260101T120000.000000Z-abc123456789",
        "task_id": "end",
        "configuration": {"parameters": {"raw_db": "db", "raw_table_key": "cohort"}},
    }
    out = etl_handle_raw_cleanup("fn_etl_raw_cleanup", data, MagicMock(), None)
    assert out["status"] == "ok"
    assert out["tables_deleted"] == 2
    assert "reason" not in out


def test_handler_dry_run_ok_without_reason() -> None:
    out = etl_handle_raw_cleanup(
        "fn_etl_raw_cleanup",
        {"run_id": "r1", "dry_run": True},
        None,
        None,
    )
    assert out["status"] == "ok"
    assert out["dry_run"] is True
    assert "reason" not in out


def test_workflow_policy_cleanup_skip_task() -> None:
    from cdf_fn_common.workflow_task_policy import discovery_task_workflow_policy

    pol = discovery_task_workflow_policy("fn_etl_raw_cleanup")
    assert pol["onFailure"] == "skipTask"
    assert pol["timeout"] == 1800
