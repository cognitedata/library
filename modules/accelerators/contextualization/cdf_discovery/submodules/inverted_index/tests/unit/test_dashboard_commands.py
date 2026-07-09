"""Unit tests for dashboard command helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from inverted_index.config import RAW_TERM_PARTITION_POLICY
from inverted_index.raw_ops import upsert_partition_registry
from inverted_index.storage.raw_adapter import RawStorageAdapter
from inverted_index.storage.raw_keys import resolve_raw_partition_table
from local_runner.commands import (
    BATCH_DELTA_DETAIL_LIMIT,
    cmd_batch_file_deltas,
    cmd_dashboard_summary,
    cmd_registry_summary,
)


def _cfg() -> dict:
    return {
        "backend": "raw",
        "raw": {
            "database": "db_test",
            "table_template": "inverted_index__{scope_slug}",
            "registry_table": "inverted_index__registry",
        },
        "term_partition": {**RAW_TERM_PARTITION_POLICY, "enabled": True},
    }


def test_cmd_registry_summary_reads_local_registry() -> None:
    cfg = _cfg()
    adapter = RawStorageAdapter(cfg, client=None)
    scope = "site:Test|unit:U1"
    upsert_partition_registry(
        None,
        cfg,
        scope,
        resolve_raw_partition_table(scope, cfg),
        local_registry=adapter._local_registry,
        extra_columns={"ROW_COUNT_ESTIMATE": "1200", "RESHARD_IN_PROGRESS": "false"},
    )

    with patch("local_runner.commands._runtime", return_value={"storage_config": cfg}), patch(
        "local_runner.commands.create_cognite_client", return_value=None
    ), patch("local_runner.commands.get_storage_adapter", return_value=adapter):
        result = cmd_registry_summary()

    assert result["scope_count"] == 1
    assert result["scopes"][0]["match_scope_key"] == scope
    assert result["scopes"][0]["row_count_estimate"] == 1200
    assert result["scopes"][0]["reshard_in_progress"] is False


def test_cmd_registry_summary_reads_cdf_registry_when_client_connected() -> None:
    cfg = _cfg()
    adapter = RawStorageAdapter(cfg, client=MagicMock())
    mock_client = MagicMock()
    mock_client.config.project = "test-project"
    registry_columns = {
        "RECORD_KIND": "partition_registry",
        "MATCH_SCOPE_KEY": "global",
        "PARTITION_TABLE": "inverted_index__global",
        "PARTITION_STRATEGY": "unified",
        "LAST_BUILD_AT": "2026-01-01T00:00:00+00:00",
    }
    list_response = MagicMock()
    list_response.status_code = 200
    list_response.json.return_value = {
        "items": [{"key": "global", "columns": registry_columns}],
    }
    mock_client.get.return_value = list_response
    mock_client.raw.rows.retrieve.return_value = MagicMock(columns=registry_columns)

    with patch("local_runner.commands._runtime", return_value={"storage_config": cfg}), patch(
        "local_runner.commands.create_cognite_client", return_value=mock_client
    ), patch("local_runner.commands.get_storage_adapter", return_value=adapter):
        result = cmd_registry_summary()

    assert result["scope_count"] == 1
    assert result["scopes"][0]["match_scope_key"] == "global"
    mock_client.get.assert_called_once()
    assert "/raw/dbs/db_test/tables/inverted_index__registry/rows" in mock_client.get.call_args[0][0]


def test_cmd_dashboard_summary_aggregates_kpis() -> None:
    cfg = _cfg()
    adapter = RawStorageAdapter(cfg, client=None)
    scope_warn = "site:Warn|unit:U1"
    scope_ok = "site:Ok|unit:U1"

    for scope in (scope_warn, scope_ok):
        upsert_partition_registry(
            None,
            cfg,
            scope,
            resolve_raw_partition_table(scope, cfg),
            local_registry=adapter._local_registry,
        )

    partition_health = {
        "term_partition_enabled": True,
        "activate_above_rows": 400_000,
        "scopes": [
            {
                "match_scope_key": scope_warn,
                "partition_strategy": "unified",
                "partition_table": "t_warn",
                "row_count": 310_000,
            },
            {
                "match_scope_key": scope_ok,
                "partition_strategy": "unified",
                "partition_table": "t_ok",
                "row_count": 10_000,
            },
        ],
        "reshard_recommended": [scope_warn],
    }

    with patch("local_runner.commands._runtime", return_value={"storage_config": cfg}), patch(
        "local_runner.commands.create_cognite_client", return_value=None
    ), patch("local_runner.commands.get_storage_adapter", return_value=adapter), patch(
        "local_runner.commands.cmd_partition_health", return_value=partition_health
    ):
        result = cmd_dashboard_summary()

    assert result["scope_count"] == 2
    assert result["total_row_count"] == 320_000
    assert result["reshard_recommended_count"] == 1
    assert result["scopes_over_warn_threshold"] == 1
    assert result["scopes_over_critical_threshold"] == 0
    by_scope = {row["match_scope_key"]: row for row in result["scopes"]}
    assert by_scope[scope_warn]["row_status"] == "warn"
    assert by_scope[scope_ok]["row_status"] == "ok"


def test_cmd_batch_file_deltas_requires_file_ids() -> None:
    with pytest.raises(ValueError, match="at least one file id"):
        cmd_batch_file_deltas([])


def test_cmd_batch_file_deltas_aggregates_per_file_counts() -> None:
    cfg = _cfg()
    adapter = RawStorageAdapter(cfg, client=None)
    client = MagicMock()

    missing = [{"term": "P-101", "normalized_term": "p101"}]
    feedback = [{"term": "X-202", "normalized_term": "x202"}]

    with patch("local_runner.commands._runtime", return_value={"storage_config": cfg}), patch(
        "local_runner.commands.create_cognite_client", return_value=client
    ), patch("local_runner.commands.get_storage_adapter", return_value=adapter), patch(
        "local_runner.commands.get_pattern_not_in_standard_delta", return_value=missing
    ) as missing_mock, patch(
        "local_runner.commands.get_standard_not_in_pattern_delta", return_value=feedback
    ) as feedback_mock:
        result = cmd_batch_file_deltas(
            ["file-a", "file-b"],
            file_space="cdf_cdm",
            detail_limit=BATCH_DELTA_DETAIL_LIMIT,
        )

    assert result["files_scanned"] == 2
    assert result["total_missing_tags"] == 2
    assert result["total_pattern_feedback"] == 2
    assert len(result["by_file"]) == 2
    assert result["by_file"][0]["file_external_id"] == "file-a"
    assert result["by_file"][0]["missing_tags_count"] == 1
    assert missing_mock.call_count == 2
    assert feedback_mock.call_count == 2
