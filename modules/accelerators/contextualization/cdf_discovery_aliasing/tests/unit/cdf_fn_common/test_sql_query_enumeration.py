"""SQL query default limit and truncation flags."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from cdf_fn_common.query_enumeration import SQL_PREVIEW_MAX_ROWS
from fn_dm_sql_query.engine.orchestration import discovery_handle_sql_query


def test_sql_query_default_limit_is_max() -> None:
    client = MagicMock()
    client.raw.rows.insert = MagicMock()

    preview_result = {
        "items": [{"external_id": f"r{i}"} for i in range(SQL_PREVIEW_MAX_ROWS)],
        "row_count": SQL_PREVIEW_MAX_ROWS,
    }

    with patch(
        "fn_dm_sql_query.engine.handlers.sql_query.run_sql_preview",
        return_value=preview_result,
    ) as mock_preview:
        data = {
            "task_id": "kea__sq",
            "run_id": "run_sql",
            "config": {"sql_query": "SELECT 1"},
        }
        summary = discovery_handle_sql_query("fn_dm_sql_query", data, client, None)

    mock_preview.assert_called_once()
    assert mock_preview.call_args.kwargs["limit"] == SQL_PREVIEW_MAX_ROWS
    assert summary["instances_written"] == SQL_PREVIEW_MAX_ROWS
    assert summary["rows_truncated"] is True
    assert summary["truncation_reason"] == "sql_preview_max"


def test_sql_query_explicit_limit_truncation() -> None:
    client = MagicMock()
    client.raw.rows.insert = MagicMock()

    with patch(
        "fn_dm_sql_query.engine.handlers.sql_query.run_sql_preview",
        return_value={"items": [{"id": "1"}, {"id": "2"}], "row_count": 2},
    ) as mock_preview:
        data = {
            "task_id": "kea__sq",
            "run_id": "run_sql",
            "config": {"sql_query": "SELECT 1", "limit": 2},
        }
        summary = discovery_handle_sql_query("fn_dm_sql_query", data, client, None)

    assert mock_preview.call_args.kwargs["limit"] == 2
    assert summary["rows_truncated"] is True
    assert summary["truncation_reason"] == "limit"
