"""Unit tests for RAW rows REST client."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from inverted_index.raw_rest import RawRowsAPIError, iter_raw_rows, list_raw_rows_page


def _mock_client() -> MagicMock:
    client = MagicMock()
    client.config.project = "test-project"
    return client


def _response(status: int, body: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = body
    resp.text = str(body)
    return resp


def test_list_raw_rows_page_builds_path_and_params() -> None:
    client = _mock_client()
    client.get.return_value = _response(
        200,
        {"items": [{"key": "k1", "columns": {"A": "1"}}], "nextCursor": "cur-2"},
    )

    page = list_raw_rows_page(client, "db_a", "table_b", limit=500, cursor="cur-1")

    assert page["nextCursor"] == "cur-2"
    client.get.assert_called_once()
    url, kwargs = client.get.call_args[0][0], client.get.call_args[1]
    assert url == "/api/v1/projects/test-project/raw/dbs/db_a/tables/table_b/rows"
    assert kwargs["params"] == {"limit": 500, "cursor": "cur-1"}


def test_iter_raw_rows_follows_next_cursor() -> None:
    client = _mock_client()
    client.get.side_effect = [
        _response(
            200,
            {
                "items": [{"key": "k1", "columns": {}}, {"key": "k2", "columns": {}}],
                "nextCursor": "page-2",
            },
        ),
        _response(
            200,
            {"items": [{"key": "k3", "columns": {}}]},
        ),
    ]

    keys = [row["key"] for row in iter_raw_rows(client, "db", "tbl", page_size=2)]

    assert keys == ["k1", "k2", "k3"]
    assert client.get.call_count == 2
    second_params = client.get.call_args_list[1][1]["params"]
    assert second_params["cursor"] == "page-2"
    assert second_params["limit"] == 2


def test_list_raw_rows_page_raises_on_http_error() -> None:
    client = _mock_client()
    client.get.return_value = _response(403, {"error": "forbidden"})

    with pytest.raises(RawRowsAPIError, match="HTTP 403"):
        list_raw_rows_page(client, "db", "tbl")
