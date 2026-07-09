"""Unit tests for RAW ops list scanners via REST pagination."""

from __future__ import annotations

from unittest.mock import MagicMock

from inverted_index.config import INDEX_STORAGE_CONFIG
from inverted_index.raw_ops import count_partition_table_rows, list_registered_scope_keys


def _cfg() -> dict:
    return {
        **INDEX_STORAGE_CONFIG,
        "backend": "raw",
        "raw": {
            "database": "db_test",
            "table_template": "inverted_index__{scope_slug}",
            "registry_table": "inverted_index__registry",
        },
    }


def _mock_client() -> MagicMock:
    client = MagicMock()
    client.config.project = "test-project"
    return client


def _response(body: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = body
    return resp


def test_list_registered_scope_keys_via_rest() -> None:
    client = _mock_client()
    client.get.return_value = _response(
        {
            "items": [
                {
                    "key": "global",
                    "columns": {
                        "RECORD_KIND": "partition_registry",
                        "MATCH_SCOPE_KEY": "global",
                    },
                },
                {
                    "key": "other",
                    "columns": {"RECORD_KIND": "other_kind", "MATCH_SCOPE_KEY": "ignored"},
                },
                {
                    "key": "site:A|unit:U1",
                    "columns": {
                        "RECORD_KIND": "partition_registry",
                        "MATCH_SCOPE_KEY": "site:A|unit:U1",
                    },
                },
            ],
        }
    )

    scopes = list_registered_scope_keys(client, _cfg())

    assert scopes == ["global", "site:A|unit:U1"]
    client.get.assert_called_once()


def test_count_partition_table_rows_across_pages() -> None:
    client = _mock_client()
    client.get.side_effect = [
        _response(
            {
                "items": [{"key": "a", "columns": {}}, {"key": "b", "columns": {}}],
                "nextCursor": "p2",
            }
        ),
        _response({"items": [{"key": "c", "columns": {}}]}),
    ]

    total = count_partition_table_rows(client, "db_test", "inverted_index__global")

    assert total == 3
    assert client.get.call_count == 2
