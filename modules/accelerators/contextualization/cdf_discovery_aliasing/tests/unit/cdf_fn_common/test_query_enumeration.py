"""Tests for shared query enumeration helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

from cdf_fn_common.query_enumeration import (
    SQL_PREVIEW_MAX_ROWS,
    QueryEnumerationStats,
    list_all_classic_resources,
    mark_truncated,
    resolve_classic_list_limit,
    resolve_page_size,
    resolve_read_limit,
    resolve_sql_row_limit,
)


def test_resolve_page_size_defaults_and_caps() -> None:
    assert resolve_page_size({}) == 1000
    assert resolve_page_size({"batch_size": 500}) == 500
    assert resolve_page_size({"batch_size": 5000}) == 1000
    assert resolve_page_size({"batch_size": 0}) == 1000


def test_resolve_read_limit_zero_means_unlimited() -> None:
    assert resolve_read_limit({}) == 0
    assert resolve_read_limit({"read_limit": 0}) == 0
    assert resolve_read_limit({"read_limit": 25}) == 25


def test_resolve_sql_row_limit() -> None:
    assert resolve_sql_row_limit({}) == SQL_PREVIEW_MAX_ROWS
    assert resolve_sql_row_limit({"limit": 0}) == SQL_PREVIEW_MAX_ROWS
    assert resolve_sql_row_limit({"limit": 50}) == 50
    assert resolve_sql_row_limit({"limit": 99_999}) == SQL_PREVIEW_MAX_ROWS


def test_resolve_classic_list_limit() -> None:
    assert resolve_classic_list_limit({}) == -1
    assert resolve_classic_list_limit({"read_limit": 10}) == 10


def test_list_all_classic_resources_plain_list() -> None:
    class _Asset:
        external_id = "A1"

    client = MagicMock()
    client.assets.list.return_value = [_Asset()]
    stats = QueryEnumerationStats()
    items = list(list_all_classic_resources(client, "assets", limit=-1, stats_out=stats))
    assert len(items) == 1
    client.assets.list.assert_called_once_with(limit=-1)
    assert stats.rows_read == 1


def test_mark_truncated() -> None:
    stats = QueryEnumerationStats()
    mark_truncated(stats, reason="read_limit")
    assert stats.rows_truncated is True
    assert stats.truncation_reason == "read_limit"
    assert stats.list_complete is False
