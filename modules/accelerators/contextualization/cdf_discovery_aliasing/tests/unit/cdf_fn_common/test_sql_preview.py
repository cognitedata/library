"""Unit tests for SQL preview helpers."""

from __future__ import annotations

from cdf_fn_common.sql_preview import resolve_sql_row_external_id


def test_resolve_sql_row_external_id_explicit_column() -> None:
    row = {"my_id": "abc", "name": "n1"}
    assert resolve_sql_row_external_id(row, "my_id") == "abc"


def test_resolve_sql_row_external_id_heuristic() -> None:
    row = {"externalId": "e1", "other": 1}
    assert resolve_sql_row_external_id(row, "") == "e1"


def test_resolve_sql_row_external_id_missing() -> None:
    assert resolve_sql_row_external_id({"foo": "bar"}, "") == ""
