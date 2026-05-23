"""Unit tests for SQL grid sort helpers (mirrors ui/src/utils/sqlGridSort.ts)."""

from __future__ import annotations

import json
from typing import Any


def _sort_rank(value: Any) -> float | str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        trimmed = value.strip()
        try:
            if trimmed != "":
                return float(trimmed)
        except ValueError:
            pass
        return trimmed.lower()
    return json.dumps(value, sort_keys=True).lower()


def sort_rows(items: list[dict], column: str, direction: str) -> list[dict]:
    def key(row: dict) -> tuple:
        rank = _sort_rank(row.get(column))
        return (rank is None, rank)

    return sorted(items, key=key, reverse=direction == "desc")


def next_sort(current: dict | None, column: str) -> dict | None:
    if not current or current["column"] != column:
        return {"column": column, "direction": "asc"}
    if current["direction"] == "asc":
        return {"column": column, "direction": "desc"}
    return None


def test_sort_numeric_strings_asc():
    rows = [{"n": "10"}, {"n": None}, {"n": "2"}, {"n": "100"}]
    out = sort_rows(rows, "n", "asc")
    assert [r["n"] for r in out] == ["2", "10", "100", None]


def test_sort_desc():
    rows = [{"k": "b"}, {"k": "a"}, {"k": "c"}]
    out = sort_rows(rows, "k", "desc")
    assert [r["k"] for r in out] == ["c", "b", "a"]


def test_next_sort_cycle():
    assert next_sort(None, "a") == {"column": "a", "direction": "asc"}
    assert next_sort({"column": "a", "direction": "asc"}, "a") == {"column": "a", "direction": "desc"}
    assert next_sort({"column": "a", "direction": "desc"}, "a") is None
