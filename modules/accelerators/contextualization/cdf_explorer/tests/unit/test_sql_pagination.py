"""Unit tests for SQL result pagination helpers (UI utils mirrored in Python for logic)."""

from __future__ import annotations

import math


def sql_page_count(total_items: int, page_size: int) -> int:
    if total_items <= 0:
        return 1
    return max(1, math.ceil(total_items / page_size))


def sql_page_items(items: list, page_index: int, page_size: int) -> list:
    start = page_index * page_size
    return items[start : start + page_size]


def test_sql_page_count_and_slice():
    items = list(range(250))
    assert sql_page_count(250, 100) == 3
    assert sql_page_items(items, 0, 100) == list(range(100))
    assert sql_page_items(items, 2, 100) == list(range(200, 250))
    assert sql_page_count(0, 100) == 1
