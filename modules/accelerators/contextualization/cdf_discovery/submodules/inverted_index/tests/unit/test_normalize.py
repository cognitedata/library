"""Unit tests for term normalization."""

import pytest

from inverted_index.normalize import normalize_query_terms, normalize_term


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("P-101A", "p101a"),
        ("p-101a", "p101a"),
        ("21-PT-1017", "21pt1017"),
        ("", ""),
        ("   ", ""),
        ("Pump 101", "pump101"),
        ("ポンプ-101", "ポンプ101"),
        ("ポンプP-101", "ポンプp101"),
        ("バルブ", "バルブ"),
    ],
)
def test_normalize_term(raw: str, expected: str) -> None:
    assert normalize_term(raw) == expected


def test_normalize_query_terms_dedupes_and_normalizes() -> None:
    assert normalize_query_terms(["P-101A", "p-101a", "P-102B"]) == ["p101a", "p102b"]
    assert normalize_query_terms(["", "   "]) == []
