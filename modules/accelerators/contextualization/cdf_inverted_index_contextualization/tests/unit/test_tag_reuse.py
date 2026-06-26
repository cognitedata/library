"""Unit tests for tag reuse analytics."""

from inverted_index.tag_reuse import audit_cross_scope_tags, summarize_tag_scope_reuse


def test_summarize_tag_scope_reuse_single_scope() -> None:
    hits = [
        {
            "term": "P-101A",
            "normalized_term": "p101a",
            "match_scope_key": "site:Test|unit:U100",
            "reference_external_id": "EQ-1",
            "source_property": "name",
            "source_type": "asset_metadata",
        }
    ]
    summary = summarize_tag_scope_reuse(
        hits,
        terms_queried=["p101a"],
        scopes_queried=["site:Test|unit:U100"],
    )
    assert summary["terms_with_hits"] == 1
    assert summary["cross_scope_duplicate_count"] == 0
    assert summary["by_term"][0]["cross_scope_duplicate"] is False


def test_summarize_tag_scope_reuse_cross_scope() -> None:
    hits = [
        {
            "term": "P-101A",
            "normalized_term": "p101a",
            "match_scope_key": "site:Test|unit:U100",
            "reference_external_id": "EQ-1",
            "source_property": "name",
            "source_type": "asset_metadata",
        },
        {
            "term": "P-101A",
            "normalized_term": "p101a",
            "match_scope_key": "site:Test|unit:U200",
            "reference_external_id": "EQ-2",
            "source_property": "name",
            "source_type": "asset_metadata",
        },
    ]
    summary = summarize_tag_scope_reuse(
        hits,
        terms_queried=["p101a"],
        scopes_queried=["site:Test|unit:U100", "site:Test|unit:U200"],
    )
    assert summary["cross_scope_duplicate_count"] == 1
    assert summary["cross_scope_duplicate_rate"] == 1.0
    row = summary["by_term"][0]
    assert row["scope_count"] == 2
    assert row["cross_scope_duplicate"] is True


def test_summarize_tag_scope_reuse_only_filter() -> None:
    hits = [
        {
            "term": "P-101A",
            "normalized_term": "p101a",
            "match_scope_key": "site:Test|unit:U100",
            "reference_external_id": "EQ-1",
            "source_property": "name",
            "source_type": "asset_metadata",
        },
        {
            "term": "P-102B",
            "normalized_term": "p102b",
            "match_scope_key": "site:Test|unit:U100",
            "reference_external_id": "EQ-2",
            "source_property": "name",
            "source_type": "asset_metadata",
        },
    ]
    summary = summarize_tag_scope_reuse(hits, reuse_only=True)
    assert summary["by_term"] == []


def test_summarize_tag_scope_reuse_empty_hits() -> None:
    summary = summarize_tag_scope_reuse([], terms_queried=["p101a"])
    assert summary["terms_with_hits"] == 0
    assert summary["cross_scope_duplicate_count"] == 0
    assert summary["by_term"] == []
