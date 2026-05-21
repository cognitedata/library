"""Unit tests for cohort filter evaluation."""

from __future__ import annotations

import pytest

from cdf_fn_common.cohort_filter_eval import (
    filter_node_matches,
    parse_cohort_filters,
    row_matches_filters,
    validate_cohort_filters_config,
)


def test_row_matches_exists_or_group() -> None:
    filters = [
        {
            "or": [
                {"operator": "EXISTS", "target_property": "aliases", "property_scope": "view"},
                {"operator": "EXISTS", "target_property": "indexKey", "property_scope": "view"},
            ]
        }
    ]
    props_fail: dict = {}
    props_ok = {"indexKey": [{"value": "k", "confidence": 0.5}]}
    assert row_matches_filters(props_fail, filters) is False
    assert row_matches_filters(props_ok, filters) is True


def test_row_matches_in_operator() -> None:
    filters = [
        {
            "operator": "IN",
            "target_property": "aliases",
            "property_scope": "view",
            "values": ["tag-a"],
        }
    ]
    props = {
        "aliases": [
            {"value": "tag-b", "confidence": 0.5},
            {"value": "tag-a", "confidence": 0.85},
        ]
    }
    assert row_matches_filters(props, filters) is True


def test_row_matches_and_group() -> None:
    filters = [
        {
            "and": [
                {"operator": "EXISTS", "target_property": "aliases"},
                {"operator": "EXISTS", "target_property": "indexKey"},
            ]
        }
    ]
    props = {
        "aliases": [{"value": "a", "confidence": 0.9}],
    }
    assert row_matches_filters(props, filters) is False


def test_negate_leaf() -> None:
    filt = {
        "operator": "EQUALS",
        "target_property": "status",
        "values": ["deprecated"],
        "negate": True,
    }
    assert filter_node_matches({"status": "active"}, filt) is True
    assert filter_node_matches({"status": "deprecated"}, filt) is False


def test_parse_cohort_filters() -> None:
    cfg = {"filters": [{"operator": "EXISTS", "target_property": "aliases"}]}
    parsed = parse_cohort_filters(cfg)
    assert len(parsed) == 1
    assert parsed[0]["target_property"] == "aliases"


def test_gte_on_confidence_property_list() -> None:
    props = {"aliases": ["a"], "confidence": [0.9, 0.3]}
    filters = [
        {
            "operator": "GTE",
            "target_property": "confidence",
            "property_scope": "view",
            "values": [0.8],
        }
    ]
    assert row_matches_filters(props, filters) is True
    assert row_matches_filters({"confidence": [0.2]}, filters) is False


def test_comparison_operators() -> None:
    props = {"score": 0.75}
    assert row_matches_filters(
        props,
        [{"operator": "GTE", "target_property": "score", "values": [0.5]}],
    )
    assert row_matches_filters(
        props,
        [{"operator": ">=", "target_property": "score", "values": [0.5]}],
    )
    assert not row_matches_filters(
        props,
        [{"operator": "LT", "target_property": "score", "values": [0.5]}],
    )


def test_exists_on_dot_path_property() -> None:
    filters = [
        {
            "operator": "EXISTS",
            "target_property": "raw_columns.key",
            "property_scope": "view",
        }
    ]
    props = {"raw_columns": {"key": "P-101"}}
    assert row_matches_filters(props, filters) is True
    assert row_matches_filters({}, filters) is False


def test_validate_requires_filters() -> None:
    with pytest.raises(ValueError, match="filters"):
        validate_cohort_filters_config({})
    validate_cohort_filters_config(
        {"filters": [{"operator": "EXISTS", "target_property": "aliases"}]}
    )
