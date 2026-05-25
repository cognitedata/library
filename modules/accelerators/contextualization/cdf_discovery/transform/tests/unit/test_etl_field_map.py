"""Unit tests for etl_field_map."""

from __future__ import annotations

import pytest

from cdf_fn_common.etl_field_map import (
    apply_field_mappings,
    parse_field_mappings,
    validate_field_map_config,
)


def test_apply_field_mappings_projects_and_preserves_identity() -> None:
    props = {
        "externalId": "a1",
        "space": "s1",
        "name": "Pump-101",
        "description": "ignored",
    }
    mappings = [("name", "indexKey")]
    out = apply_field_mappings(props, mappings)
    assert out["externalId"] == "a1"
    assert out["space"] == "s1"
    assert out["indexKey"] == "Pump-101"
    assert "name" not in out
    assert "description" not in out


def test_parse_field_mappings_rejects_duplicate_outputs() -> None:
    cfg = {
        "mappings": [
            {"input_field": "a", "output_field": "x"},
            {"input_field": "b", "output_field": "x"},
        ]
    }
    with pytest.raises(ValueError, match="duplicate output_field"):
        parse_field_mappings(cfg)


def test_validate_field_map_config_requires_nonempty_mappings() -> None:
    with pytest.raises(ValueError, match="non-empty array"):
        validate_field_map_config({})
    with pytest.raises(ValueError, match="at least one"):
        validate_field_map_config({"mappings": [{"input_field": "", "output_field": ""}]})


def test_apply_field_mappings_skips_missing_input() -> None:
    props = {"externalId": "a1", "name": "x"}
    out = apply_field_mappings(props, [("missing", "out"), ("name", "indexKey")])
    assert out["indexKey"] == "x"
    assert "out" not in out
