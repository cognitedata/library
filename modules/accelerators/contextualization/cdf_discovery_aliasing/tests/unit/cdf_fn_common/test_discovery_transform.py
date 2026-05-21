"""Unit tests for discovery transform v1 handlers."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.discovery_transform import (  # noqa: E402
    ASSET_TAG_FROM_NAME_REGEX,
    apply_leading_zero_normalize,
    apply_regex_substitution,
    apply_sequential_literal_replace,
    apply_substitution_variants,
    extract_field_values,
    transform_row_properties,
    validate_transform_config,
)


def test_extract_field_values_regex_options_ignore_case() -> None:
    out = extract_field_values(
        {"name": "P-abc"},
        [
            {
                "field_name": "name",
                "regex": r"p-([a-z]+)",
                "regex_options": {"ignore_case": True},
            }
        ],
    )
    assert out["name"] == "P-abc"


def test_extract_field_values_max_matches_and_join() -> None:
    out = extract_field_values(
        {"t": "a-b-c-d"},
        [
            {
                "field_name": "t",
                "regex": r"[a-z]",
                "max_matches_per_field": 3,
                "regex_options": {"match_join": "|"},
            }
        ],
    )
    assert out["t"] == "a|b|c"


def test_extract_field_values_priority_last_wins() -> None:
    out = extract_field_values(
        {"name": "PREFIX-TAG"},
        [
            {"field_name": "name", "regex": "PREFIX", "priority": 2},
            {"field_name": "name", "regex": "TAG", "priority": 1},
        ],
    )
    assert out["name"] == "TAG"


def test_regex_substitution_patterns() -> None:
    block = {
        "patterns": [
            {"pattern": r"^(\d+)-", "replacement": ""},
            {"pattern": r"OLD", "replacement": "NEW"},
        ]
    }
    assert apply_regex_substitution("10-P-OLD", block) == "P-NEW"


def test_leading_zero_normalize_digit_runs() -> None:
    assert apply_leading_zero_normalize("TAG-00012-A", {}) == "TAG-12-A"
    assert apply_leading_zero_normalize("000", {}) == "0"


def test_sequential_literal_replace_ordered() -> None:
    block = {"replacements": [{"from": "FIC", "to": "FT"}, {"from": "FT", "to": "FLOW"}]}
    assert apply_sequential_literal_replace("FIC-1234", block) == "FLOW-1234"


def test_substitution_variants_literal_prefix() -> None:
    block = {"match_literal": "FIC", "variants": ["FIC", "FI", "FT", "FC"]}
    out = apply_substitution_variants("FIC-1234", block)
    assert out == ["FIC-1234", "FI-1234", "FT-1234", "FC-1234"]


def test_substitution_variants_rejects_duplicates() -> None:
    with pytest.raises(ValueError, match="duplicates"):
        validate_transform_config(
            {
                "handler_id": "substitution_variants",
                "substitution_variants": {"match_literal": "FIC", "variants": ["FI", "FI"]},
            }
        )


def test_transform_row_properties_template_and_output_field() -> None:
    cfg = {
        "handler_id": "regex_substitution",
        "fields": [{"field_name": "tags"}],
        "output_template": "{tags}",
        "output_field": "indexKey",
        "output_mode": "overwrite",
        "regex_substitution": {"pattern": r"^asset_", "replacement": ""},
    }
    rows = transform_row_properties({"tags": "asset_P-001"}, cfg)
    assert len(rows) == 1
    assert rows[0]["indexKey"] == "P-001"


def test_split_string_defaults_to_array_json_when_omitted() -> None:
    cfg = {
        "handler_id": "split_string",
        "fields": [{"field_name": "tags"}],
        "output_template": "{tags}",
        "output_field": "parts",
        "output_mode": "overwrite",
        "split_string": {"delimiter": ",", "trim": True},
    }
    rows = transform_row_properties({"tags": "a,b,c"}, cfg)
    assert len(rows) == 1
    assert rows[0]["parts"] == ["a", "b", "c"]


def test_transform_row_properties_substitution_variants_explode() -> None:
    cfg = {
        "handler_id": "substitution_variants",
        "fields": [{"field_name": "name"}],
        "output_template": "{name}",
        "output_field": "variants",
        "output_mode": "overwrite",
        "output_multi_value": "explode_rows",
        "substitution_variants": {
            "match_literal": "FIC",
            "variants": ["FIC", "FI", "FT"],
        },
    }
    rows = transform_row_properties({"name": "FIC-1234"}, cfg)
    assert [r["variants"] for r in rows] == ["FIC-1234", "FI-1234", "FT-1234"]


def test_extract_asset_tag_from_cognite_asset_name() -> None:
    cfg = {
        "handler_id": "regex_substitution",
        "fields": [{"field_name": "name", "regex": ASSET_TAG_FROM_NAME_REGEX}],
        "output_template": "{name}",
        "output_field": "assetTag",
        "output_mode": "overwrite",
        "regex_substitution": {"patterns": []},
    }
    rows = transform_row_properties(
        {"name": "Centrifugal pump P-1234A (unit 10)", "externalId": "asset-1"},
        cfg,
    )
    assert len(rows) == 1
    assert rows[0]["assetTag"] == "P-1234A"
    assert rows[0]["name"] == "Centrifugal pump P-1234A (unit 10)"

    rows2 = transform_row_properties({"name": "AREA_P-5678"}, cfg)
    assert rows2[0]["assetTag"] == "P-5678"

    rows3 = transform_row_properties({"name": "No instrument tag"}, cfg)
    assert rows3[0]["assetTag"] == ""


def test_output_field_type_coerces_to_int() -> None:
    cfg = {
        "handler_id": "regex_substitution",
        "fields": [{"field_name": "t", "regex": r"(\d+)"}],
        "output_template": "{t}",
        "output_field": "n",
        "output_mode": "overwrite",
        "output_field_type": "int",
        "regex_substitution": {"patterns": [{"pattern": r"^(\d+)$", "replacement": "7"}]},
    }
    rows = transform_row_properties({"t": "42"}, cfg)
    assert rows[0]["n"] == 7


def test_multifield_working_comma_join_then_heuristic_sampler() -> None:
    cfg = {
        "handler_id": "heuristic_sampler",
        "fields": [{"field_name": "unit"}, {"field_name": "desc"}],
        "output_field": "picked",
        "output_mode": "overwrite",
        "heuristic_sampler": {"samples": ["P-101", "P-10"]},
    }
    rows = transform_row_properties(
        {"unit": "AREA-A", "desc": "See P-101 and P-10 on line"},
        cfg,
    )
    assert len(rows) == 1
    assert rows[0]["picked"] == "P-101"


def test_validate_heuristic_sampler_pattern_without_samples() -> None:
    validate_transform_config(
        {
            "handler_id": "heuristic_sampler",
            "fields": [{"field_name": "x"}],
            "output_template": "{x}",
            "output_field": "o",
            "output_mode": "overwrite",
            "heuristic_sampler": {"pattern": r"P-\d+"},
        }
    )

