"""Unit tests for discovery transform ELT catalog handlers."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.discovery_transform import (  # noqa: E402
    apply_change_case,
    apply_coerce_scalar,
    apply_default_if_empty,
    apply_format_datetime,
    apply_hash_stable,
    apply_heuristic_sampler,
    apply_mask_string,
    apply_parse_json_extract,
    apply_split_join,
    apply_split_string,
    apply_static_lookup_map,
    apply_trim_whitespace,
    transform_row_properties,
    validate_transform_config,
)


def test_trim_whitespace_modes() -> None:
    assert apply_trim_whitespace("  a  b  ", {"mode": "ends_only"}) == "a  b"
    assert apply_trim_whitespace("  a  b  ", {"mode": "collapse_internal"}) == "a b"


def test_change_case() -> None:
    assert apply_change_case("AbC", {"case": "upper"}) == "ABC"
    assert apply_change_case("hello world", {"case": "title"}) == "Hello World"


def test_coerce_scalar_lenient_and_strict() -> None:
    assert apply_coerce_scalar("42", {"type": "int"}) == 42
    assert apply_coerce_scalar("", {"type": "int", "empty_as_null": True}) is None
    with pytest.raises(ValueError, match="strict"):
        apply_coerce_scalar("x", {"type": "int", "strict": True})


def test_default_if_empty() -> None:
    assert apply_default_if_empty("", {"literal": "N/A"}) == "N/A"
    assert apply_default_if_empty("ok", {"literal": "N/A"}) == "ok"
    assert apply_default_if_empty("", {"field": "name"}, props={"name": "fallback"}) == "fallback"


def test_split_join_template_reassembles_tokens() -> None:
    block = {"delimiter": "-", "trim": True, "template": "{3}-{4}"}
    assert apply_split_join("TS-UNIT-A-FIC-1001-VALUE", block) == "FIC-1001"


def test_split_join_indexes_and_join() -> None:
    block = {"delimiter": "-", "indexes": [3, 4], "join": "-"}
    assert apply_split_join("TS-UNIT-A-FIC-1001-VALUE", block) == "FIC-1001"


def test_split_join_indexes_from_comma_string() -> None:
    block = {"delimiter": "-", "indexes": "3, 4", "join": "-"}
    assert apply_split_join("TS-UNIT-A-FIC-1001-VALUE", block) == "FIC-1001"


def test_split_join_indexes_prefer_over_template() -> None:
    block = {"delimiter": "-", "template": "{0}-{1}", "indexes": [3, 4], "join": "-"}
    assert apply_split_join("TS-UNIT-A-FIC-1001-VALUE", block) == "FIC-1001"


def test_split_join_negative_index() -> None:
    block = {"delimiter": "-", "template": "{-2}-{1}"}
    assert apply_split_join("a-b-c-d-e", block) == "d-b"


def test_split_join_row_transform() -> None:
    cfg = {
        "handler_id": "split_join",
        "fields": [{"field_name": "externalId"}],
        "output_template": "{externalId}",
        "output_field": "indexKey",
        "output_mode": "overwrite",
        "split_join": {"delimiter": "-", "template": "{3}-{4}"},
    }
    rows = transform_row_properties(
        {"externalId": "TS-UNIT-A-FIC-1001-VALUE"},
        cfg,
    )
    assert rows[0]["indexKey"] == "FIC-1001"


def test_validate_split_join_requires_template_or_indexes() -> None:
    with pytest.raises(ValueError, match="template"):
        validate_transform_config(_minimal_transform_cfg("split_join", {"delimiter": "-"}))


def test_split_join_mixed_delimiters_via_delimiters_list() -> None:
    block = {
        "delimiters": [".", "/", "-", "_"],
        "template": "{2}-{3}",
    }
    assert apply_split_join("PlantA.Unit1/FIC-101.PV", block) == "FIC-101"


def test_split_join_mixed_delimiters_via_regex() -> None:
    block = {"delimiter_regex": r"[./_-]+", "template": "{2}-{3}"}
    assert apply_split_join("PlantA.Unit1/FIC-101.PV", block) == "FIC-101"


def test_split_join_colon_delimiter_via_regex() -> None:
    block = {"delimiter_regex": r"[-./_:]+", "indexes": [2, 3], "join": "-"}
    assert apply_split_join("PlantA:Unit1/FIC-101.PV", block) == "FIC-101"


def test_split_join_colon_delimiter_via_delimiters_list() -> None:
    block = {
        "delimiters": [".", "/", "-", "_", ":"],
        "indexes": [2, 3],
        "join": "-",
    }
    assert apply_split_join("PlantA:Unit1/FIC-101.PV", block) == "FIC-101"


def test_split_string_mixed_delimiters() -> None:
    from cdf_fn_common.discovery_transform import apply_split_string

    parts = apply_split_string("a.b/c-d", {"delimiters": [".", "/", "-"]})
    assert parts == ["a", "b", "c", "d"]


def test_validate_split_parts_rejects_bad_regex() -> None:
    with pytest.raises(ValueError, match="delimiter_regex"):
        validate_transform_config(
            _minimal_transform_cfg("split_string", {"delimiter_regex": "[invalid"})
        )


def test_split_string_explode_rows() -> None:
    cfg = {
        "handler_id": "split_string",
        "fields": [{"field_name": "tags"}],
        "output_template": "{tags}",
        "output_field": "parts",
        "output_mode": "overwrite",
        "output_multi_value": "explode_rows",
        "split_string": {"delimiter": ",", "trim": True},
    }
    rows = transform_row_properties({"tags": "a, b ,c"}, cfg)
    assert [r["parts"] for r in rows] == ["a", "b", "c"]


def test_parse_json_extract_path() -> None:
    block = {"path": "meta.id"}
    assert apply_parse_json_extract('{"meta": {"id": "x1"}}', block) == "x1"


def test_format_datetime_iso() -> None:
    out = apply_format_datetime("2024-06-15T10:30:00Z", {"output_format": "%Y-%m-%d"})
    assert out == "2024-06-15"


def test_hash_stable() -> None:
    a = apply_hash_stable("tag", {"salt": "s", "algorithm": "sha256"})
    b = apply_hash_stable("tag", {"salt": "s", "algorithm": "sha256"})
    assert a == b
    assert len(a) == 64


def test_mask_string() -> None:
    assert apply_mask_string("ABCDEFGH", {"keep_last": 3, "mask_char": "*"}) == "*****FGH"


def test_static_lookup_map() -> None:
    block = {"map": {"FIC": "FLOW", "FI": "FLOW"}}
    assert apply_static_lookup_map("FIC-1", block) == "FLOW"
    block2 = {"pairs": [{"key": "FI", "value": "FLOW"}]}
    assert apply_static_lookup_map("FI-9", block2) == "FLOW"


def _minimal_transform_cfg(handler_id: str, block: dict) -> dict:
    return {
        "handler_id": handler_id,
        "fields": [{"field_name": "x"}],
        "output_template": "{x}",
        "output_field": "out",
        "output_mode": "overwrite",
        handler_id: block,
    }


def test_multifield_working_comma_join_no_template() -> None:
    cfg = {
        "handler_id": "trim_whitespace",
        "fields": [{"field_name": "a"}, {"field_name": "b"}],
        "output_field": "merged",
        "output_mode": "overwrite",
        "trim_whitespace": {"mode": "ends_only"},
    }
    rows = transform_row_properties({"a": "1", "b": "2"}, cfg)
    assert rows[0]["merged"] == "1,2"


def test_multifield_working_dedupes_duplicate_values() -> None:
    cfg = {
        "handler_id": "trim_whitespace",
        "fields": [{"field_name": "a"}, {"field_name": "b"}, {"field_name": "c"}],
        "output_field": "merged",
        "output_mode": "overwrite",
        "trim_whitespace": {"mode": "ends_only"},
    }
    rows = transform_row_properties({"a": "x", "b": "y", "c": "x"}, cfg)
    assert rows[0]["merged"] == "x,y"


def test_heuristic_sampler_prefers_longer_literal() -> None:
    assert apply_heuristic_sampler("Area P-101 valve", {"samples": ["P-10", "P-101"]}) == "P-101"


def test_heuristic_sampler_on_no_match_modes() -> None:
    assert apply_heuristic_sampler("abc", {"samples": ["z"]}) == "abc"
    assert apply_heuristic_sampler("abc", {"samples": ["z"], "on_no_match": "empty"}) == ""
    assert (
        apply_heuristic_sampler(
            "abc", {"samples": ["z"], "on_no_match": "default", "default_value": "N/A"}
        )
        == "N/A"
    )


def test_heuristic_sampler_samples_as_regex() -> None:
    out = apply_heuristic_sampler(
        "id-42-x", {"samples": [r"[A-Z]+", r"\d+"], "samples_as_regex": True}
    )
    assert out == "42"


def test_validate_heuristic_sampler_requires_samples_or_pattern() -> None:
    with pytest.raises(ValueError, match="samples"):
        validate_transform_config(_minimal_transform_cfg("heuristic_sampler", {"samples": []}))


def test_validate_heuristic_sampler_rejects_too_many_samples() -> None:
    samples = [f"s{i}" for i in range(201)]
    with pytest.raises(ValueError, match="201"):
        validate_transform_config(_minimal_transform_cfg("heuristic_sampler", {"samples": samples}))
