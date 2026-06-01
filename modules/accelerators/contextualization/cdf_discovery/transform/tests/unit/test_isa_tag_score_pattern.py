from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

import pytest

from cdf_fn_common.etl_score_match_eval import (  # noqa: E402
    apply_score_match_rules_to_float_scores,
    build_sorted_score_runtime,
)
from cdf_fn_common.isa_tag_pattern import (  # noqa: E402
    should_apply_isa_tag_area_prefix,
    with_optional_isa_tag_area_prefix,
)


def test_should_apply_isa_tag_area_prefix_for_isa_suffix_only() -> None:
    assert should_apply_isa_tag_area_prefix(r"\bP[-_]?\d{1,6}[A-Z]?\b")
    assert not should_apply_isa_tag_area_prefix("^[0-9]{0,3}$")
    assert not should_apply_isa_tag_area_prefix("(?s).*")


def test_with_optional_isa_tag_area_prefix_wraps_suffix() -> None:
    wrapped = with_optional_isa_tag_area_prefix(r"\bP[-_]?\d{1,6}[A-Z]?\b")
    assert wrapped.startswith("(?:")
    assert wrapped.endswith(r"\bP[-_]?\d{1,6}[A-Z]?\b")


def test_isa_score_rule_matches_tag_with_area_prefix() -> None:
    rules = [
        {
            "name": "isa_compliant",
            "priority": 10,
            "match": {
                "expressions": [{"pattern": r"\bP[-_]?\d{1,6}[A-Z]?\b"}],
            },
            "score_modifier": {"mode": "explicit", "value": 1.0},
        }
    ]
    out = apply_score_match_rules_to_float_scores(
        [("P-101", 0.5), ("10-P-101", 0.5), ("U1-P-10001", 0.5)],
        rules_raw=rules,
    )
    assert out[0][1] == pytest.approx(1.0)
    assert out[1][1] == pytest.approx(1.0)
    assert out[2][1] == pytest.approx(1.0)


def test_build_sorted_score_runtime_uses_prefixed_regex() -> None:
    runtime = build_sorted_score_runtime(
        [
            {
                "name": "isa",
                "match": {"expressions": [r"\bC[-_]?\d{1,6}[A-Z]?\b"]},
                "score_modifier": {"mode": "offset", "value": 0.1},
            }
        ],
        rules_order="list",
    )
    assert len(runtime) == 1
    _pri, _idx, _name, compiled, _kws, _mode, _on, _off = runtime[0]
    assert len(compiled) == 1
    assert compiled[0].search("10-C-201")
