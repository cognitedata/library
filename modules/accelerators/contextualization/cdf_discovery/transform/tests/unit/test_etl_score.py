from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

import pytest

from cdf_fn_common.etl_score_match_eval import apply_score_match_rules_to_float_scores  # noqa: E402
from cdf_fn_common.etl_score_property import score_property_key  # noqa: E402
from cdf_fn_common.etl_score_validate import score_row_properties  # noqa: E402


def test_score_property_key_uses_score_suffix() -> None:
    assert score_property_key("aliases") == "aliases_score"


def test_apply_score_rules_offset() -> None:
    rules = [
        {
            "name": "penalty",
            "priority": 10,
            "match": {"expressions": [".*"]},
            "score_modifier": {"mode": "offset", "value": -0.2},
        }
    ]
    out = apply_score_match_rules_to_float_scores([("x", 1.0)], rules_raw=rules)
    assert out[0][1] == pytest.approx(0.8)


def test_score_row_properties_writes_parallel_scores() -> None:
    cfg = {
        "description": "test",
        "score_fields": ["tags"],
        "scoring_rules": [],
    }
    props = {"tags": ["a", "b"]}
    out = score_row_properties(props, cfg, [])
    assert out["tags"] == ["a", "b"]
    assert out["tags_score"] == [1.0, 1.0]
