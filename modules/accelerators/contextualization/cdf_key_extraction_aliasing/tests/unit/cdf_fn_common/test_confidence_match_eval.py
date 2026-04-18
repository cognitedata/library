"""Tests for shared confidence_match_rules evaluation."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.confidence_match_eval import (  # noqa: E402
    apply_confidence_match_rules_to_float_scores,
    normalize_confidence_match_step,
    resolve_expression_match_for_rule,
    value_matches_confidence_rule,
)


class TestResolveExpressionMatch(unittest.TestCase):
    def test_rule_overrides_validation_default(self):
        rule = {"expression_match": "fullmatch"}
        self.assertEqual(
            resolve_expression_match_for_rule(rule, {"expression_match": "search"}),
            "fullmatch",
        )

    def test_validation_default_when_rule_omits(self):
        rule = {"name": "r"}
        self.assertEqual(
            resolve_expression_match_for_rule(rule, {"expression_match": "fullmatch"}),
            "fullmatch",
        )

    def test_fallback_search(self):
        self.assertEqual(resolve_expression_match_for_rule({}, {}), "search")
        self.assertEqual(resolve_expression_match_for_rule({}, None), "search")


class TestValueMatchesExpressionMode(unittest.TestCase):
    def test_search_finds_substring(self):
        import re

        pats = [re.compile(r"\d+")]
        self.assertTrue(
            value_matches_confidence_rule("ab12cd", pats, [], "search")
        )
        self.assertFalse(
            value_matches_confidence_rule("ab12cd", pats, [], "fullmatch")
        )

    def test_fullmatch_whole_string(self):
        import re

        pats = [re.compile(r"\d+")]
        self.assertTrue(value_matches_confidence_rule("12", pats, [], "fullmatch"))


class TestApplyRulesChaining(unittest.TestCase):
    def test_offset_chains(self):
        rules = [
            {
                "name": "a",
                "priority": 10,
                "match": {"expressions": [".*"]},
                "confidence_modifier": {"mode": "offset", "value": -0.1},
            },
            {
                "name": "b",
                "priority": 20,
                "match": {"expressions": [".*"]},
                "confidence_modifier": {"mode": "offset", "value": -0.2},
            },
        ]
        out = apply_confidence_match_rules_to_float_scores(
            [("x", 1.0)], rules_raw=rules
        )
        self.assertAlmostEqual(out[0][1], 0.7, places=4)

    def test_explicit_stops(self):
        rules = [
            {
                "name": "lock",
                "priority": 10,
                "match": {"expressions": [".*"]},
                "confidence_modifier": {"mode": "explicit", "value": 1.0},
            },
            {
                "name": "penalty",
                "priority": 20,
                "match": {"expressions": [".*"]},
                "confidence_modifier": {"mode": "offset", "value": -0.5},
            },
        ]
        out = apply_confidence_match_rules_to_float_scores(
            [("x", 0.5)], rules_raw=rules
        )
        self.assertAlmostEqual(out[0][1], 1.0, places=4)

    def test_concurrent_sorts_by_priority_vs_flat_list_order(self):
        """``hierarchy`` concurrent applies children by ascending priority; flat list uses YAML order."""
        rules = [
            {
                "name": "strict_foo",
                "priority": 100,
                "match": {"expressions": [r"^foo$"]},
                "confidence_modifier": {"mode": "explicit", "value": 0.9},
            },
            {
                "name": "broad_f",
                "priority": 10,
                "match": {"expressions": [r"f.*"]},
                "confidence_modifier": {"mode": "explicit", "value": 0.1},
            },
        ]
        out_conc = apply_confidence_match_rules_to_float_scores(
            [("foo", 1.0)],
            rules_raw=[{"hierarchy": {"mode": "concurrent", "children": rules}}],
        )
        out_list = apply_confidence_match_rules_to_float_scores(
            [("foo", 1.0)],
            rules_raw=rules,
        )
        self.assertAlmostEqual(out_conc[0][1], 0.1, places=4)
        self.assertAlmostEqual(out_list[0][1], 0.9, places=4)

    def test_invalid_hierarchy_mode_raises(self):
        with self.assertRaises(ValueError):
            apply_confidence_match_rules_to_float_scores(
                [("x", 1.0)],
                rules_raw=[{"hierarchy": {"mode": "bogus", "children": []}}],
            )

    def test_normalize_shorthand_to_hierarchy(self):
        n = normalize_confidence_match_step({"blacklist": ["isa_compliant", "not_isa_penalty"]})
        self.assertEqual(
            n,
            {
                "hierarchy": {
                    "mode": "ordered",
                    "children": ["blacklist", "isa_compliant", "not_isa_penalty"],
                }
            },
        )

    def test_normalize_nested_shorthand_right_associative(self):
        nested = {"blacklist": [{"isa_compliant": ["not_isa_penalty"]}]}
        n = normalize_confidence_match_step(nested)
        self.assertEqual(
            n,
            {
                "hierarchy": {
                    "mode": "ordered",
                    "children": [
                        "blacklist",
                        {
                            "hierarchy": {
                                "mode": "ordered",
                                "children": ["isa_compliant", "not_isa_penalty"],
                            }
                        },
                    ],
                }
            },
        )


if __name__ == "__main__":
    unittest.main()
