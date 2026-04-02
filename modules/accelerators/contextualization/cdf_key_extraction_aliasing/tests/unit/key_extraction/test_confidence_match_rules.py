"""Unit tests for validation.confidence_match_rules (offset chains; explicit breaks)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)


def _base_rule(name: str, pattern: str) -> dict:
    return {
        "name": name,
        "extraction_type": "candidate_key",
        "method": "regex",
        "priority": 10,
        "enabled": True,
        "source_fields": [{"field_name": "name", "required": True}],
        "config": {"pattern": pattern},
    }


class TestConfidenceMatchRules(unittest.TestCase):
    def test_explicit_zero_then_min_confidence_drops(self):
        config = {
            "extraction_rules": [
                {
                    **_base_rule("tag", r"(?P<tag>\S+)"),
                    "pattern": r"(?P<tag>\S+)",
                }
            ],
            "validation": {
                "min_confidence": 0.5,
                "confidence_match_rules": [
                    {
                        "name": "bad",
                        "priority": 5,
                        "match": {"keywords": ["dummy"]},
                        "confidence_modifier": {"mode": "explicit", "value": 0.0},
                    }
                ],
            },
        }
        engine = KeyExtractionEngine(config)
        r_ok = engine.extract_keys({"id": "1", "name": "P-101"}, "asset")
        r_bad = engine.extract_keys({"id": "2", "name": "dummy-tag"}, "asset")
        self.assertGreater(len(r_ok.candidate_keys), 0)
        self.assertEqual(len(r_bad.candidate_keys), 0)

    def test_offset_rules_chain_isa_then_catch_all(self):
        config = {
            "extraction_rules": [
                {
                    **_base_rule("pump", r"\bP[-_]?\d+\b"),
                    "pattern": r"\bP[-_]?\d+\b",
                }
            ],
            "validation": {
                "min_confidence": 0.1,
                "confidence_match_rules": [
                    {
                        "name": "isa",
                        "priority": 50,
                        "match": {"expressions": [r"\bP[-_]?\d+\b"]},
                        "confidence_modifier": {"mode": "offset", "value": 0.05},
                    },
                    {
                        "name": "catch_all",
                        "priority": 1000,
                        "match": {"expressions": ["(?s).*"]},
                        "confidence_modifier": {"mode": "offset", "value": -0.5},
                    },
                ],
            },
        }
        engine = KeyExtractionEngine(config)
        result = engine.extract_keys({"id": "1", "name": "P-101"}, "asset")
        self.assertEqual(len(result.candidate_keys), 1)
        k = result.candidate_keys[0]
        # Both match: base ~1.0 + 0.05 (clamp 1.0) then -0.5 from catch-all
        self.assertAlmostEqual(k.confidence, 0.5, places=4)

    def test_explicit_modifier_stops_further_rules(self):
        config = {
            "extraction_rules": [
                {
                    **_base_rule("pump", r"\bP[-_]?\d+\b"),
                    "pattern": r"\bP[-_]?\d+\b",
                }
            ],
            "validation": {
                "min_confidence": 0.1,
                "confidence_match_rules": [
                    {
                        "name": "isa",
                        "priority": 50,
                        "match": {"expressions": [r"\bP[-_]?\d+\b"]},
                        "confidence_modifier": {"mode": "explicit", "value": 1.0},
                    },
                    {
                        "name": "catch_all",
                        "priority": 1000,
                        "match": {"expressions": ["(?s).*"]},
                        "confidence_modifier": {"mode": "offset", "value": -0.5},
                    },
                ],
            },
        }
        engine = KeyExtractionEngine(config)
        result = engine.extract_keys({"id": "1", "name": "P-101"}, "asset")
        self.assertEqual(len(result.candidate_keys), 1)
        self.assertGreaterEqual(result.candidate_keys[0].confidence, 0.99)

    def test_catch_all_penalty_when_no_earlier_match(self):
        config = {
            "extraction_rules": [
                {
                    **_base_rule("word", r"[A-Za-z]+"),
                    "pattern": r"[A-Za-z]+",
                }
            ],
            "validation": {
                "min_confidence": 0.1,
                "confidence_match_rules": [
                    {
                        "name": "isa_only",
                        "priority": 50,
                        "match": {"expressions": [r"\bP[-_]?\d+\b"]},
                        "confidence_modifier": {"mode": "offset", "value": 0.05},
                    },
                    {
                        "name": "catch_all",
                        "priority": 1000,
                        "match": {"expressions": ["(?s).*"]},
                        "confidence_modifier": {"mode": "offset", "value": -0.2},
                    },
                ],
            },
        }
        engine = KeyExtractionEngine(config)
        result = engine.extract_keys({"id": "1", "name": "ZZZ"}, "asset")
        self.assertEqual(len(result.candidate_keys), 1)
        k = result.candidate_keys[0]
        self.assertLess(k.confidence, 0.95)

    def test_keyword_or_expression_match(self):
        config = {
            "extraction_rules": [
                {
                    **_base_rule("any", r"\S+"),
                    "pattern": r"\S+",
                }
            ],
            "validation": {
                "min_confidence": 0.5,
                "confidence_match_rules": [
                    {
                        "name": "either",
                        "priority": 10,
                        "match": {
                            "keywords": ["alpha"],
                            "expressions": [r"^BETA-\d+$"],
                        },
                        "confidence_modifier": {"mode": "explicit", "value": 0.0},
                    }
                ],
            },
        }
        engine = KeyExtractionEngine(config)
        self.assertEqual(
            len(engine.extract_keys({"id": "1", "name": "X-alpha-Y"}, "asset").candidate_keys),
            0,
        )
        self.assertEqual(
            len(engine.extract_keys({"id": "2", "name": "BETA-1"}, "asset").candidate_keys),
            0,
        )
        self.assertGreater(
            len(engine.extract_keys({"id": "3", "name": "OKVAL"}, "asset").candidate_keys),
            0,
        )

    def test_expression_as_pattern_description_dict(self):
        config = {
            "extraction_rules": [
                {
                    **_base_rule("p", r"\bP[-_]?\d+\b"),
                    "pattern": r"\bP[-_]?\d+\b",
                }
            ],
            "validation": {
                "min_confidence": 0.5,
                "confidence_match_rules": [
                    {
                        "name": "isa",
                        "priority": 10,
                        "match": {
                            "expressions": [
                                {
                                    "pattern": r"\bP[-_]?\d+\b",
                                    "description": "pump tag",
                                }
                            ]
                        },
                        "confidence_modifier": {"mode": "explicit", "value": 1.0},
                    }
                ],
            },
        }
        engine = KeyExtractionEngine(config)
        result = engine.extract_keys({"id": "1", "name": "P-101"}, "asset")
        self.assertEqual(len(result.candidate_keys), 1)
        self.assertGreaterEqual(result.candidate_keys[0].confidence, 0.99)

    def test_enabled_false_skipped(self):
        config = {
            "extraction_rules": [
                {
                    **_base_rule("any", r"\S+"),
                    "pattern": r"\S+",
                }
            ],
            "validation": {
                "min_confidence": 0.5,
                "confidence_match_rules": [
                    {
                        "name": "disabled",
                        "enabled": False,
                        "priority": 5,
                        "match": {"keywords": ["every"]},
                        "confidence_modifier": {"mode": "explicit", "value": 0.0},
                    },
                    {
                        "name": "catch",
                        "priority": 100,
                        "match": {"expressions": ["(?s).*"]},
                        "confidence_modifier": {"mode": "offset", "value": 0.0},
                    },
                ],
            },
        }
        engine = KeyExtractionEngine(config)
        result = engine.extract_keys({"id": "1", "name": "everything"}, "asset")
        self.assertGreater(len(result.candidate_keys), 0)


if __name__ == "__main__":
    unittest.main()
