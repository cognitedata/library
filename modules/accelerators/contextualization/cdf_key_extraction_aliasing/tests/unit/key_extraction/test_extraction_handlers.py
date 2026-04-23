#!/usr/bin/env python3
"""Unit tests for regex_handler and heuristic handlers (extract_from_entity)."""

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.handlers import (
    FieldRuleExtractionHandler,
    HeuristicExtractionHandler,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.DataStructures import (
    ExtractionMethod,
    ExtractionType,
)


def _gv_name(entity, spec, rule_name=None):
    fn = spec.field_name if hasattr(spec, "field_name") else spec["field_name"]
    return entity.get(fn)


class TestFieldRuleHandler(unittest.TestCase):
    def test_regex_matches(self):
        h = FieldRuleExtractionHandler()
        rule = SimpleNamespace(
            rule_id="r1",
            handler="regex_handler",
            field_results_mode="merge_all",
            fields=[
                SimpleNamespace(
                    field_name="name",
                    required=False,
                    regex=r"\bP[-_]?\d{1,6}[A-Z]?\b",
                    regex_options={"ignore_case": True},
                )
            ],
            validation=SimpleNamespace(min_confidence=0.1),
            entity_types=[],
        )
        ctx = {"entity_type": "asset"}
        ent = {"name": "Pump P-10001 ok"}
        keys = h.extract_from_entity(ent, rule, ctx, get_field_value=_gv_name)
        self.assertTrue(any(k.value == "P-10001" for k in keys))
        self.assertEqual(keys[0].method, ExtractionMethod.REGEX_HANDLER)

    def test_trim_only(self):
        h = FieldRuleExtractionHandler()
        rule = SimpleNamespace(
            rule_id="r2",
            field_results_mode="merge_all",
            fields=[SimpleNamespace(field_name="name", required=True)],
            validation=SimpleNamespace(min_confidence=0.1),
            entity_types=[],
        )
        keys = h.extract_from_entity(
            {"name": "  XY-1  "},
            rule,
            {"entity_type": "asset"},
            get_field_value=_gv_name,
        )
        self.assertEqual(len(keys), 1)
        self.assertEqual(keys[0].value, "XY-1")


class TestHeuristicHandler(unittest.TestCase):
    def test_emits_candidates(self):
        h = HeuristicExtractionHandler()
        rule = SimpleNamespace(
            rule_id="h1",
            field_results_mode="merge_all",
            fields=[SimpleNamespace(field_name="description", required=False)],
            parameters=SimpleNamespace(
                strategies=[
                    SimpleNamespace(id="delimiter_split", weight=1.0),
                ],
                max_candidates_per_field=10,
            ),
            entity_types=[],
        )
        keys = h.extract_from_entity(
            {"description": "see tag P-101 and FIC-2001"},
            rule,
            {"entity_type": "asset"},
            get_field_value=_gv_name,
        )
        self.assertGreater(len(keys), 0)
        self.assertEqual(keys[0].method, ExtractionMethod.HEURISTIC)

    def test_loose_patterns_unit_numeric_prefix_tag(self):
        h = HeuristicExtractionHandler()
        rule = SimpleNamespace(
            rule_id="h2",
            field_results_mode="merge_all",
            fields=[SimpleNamespace(field_name="name", required=False)],
            parameters=SimpleNamespace(
                strategies=[
                    SimpleNamespace(id="loose_patterns", weight=1.0),
                ],
                max_candidates_per_field=50,
            ),
            entity_types=[],
        )
        keys = h.extract_from_entity(
            {"name": "Instrument 10-P-1234 on line"},
            rule,
            {"entity_type": "asset"},
            get_field_value=_gv_name,
        )
        vals = [k.value for k in keys]
        self.assertIn("10-P-1234", vals)


class TestEngineIntegration(unittest.TestCase):
    def test_regex_handler_end_to_end(self):
        cfg = {
            "associations": [
                {
                    "kind": "source_view_to_extraction",
                    "source_view_index": 0,
                    "extraction_rule_name": "t1",
                }
            ],
            "extraction_rules": [
                {
                    "rule_id": "t1",
                    "name": "t1",
                    "handler": "regex_handler",
                    "extraction_type": "candidate_key",
                    "field_results_mode": "merge_all",
                    "fields": [
                        {
                            "field_name": "name",
                            "regex": r"\bP-\d+\b",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.5},
                }
            ],
            "validation": {"min_confidence": 0.5, "max_keys_per_type": 10},
        }
        eng = KeyExtractionEngine(cfg)
        r = eng.extract_keys({"id": "1", "name": "Unit P-101"}, "asset", source_view_index=0)
        vals = [k.value for k in r.candidate_keys]
        self.assertIn("P-101", vals)


if __name__ == "__main__":
    unittest.main()
