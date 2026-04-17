"""Tests for merging source_views[].validation with global validation in KeyExtractionEngine."""

import sys
import unittest
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)


def _stamped_entity(
    *,
    name: str,
    entity_type: str = "file",
    view_external_id: str = "CogniteFile",
    view_space: str = "cdf_cdm",
    view_version: str = "v1",
) -> dict:
    return {
        "externalId": "e1",
        "name": name,
        "entity_type": entity_type,
        "view_external_id": view_external_id,
        "view_space": view_space,
        "view_version": view_version,
    }


class TestSourceViewValidationMerge(unittest.TestCase):
    def _passthrough_rule(self, entity_types):
        return {
            "name": "pt",
            "handler": "passthrough",
            "extraction_type": "candidate_key",
            "enabled": True,
            "priority": 50,
            "min_confidence": 1.0,
            "scope_filters": {"entity_type": entity_types},
            "config": {},
            "source_fields": [
                {
                    "field_name": "name",
                    "required": True,
                    "max_length": 500,
                    "field_type": "string",
                    "priority": 1,
                    "role": "target",
                    "preprocessing": ["trim"],
                }
            ],
            "field_selection_strategy": "first_match",
        }

    def test_no_source_views_uses_global_only(self):
        cfg = {
            "extraction_rules": [self._passthrough_rule(["file"])],
            "validation": {
                "min_confidence": 0.1,
                "confidence_match_rules": [
                    {
                        "name": "penalty",
                        "priority": 100,
                        "match": {"expressions": [{"pattern": ".*"}]},
                        "confidence_modifier": {"mode": "offset", "value": -0.5},
                    }
                ],
            },
        }
        engine = KeyExtractionEngine(cfg)
        self.assertEqual(engine._source_views, [])
        ent = _stamped_entity(name="x.pdf")
        res = engine.extract_keys(ent, "file")
        self.assertEqual(len(res.candidate_keys), 1)
        self.assertAlmostEqual(res.candidate_keys[0].confidence, 0.5, places=4)

    def test_view_validation_merges_confidence_rules(self):
        global_penalty = {
            "name": "global_penalty",
            "priority": 100,
            "match": {"expressions": [{"pattern": ".*"}]},
            "confidence_modifier": {"mode": "offset", "value": -0.5},
        }
        cfg = {
            "extraction_rules": [self._passthrough_rule(["file"])],
            "validation": {
                "min_confidence": 0.1,
                "confidence_match_rules": [global_penalty],
            },
            "source_views": [
                {
                    "view_external_id": "CogniteFile",
                    "view_space": "cdf_cdm",
                    "view_version": "v1",
                    "entity_type": "file",
                    "validation": {
                        "confidence_match_rules": [
                            {
                                "name": "view_first",
                                "priority": 10,
                                "match": {"expressions": [{"pattern": ".*"}]},
                                "confidence_modifier": {"mode": "offset", "value": -0.1},
                            }
                        ]
                    },
                }
            ],
        }
        engine = KeyExtractionEngine(cfg)
        ent = _stamped_entity(name="doc.pdf")
        res = engine.extract_keys(ent, "file")
        self.assertEqual(len(res.candidate_keys), 1)
        # Sorted by priority: view -0.1 then global -0.5; both .* match -> 1.0 - 0.1 - 0.5 = 0.4
        self.assertAlmostEqual(res.candidate_keys[0].confidence, 0.4, places=4)

    def test_min_confidence_override_on_view(self):
        cfg = {
            "extraction_rules": [self._passthrough_rule(["asset"])],
            "validation": {"min_confidence": 0.95, "confidence_match_rules": []},
            "source_views": [
                {
                    "view_external_id": "CogniteAsset",
                    "view_space": "cdf_cdm",
                    "view_version": "v1",
                    "entity_type": "asset",
                    "validation": {"min_confidence": 0.5},
                }
            ],
        }
        engine = KeyExtractionEngine(cfg)
        ent = _stamped_entity(
            name="P-101",
            entity_type="asset",
            view_external_id="CogniteAsset",
        )
        res = engine.extract_keys(ent, "asset")
        self.assertEqual(len(res.candidate_keys), 1)
        self.assertAlmostEqual(res.candidate_keys[0].confidence, 1.0)

    def test_first_source_view_wins_when_both_match(self):
        cfg = {
            "extraction_rules": [self._passthrough_rule(["asset"])],
            "validation": {"min_confidence": 0.1, "confidence_match_rules": []},
            "source_views": [
                {
                    "view_external_id": "CogniteAsset",
                    "view_space": "cdf_cdm",
                    "view_version": "v1",
                    "entity_type": "asset",
                    "validation": {"min_confidence": 1.01},
                },
                {
                    "view_external_id": "CogniteAsset",
                    "view_space": "cdf_cdm",
                    "view_version": "v1",
                    "entity_type": "asset",
                    "validation": {"min_confidence": 0.1},
                },
            ],
        }
        engine = KeyExtractionEngine(cfg)
        ent = _stamped_entity(
            name="P-101",
            entity_type="asset",
            view_external_id="CogniteAsset",
        )
        res = engine.extract_keys(ent, "asset")
        # First matching source_views row wins; min_confidence 1.01 drops the key.
        self.assertEqual(len(res.candidate_keys), 0)

    def test_file_without_per_view_rules_no_global_isa_penalty(self):
        cfg = {
            "extraction_rules": [self._passthrough_rule(["file"])],
            "validation": {
                "min_confidence": 0.5,
                "max_keys_per_type": 1000,
                "confidence_match_rules": [],
            },
            "source_views": [
                {
                    "view_external_id": "CogniteFile",
                    "view_space": "cdf_cdm",
                    "view_version": "v1",
                    "entity_type": "file",
                }
            ],
        }
        engine = KeyExtractionEngine(cfg)
        ent = _stamped_entity(name="drawing-100.pdf")
        res = engine.extract_keys(ent, "file")
        self.assertEqual(len(res.candidate_keys), 1)
        self.assertAlmostEqual(res.candidate_keys[0].confidence, 1.0, places=4)

    def test_view_empty_confidence_match_rules_keeps_global(self):
        penalty = {
            "name": "penalty",
            "priority": 100,
            "match": {"expressions": [{"pattern": ".*"}]},
            "confidence_modifier": {"mode": "offset", "value": -0.4},
        }
        cfg = {
            "extraction_rules": [self._passthrough_rule(["file"])],
            "validation": {
                "min_confidence": 0.1,
                "confidence_match_rules": [penalty],
            },
            "source_views": [
                {
                    "view_external_id": "CogniteFile",
                    "view_space": "cdf_cdm",
                    "view_version": "v1",
                    "entity_type": "file",
                    "validation": {"confidence_match_rules": []},
                }
            ],
        }
        engine = KeyExtractionEngine(cfg)
        ent = _stamped_entity(name="a.pdf")
        res = engine.extract_keys(ent, "file")
        self.assertAlmostEqual(res.candidate_keys[0].confidence, 0.6, places=4)


if __name__ == "__main__":
    unittest.main()
