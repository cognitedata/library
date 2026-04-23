"""Tests that ``source_views[].validation`` is ignored by KeyExtractionEngine (global / rule validation only)."""

import sys
import unittest
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)

_ASSOC_PT = [
    {
        "kind": "source_view_to_extraction",
        "source_view_index": 0,
        "extraction_rule_name": "pt",
    }
]


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


class TestSourceViewValidationIgnored(unittest.TestCase):
    def _passthrough_rule(self, entity_types):
        return {
            "name": "pt",
            "rule_id": "pt",
            "handler": "regex_handler",
            "extraction_type": "candidate_key",
            "enabled": True,
            "priority": 50,
            "field_results_mode": "merge_all",
            "fields": [
                {
                    "field_name": "name",
                    "required": True,
                    "max_length": 500,
                    "priority": 1,
                    "preprocessing": ["trim"],
                }
            ],
            "scope_filters": {"entity_type": entity_types},
        }

    def test_no_source_views_uses_global_only(self):
        cfg = {
            "associations": list(_ASSOC_PT),
            "extraction_rules": [self._passthrough_rule(["file"])],
            "validation": {
                "min_confidence": 0.1,
                "validation_rules": [
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
        res = engine.extract_keys(ent, "file", source_view_index=0)
        self.assertEqual(len(res.candidate_keys), 1)
        self.assertAlmostEqual(res.candidate_keys[0].confidence, 0.5, places=4)

    def test_source_view_validation_rules_do_not_merge(self):
        global_penalty = {
            "name": "global_penalty",
            "priority": 100,
            "match": {"expressions": [{"pattern": ".*"}]},
            "confidence_modifier": {"mode": "offset", "value": -0.5},
        }
        cfg = {
            "associations": list(_ASSOC_PT),
            "extraction_rules": [self._passthrough_rule(["file"])],
            "validation": {
                "min_confidence": 0.1,
                "validation_rules": [global_penalty],
            },
            "source_views": [
                {
                    "view_external_id": "CogniteFile",
                    "view_space": "cdf_cdm",
                    "view_version": "v1",
                    "entity_type": "file",
                    "validation": {
                        "validation_rules": [
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
        res = engine.extract_keys(ent, "file", source_view_index=0)
        self.assertEqual(len(res.candidate_keys), 1)
        self.assertAlmostEqual(res.candidate_keys[0].confidence, 0.5, places=4)

    def test_source_view_min_confidence_is_ignored(self):
        cfg = {
            "associations": list(_ASSOC_PT),
            "extraction_rules": [self._passthrough_rule(["asset"])],
            "validation": {"min_confidence": 0.95, "validation_rules": []},
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
        res = engine.extract_keys(ent, "asset", source_view_index=0)
        self.assertEqual(len(res.candidate_keys), 1)
        self.assertAlmostEqual(res.candidate_keys[0].confidence, 1.0)

    def test_multiple_source_view_rows_validation_ignored(self):
        cfg = {
            "associations": list(_ASSOC_PT),
            "extraction_rules": [self._passthrough_rule(["asset"])],
            "validation": {"min_confidence": 0.1, "validation_rules": []},
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
        res = engine.extract_keys(ent, "asset", source_view_index=0)
        self.assertEqual(len(res.candidate_keys), 1)
        self.assertAlmostEqual(res.candidate_keys[0].confidence, 1.0)

    def test_file_without_per_view_rules_no_global_isa_penalty(self):
        cfg = {
            "associations": list(_ASSOC_PT),
            "extraction_rules": [self._passthrough_rule(["file"])],
            "validation": {
                "min_confidence": 0.5,
                "max_keys_per_type": 1000,
                "validation_rules": [],
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
        res = engine.extract_keys(ent, "file", source_view_index=0)
        self.assertEqual(len(res.candidate_keys), 1)
        self.assertAlmostEqual(res.candidate_keys[0].confidence, 1.0, places=4)

    def test_view_empty_validation_rules_still_uses_global(self):
        penalty = {
            "name": "penalty",
            "priority": 100,
            "match": {"expressions": [{"pattern": ".*"}]},
            "confidence_modifier": {"mode": "offset", "value": -0.4},
        }
        cfg = {
            "associations": list(_ASSOC_PT),
            "extraction_rules": [self._passthrough_rule(["file"])],
            "validation": {
                "min_confidence": 0.1,
                "validation_rules": [penalty],
            },
            "source_views": [
                {
                    "view_external_id": "CogniteFile",
                    "view_space": "cdf_cdm",
                    "view_version": "v1",
                    "entity_type": "file",
                    "validation": {"validation_rules": []},
                }
            ],
        }
        engine = KeyExtractionEngine(cfg)
        ent = _stamped_entity(name="a.pdf")
        res = engine.extract_keys(ent, "file", source_view_index=0)
        self.assertAlmostEqual(res.candidate_keys[0].confidence, 0.6, places=4)


if __name__ == "__main__":
    unittest.main()
