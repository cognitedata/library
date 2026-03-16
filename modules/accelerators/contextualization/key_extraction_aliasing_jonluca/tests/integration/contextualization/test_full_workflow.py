#!/usr/bin/env python3
"""
Integration Tests for Full Contextualization Workflow

This module provides integration tests for the complete workflow
combining key extraction and aliasing.
"""

import sys
import unittest
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.contextualization.key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingEngine,
)
from modules.contextualization.key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    ExtractionResult,
    KeyExtractionEngine,
)


class TestIntegrationWorkflow(unittest.TestCase):
    """Integration tests for the full workflow."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a comprehensive test configuration
        self.workflow_config = {
            "extraction_rules": [
                {
                    "name": "pump_extraction",
                    "description": "Extract pump tags",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "pattern": r"\bP[-_]?\d{1,6}[A-Z]?\b",
                    "priority": 50,
                    "enabled": True,
                    "min_confidence": 0.7,
                    "case_sensitive": False,
                    "source_fields": [{"field_name": "name", "required": True}],
                    "config": {"pattern": "P[-_]?\d{1,6}[A-Z]?"},
                },
                {
                    "name": "valve_extraction",
                    "description": "Extract valve tags",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "pattern": r"\bV[-_]?\d{1,6}[A-Z]?\b",
                    "priority": 50,
                    "enabled": True,
                    "min_confidence": 0.7,
                    "case_sensitive": False,
                    "source_fields": [{"field_name": "name", "required": True}],
                    "config": {"pattern": "V[-_]?\d{1,6}[A-Z]?"},
                },
            ],
            "validation": {
                "min_confidence": 0.5,
                "max_keys_per_type": 10,
                "min_alias_length": 2,
                "max_alias_length": 50,
                "allowed_characters": "A-Za-z0-9-_/. ",
            },
        }

        self.aliasing_config = {
            "rules": [
                {
                    "name": "separator_variants",
                    "type": "character_substitution",
                    "enabled": True,
                    "priority": 10,
                    "preserve_original": True,
                    "config": {"substitutions": {"-": ["_", " ", ""]}},
                }
            ],
            "validation": {
                "max_aliases_per_tag": 30,
                "min_alias_length": 2,
                "max_alias_length": 50,
            },
        }

        self.extraction_engine = KeyExtractionEngine(self.workflow_config)
        self.aliasing_engine = AliasingEngine(self.aliasing_config)

    def test_full_workflow(self):
        """Test the complete workflow from extraction to aliasing."""
        test_asset = {
            "id": "workflow_test_001",
            "name": "P-10001",
            "description": "Main feed pump for Tank T-301",
            "metadata": {"site": "Plant_A", "equipmentType": "pump"},
        }

        # Step 1: Extract keys
        extraction_result = self.extraction_engine.extract_keys(test_asset, "asset")

        self.assertIsInstance(extraction_result, ExtractionResult)
        self.assertGreater(len(extraction_result.candidate_keys), 0)

        # Step 2: Generate aliases for each extracted key
        all_aliases = {}
        for key in extraction_result.candidate_keys:
            alias_result = self.aliasing_engine.generate_aliases(
                key.value,
                "asset",
                {"equipment_type": test_asset["metadata"]["equipmentType"]},
            )
            all_aliases[key.value] = alias_result.aliases

        # Verify aliases were generated
        self.assertGreater(len(all_aliases), 0)

        # Check that original key has aliases
        pump_key = next(
            (k for k in extraction_result.candidate_keys if k.value == "P-10001"), None
        )
        self.assertIsNotNone(pump_key)
        self.assertIn("P-10001", all_aliases)

        # Verify alias variants
        pump_aliases = all_aliases["P-10001"]
        self.assertIn("P_10001", pump_aliases)
        self.assertIn("P 10001", pump_aliases)
        self.assertIn("P10001", pump_aliases)

    def test_workflow_with_multiple_assets(self):
        """Test workflow with multiple assets."""
        test_assets = [
            {
                "id": "asset_001",
                "name": "P-10001",
                "description": "Feed pump",
                "metadata": {"equipmentType": "pump"},
            },
            {
                "id": "asset_002",
                "name": "V-20001",
                "description": "Control valve",
                "metadata": {"equipmentType": "valve"},
            },
        ]

        results = []
        for asset in test_assets:
            # Extract keys
            extraction_result = self.extraction_engine.extract_keys(asset, "asset")

            # Generate aliases
            aliases = {}
            for key in extraction_result.candidate_keys:
                alias_result = self.aliasing_engine.generate_aliases(
                    key.value,
                    "asset",
                    {"equipment_type": asset["metadata"]["equipmentType"]},
                )
                aliases[key.value] = alias_result.aliases

            results.append(
                {
                    "asset": asset,
                    "extraction_result": extraction_result,
                    "aliases": aliases,
                }
            )

        self.assertEqual(len(results), 2)

        # Verify first asset results
        self.assertEqual(len(results[0]["extraction_result"].candidate_keys), 1)
        self.assertIn("P-10001", results[0]["aliases"])

        # Verify second asset results
        self.assertEqual(len(results[1]["extraction_result"].candidate_keys), 1)
        self.assertIn("V-20001", results[1]["aliases"])


if __name__ == "__main__":
    unittest.main()
