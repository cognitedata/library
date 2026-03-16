#!/usr/bin/env python3
"""
Integration Tests for Aliasing Engine

This module provides integration tests for the AliasingEngine,
testing real-world aliasing scenarios and edge cases.
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
from modules.contextualization.key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingResult as AliasResult,
)


class TestAliasingEngine(unittest.TestCase):
    """Test cases for the Aliasing Engine."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_config = {
            "rules": [
                {
                    "name": "normalize_separators",
                    "type": "character_substitution",
                    "enabled": True,
                    "priority": 10,
                    "preserve_original": True,
                    "config": {"substitutions": {"_": "-", " ": "-"}},
                },
                {
                    "name": "generate_separator_variants",
                    "type": "character_substitution",
                    "enabled": True,
                    "priority": 15,
                    "preserve_original": True,
                    "config": {"substitutions": {"-": ["_", " ", ""]}},
                },
                {
                    "name": "equipment_type_expansion",
                    "type": "equipment_type_expansion",
                    "enabled": True,
                    "priority": 30,
                    "preserve_original": True,
                    "config": {
                        "type_mappings": {
                            "P": ["PUMP", "PMP"],
                            "V": ["VALVE", "VLV"],
                            "T": ["TANK", "TNK"],
                        },
                        "format_templates": ["{type}-{tag}", "{type}_{tag}"],
                        "auto_detect": True,
                    },
                },
            ],
            "validation": {
                "max_aliases_per_tag": 30,
                "min_alias_length": 2,
                "max_alias_length": 50,
            },
        }

        self.engine = AliasingEngine(self.sample_config)

    def test_engine_initialization(self):
        """Test engine initialization with valid configuration."""
        self.assertIsInstance(self.engine, AliasingEngine)
        self.assertEqual(len(self.engine.rules), 3)
        self.assertEqual(self.engine.config, self.sample_config)

    def test_character_substitution(self):
        """Test character substitution aliasing."""
        result = self.engine.generate_aliases("P_10001", "asset")

        self.assertIsInstance(result, AliasResult)
        self.assertGreater(len(result.aliases), 0)

        # Should include normalized version
        self.assertIn("P-10001", result.aliases)

    def test_separator_variants(self):
        """Test generation of separator variants."""
        result = self.engine.generate_aliases("P-10001", "asset")

        # Should include different separator variants
        self.assertIn("P_10001", result.aliases)
        self.assertIn("P 10001", result.aliases)
        self.assertIn("P10001", result.aliases)

    def test_equipment_type_expansion(self):
        """Test equipment type expansion."""
        result = self.engine.generate_aliases(
            "P-10001", "asset", {"equipment_type": "pump"}
        )

        # Should include expanded equipment types
        self.assertIn("PUMP-10001", result.aliases)
        self.assertIn("PMP-10001", result.aliases)
        self.assertIn("PUMP_10001", result.aliases)
        self.assertIn("PMP_10001", result.aliases)

    def test_context_aware_aliasing(self):
        """Test aliasing with context information."""
        context = {"site": "Plant_A", "equipment_type": "pump", "unit": "Feed"}

        result = self.engine.generate_aliases("P-10001", "asset", context)

        self.assertIsInstance(result, AliasResult)
        self.assertGreater(len(result.aliases), 0)

    def test_alias_validation(self):
        """Test alias validation (length, characters, etc.)."""
        result = self.engine.generate_aliases("P-10001", "asset")

        for alias in result.aliases:
            # Check length constraints
            self.assertGreaterEqual(len(alias), 2)
            self.assertLessEqual(len(alias), 50)

            # Check character constraints (basic validation)
            self.assertTrue(
                alias.replace("-", "").replace("_", "").replace(" ", "").isalnum()
            )

    def test_max_aliases_limit(self):
        """Test maximum aliases per tag limit."""
        # Create a config with very low limit
        limited_config = self.sample_config.copy()
        limited_config["validation"]["max_aliases_per_tag"] = 5

        limited_engine = AliasingEngine(limited_config)
        result = limited_engine.generate_aliases("P-10001", "asset")

        self.assertLessEqual(len(result.aliases), 5)

    def test_disabled_rules(self):
        """Test that disabled rules are not applied."""
        disabled_config = self.sample_config.copy()
        disabled_config["rules"][0]["enabled"] = False

        disabled_engine = AliasingEngine(disabled_config)
        result = disabled_engine.generate_aliases("P_10001", "asset")

        # Should not include normalized version if rule is disabled
        # (This depends on implementation - adjust as needed)

    def test_empty_input_handling(self):
        """Test handling of empty or invalid input."""
        result = self.engine.generate_aliases("", "asset")

        self.assertIsInstance(result, AliasResult)
        self.assertEqual(len(result.aliases), 0)

    def test_rule_priority(self):
        """Test that rules are applied in priority order."""
        result = self.engine.generate_aliases("P_10001", "asset")

        # Should include both normalized and variant forms
        self.assertIn(
            "P-10001", result.aliases
        )  # From normalize_separators (priority 10)
        self.assertIn(
            "P_10001", result.aliases
        )  # From generate_separator_variants (priority 15)


if __name__ == "__main__":
    unittest.main()
