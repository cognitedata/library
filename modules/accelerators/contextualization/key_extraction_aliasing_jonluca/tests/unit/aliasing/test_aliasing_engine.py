"""
Unit Tests for AliasingEngine

This module provides comprehensive unit tests for the AliasingEngine,
testing the full engine orchestration including all transformation types,
rule application, validation, and edge cases.
"""

# Add project root to path for imports
import sys
import unittest
from pathlib import Path
from typing import Any, Dict, List

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from tests.fixtures.aliasing.sample_data import get_sample_tags

from modules.contextualization.key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingEngine,
    AliasingResult,
    AliasRule,
    TransformationType,
)


class TestAliasingRuleTypes(unittest.TestCase):
    """Test different aliasing rule types."""

    def setUp(self):
        """Set up test fixtures."""
        self.base_config = {
            "rules": [],
            "validation": {
                "max_aliases_per_tag": 30,
                "min_alias_length": 2,
                "max_alias_length": 50,
            },
        }

    def test_character_substitution_rule(self):
        """Test character substitution aliasing rule."""
        config = self.base_config.copy()
        config["rules"] = [
            {
                "name": "separator_normalization",
                "type": "character_substitution",
                "enabled": True,
                "priority": 10,
                "preserve_original": True,
                "config": {"substitutions": {"_": "-", " ": "-"}},
            }
        ]

        engine = AliasingEngine(config)
        result = engine.generate_aliases("P_10001", "asset")

        self.assertIsInstance(result, AliasingResult)
        self.assertIn("P-10001", result.aliases)
        self.assertIn("P_10001", result.aliases)  # Original preserved

    def test_equipment_type_expansion_rule(self):
        """Test equipment type expansion aliasing rule."""
        config = self.base_config.copy()
        config["rules"] = [
            {
                "name": "equipment_expansion",
                "type": "equipment_type_expansion",
                "enabled": True,
                "priority": 20,
                "preserve_original": True,
                "config": {
                    "type_mappings": {"P": ["PUMP", "PMP"], "V": ["VALVE", "VLV"]},
                    "format_templates": ["{type}-{tag}", "{type}_{tag}"],
                    "auto_detect": True,
                },
            }
        ]

        engine = AliasingEngine(config)
        context = {"equipment_type": "pump"}
        result = engine.generate_aliases("P-10001", "asset", context)

        self.assertIsInstance(result, AliasingResult)
        self.assertIn("PUMP-10001", result.aliases)
        self.assertIn("PMP-10001", result.aliases)
        self.assertIn("PUMP_10001", result.aliases)
        self.assertIn("PMP_10001", result.aliases)


class TestContextHandling(unittest.TestCase):
    """Test context handling in aliasing."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "rules": [
                {
                    "name": "context_test",
                    "type": "equipment_type_expansion",
                    "enabled": True,
                    "priority": 10,
                    "preserve_original": True,
                    "config": {
                        "type_mappings": {"P": ["PUMP", "PMP"], "V": ["VALVE", "VLV"]},
                        "format_templates": ["{type}-{tag}"],
                    },
                }
            ],
            "validation": {
                "max_aliases_per_tag": 30,
                "min_alias_length": 2,
                "max_alias_length": 50,
            },
        }

        self.engine = AliasingEngine(self.config)

    def test_equipment_type_context(self):
        """Test aliasing with equipment type context."""
        context = {"equipment_type": "pump"}
        result = self.engine.generate_aliases("P-10001", "asset", context)

        self.assertIn("PUMP-10001", result.aliases)
        self.assertIn("PMP-10001", result.aliases)

    def test_site_context(self):
        """Test aliasing with site context."""
        context = {"site": "Plant_A", "equipment_type": "pump"}
        result = self.engine.generate_aliases("P-10001", "asset", context)

        # Should still generate equipment type aliases
        self.assertIn("PUMP-10001", result.aliases)

    def test_unit_context(self):
        """Test aliasing with unit context."""
        context = {"unit": "Feed", "equipment_type": "pump"}
        result = self.engine.generate_aliases("P-10001", "asset", context)

        # Should still generate equipment type aliases
        self.assertIn("PUMP-10001", result.aliases)

    def test_multiple_context_values(self):
        """Test aliasing with multiple context values."""
        context = {
            "site": "Plant_A",
            "unit": "Feed",
            "equipment_type": "pump",
            "area": "Process",
        }
        result = self.engine.generate_aliases("P-10001", "asset", context)

        # Should generate aliases based on available context
        self.assertIn("PUMP-10001", result.aliases)

    def test_empty_context(self):
        """Test aliasing with empty context."""
        result = self.engine.generate_aliases("P-10001", "asset", {})

        # Should still generate some aliases
        self.assertIsInstance(result, AliasingResult)
        self.assertGreater(len(result.aliases), 0)

    def test_none_context(self):
        """Test aliasing with None context."""
        result = self.engine.generate_aliases("P-10001", "asset", None)

        # Should handle None context gracefully
        self.assertIsInstance(result, AliasingResult)
        self.assertGreater(len(result.aliases), 0)


class TestAliasValidation(unittest.TestCase):
    """Test alias validation mechanisms."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "rules": [
                {
                    "name": "validation_test",
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

        self.engine = AliasingEngine(self.config)

    def test_minimum_alias_length(self):
        """Test minimum alias length validation."""
        result = self.engine.generate_aliases("P-10001", "asset")

        for alias in result.aliases:
            self.assertGreaterEqual(len(alias), 2)

    def test_maximum_alias_length(self):
        """Test maximum alias length validation."""
        result = self.engine.generate_aliases("P-10001", "asset")

        for alias in result.aliases:
            self.assertLessEqual(len(alias), 50)

    def test_maximum_aliases_per_tag(self):
        """Test maximum aliases per tag limit."""
        # Create config with very low limit
        limited_config = self.config.copy()
        limited_config["validation"]["max_aliases_per_tag"] = 5

        engine = AliasingEngine(limited_config)
        result = engine.generate_aliases("P-10001", "asset")

        self.assertLessEqual(len(result.aliases), 5)

    def test_alias_uniqueness(self):
        """Test that aliases are unique."""
        result = self.engine.generate_aliases("P-10001", "asset")

        # All aliases should be unique
        self.assertEqual(len(result.aliases), len(set(result.aliases)))

    def test_alias_character_validation(self):
        """Test alias character validation."""
        result = self.engine.generate_aliases("P-10001", "asset")

        for alias in result.aliases:
            # Should not contain invalid characters
            self.assertNotIn("!", alias)
            self.assertNotIn("@", alias)
            self.assertNotIn("#", alias)


class TestRulePriorityAndOrdering(unittest.TestCase):
    """Test rule priority and processing order."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "rules": [
                {
                    "name": "low_priority_rule",
                    "type": "character_substitution",
                    "enabled": True,
                    "priority": 100,  # Lower priority (higher number)
                    "preserve_original": True,
                    "config": {"substitutions": {"-": "_"}},
                },
                {
                    "name": "high_priority_rule",
                    "type": "character_substitution",
                    "enabled": True,
                    "priority": 10,  # Higher priority (lower number)
                    "preserve_original": True,
                    "config": {"substitutions": {"_": "-"}},
                },
            ],
            "validation": {
                "max_aliases_per_tag": 30,
                "min_alias_length": 2,
                "max_alias_length": 50,
            },
        }

        self.engine = AliasingEngine(self.config)

    def test_rule_priority_ordering(self):
        """Test that rules are applied in priority order."""
        result = self.engine.generate_aliases("P_10001", "asset")

        # Should include aliases from both rules
        self.assertIn("P-10001", result.aliases)  # From high priority rule
        self.assertIn("P_10001", result.aliases)  # Original preserved

    def test_disabled_rules(self):
        """Test that disabled rules are not applied."""
        disabled_config = self.config.copy()
        disabled_config["rules"][0]["enabled"] = False

        engine = AliasingEngine(disabled_config)
        result = engine.generate_aliases("P_10001", "asset")

        # Should only apply enabled rules
        self.assertIn("P-10001", result.aliases)  # From enabled rule
        self.assertIn("P_10001", result.aliases)  # Original preserved

    def test_rule_conflicts(self):
        """Test handling of conflicting rules."""
        # Rules that might conflict should be handled gracefully
        result = self.engine.generate_aliases("P_10001", "asset")

        # Should produce consistent results
        self.assertIsInstance(result, AliasingResult)
        self.assertGreater(len(result.aliases), 0)


class TestEdgeCasesAndErrorHandling(unittest.TestCase):
    """Test edge cases and error handling in aliasing."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "rules": [
                {
                    "name": "edge_case_test",
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

        self.engine = AliasingEngine(self.config)

    def test_empty_input(self):
        """Test handling of empty input."""
        result = self.engine.generate_aliases("", "asset")

        self.assertIsInstance(result, AliasingResult)
        self.assertEqual(len(result.aliases), 0)

    def test_none_input(self):
        """Test handling of None input."""
        result = self.engine.generate_aliases(None, "asset")

        self.assertIsInstance(result, AliasingResult)
        self.assertEqual(len(result.aliases), 0)

    def test_very_long_input(self):
        """Test handling of very long input."""
        long_tag = "P-" + "1" * 1000
        result = self.engine.generate_aliases(long_tag, "asset")

        self.assertIsInstance(result, AliasingResult)
        # Should respect maximum alias length
        for alias in result.aliases:
            self.assertLessEqual(len(alias), 50)

    def test_special_characters(self):
        """Test handling of special characters."""
        special_tag = "P-10001@#$%^&*()"
        result = self.engine.generate_aliases(special_tag, "asset")

        self.assertIsInstance(result, AliasingResult)
        # Should handle special characters gracefully
        self.assertGreaterEqual(len(result.aliases), 0)

    def test_unicode_characters(self):
        """Test handling of Unicode characters."""
        unicode_tag = "P-10001αβγδε"
        result = self.engine.generate_aliases(unicode_tag, "asset")

        self.assertIsInstance(result, AliasingResult)
        # Should handle Unicode characters
        self.assertGreaterEqual(len(result.aliases), 0)

    def test_invalid_rule_config(self):
        """Test handling of invalid rule configuration."""
        invalid_config = {
            "rules": [
                {
                    "name": "invalid_rule",
                    "type": "character_substitution",
                    "enabled": True,
                    "priority": 10,
                    "preserve_original": True,
                    "config": {"substitutions": None},  # Invalid config
                }
            ],
            "validation": {
                "max_aliases_per_tag": 30,
                "min_alias_length": 2,
                "max_alias_length": 50,
            },
        }

        # Should handle invalid config gracefully
        engine = AliasingEngine(invalid_config)
        result = engine.generate_aliases("P-10001", "asset")

        self.assertIsInstance(result, AliasingResult)

    def test_missing_rule_config(self):
        """Test handling of missing rule configuration."""
        missing_config = {
            "rules": [
                {
                    "name": "missing_config_rule",
                    "type": "character_substitution",
                    "enabled": True,
                    "priority": 10,
                    "preserve_original": True,
                    "config": {},  # Empty config
                }
            ],
            "validation": {
                "max_aliases_per_tag": 30,
                "min_alias_length": 2,
                "max_alias_length": 50,
            },
        }

        engine = AliasingEngine(missing_config)
        result = engine.generate_aliases("P-10001", "asset")

        self.assertIsInstance(result, AliasingResult)


class TestPerformanceAndScalability(unittest.TestCase):
    """Test performance and scalability aspects."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "rules": [
                {
                    "name": "performance_test",
                    "type": "character_substitution",
                    "enabled": True,
                    "priority": 10,
                    "preserve_original": True,
                    "config": {
                        "substitutions": {
                            "-": ["_", " ", ""],
                            "_": ["-", " ", ""],
                            " ": ["-", "_", ""],
                        }
                    },
                }
            ],
            "validation": {
                "max_aliases_per_tag": 30,
                "min_alias_length": 2,
                "max_alias_length": 50,
            },
        }

        self.engine = AliasingEngine(self.config)

    def test_large_number_of_aliases(self):
        """Test generation of large number of aliases."""
        result = self.engine.generate_aliases("P-10001", "asset")

        # Should respect maximum aliases limit
        self.assertLessEqual(len(result.aliases), 30)

    def test_complex_tag_processing(self):
        """Test processing of complex tags."""
        complex_tag = "UNIT1_P101_A_BACKUP"
        result = self.engine.generate_aliases(complex_tag, "asset")

        self.assertIsInstance(result, AliasingResult)
        self.assertGreater(len(result.aliases), 0)

    def test_multiple_tags_processing(self):
        """Test processing multiple tags efficiently."""
        tags = ["P-10001", "V-20001", "T-30001", "E-40001", "C-50001"]

        results = []
        for tag in tags:
            result = self.engine.generate_aliases(tag, "asset")
            results.append(result)

        # All should be processed successfully
        self.assertEqual(len(results), len(tags))
        for result in results:
            self.assertIsInstance(result, AliasingResult)
            self.assertGreater(len(result.aliases), 0)


if __name__ == "__main__":
    unittest.main()
