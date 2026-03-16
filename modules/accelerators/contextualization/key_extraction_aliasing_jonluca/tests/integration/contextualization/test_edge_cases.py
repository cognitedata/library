#!/usr/bin/env python3
"""
Integration Tests for Edge Cases and Error Handling

This module provides integration tests for edge cases and error handling
across the key extraction and aliasing workflows.
"""

import sys
import unittest
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.contextualization.key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    ExtractionResult,
    KeyExtractionEngine,
)


class TestEdgeCasesAndErrorHandling(unittest.TestCase):
    """Test edge cases and error handling scenarios."""

    def setUp(self):
        """Set up test fixtures."""
        self.minimal_config = {
            "extraction_rules": [],
            "validation": {
                "min_confidence": 0.5,
                "max_keys_per_type": 10,
                "min_alias_length": 2,
                "max_alias_length": 50,
                "allowed_characters": "A-Za-z0-9-_/. ",
            },
        }

    def test_empty_configuration(self):
        """Test handling of empty configuration."""
        engine = KeyExtractionEngine(self.minimal_config)

        test_asset = {"id": "test", "name": "P-10001"}
        result = engine.extract_keys(test_asset, "asset")

        self.assertIsInstance(result, ExtractionResult)
        self.assertEqual(len(result.candidate_keys), 0)

    def test_invalid_regex_pattern(self):
        """Test handling of invalid regex patterns."""
        invalid_config = self.minimal_config.copy()
        invalid_config["extraction_rules"] = [
            {
                "name": "invalid_regex",
                "description": "Invalid regex pattern",
                "extraction_type": "candidate_key",
                "method": "regex",
                "pattern": "[invalid",  # Invalid regex
                "priority": 50,
                "enabled": True,
                "min_confidence": 0.7,
                "case_sensitive": False,
                "source_fields": [{"field_name": "name", "required": True}],
                "config": {},
            }
        ]

        # Should not raise exception, but handle gracefully
        engine = KeyExtractionEngine(invalid_config)
        test_asset = {"id": "test", "name": "P-10001"}
        result = engine.extract_keys(test_asset, "asset")

        self.assertIsInstance(result, ExtractionResult)

    def test_very_long_text(self):
        """Test handling of very long text input."""
        engine = KeyExtractionEngine(self.minimal_config)

        # Create very long text
        long_text = "P-10001 " * 10000
        test_asset = {"id": "test", "name": long_text}

        result = engine.extract_keys(test_asset, "asset")

        self.assertIsInstance(result, ExtractionResult)

    def test_special_characters(self):
        """Test handling of special characters in input."""
        engine = KeyExtractionEngine(self.minimal_config)

        special_asset = {
            "id": "test",
            "name": "P-10001@#$%^&*()",
            "description": "Pump with special chars: !@#$%^&*()",
        }

        result = engine.extract_keys(special_asset, "asset")

        self.assertIsInstance(result, ExtractionResult)

    def test_unicode_characters(self):
        """Test handling of Unicode characters."""
        engine = KeyExtractionEngine(self.minimal_config)

        unicode_asset = {
            "id": "test",
            "name": "P-10001",
            "description": "Pump with unicode: αβγδε",
        }

        result = engine.extract_keys(unicode_asset, "asset")

        self.assertIsInstance(result, ExtractionResult)

    def test_none_values(self):
        """Test handling of None values in input."""
        engine = KeyExtractionEngine(self.minimal_config)

        none_asset = {"id": "test", "name": None, "description": None, "metadata": None}

        result = engine.extract_keys(none_asset, "asset")

        self.assertIsInstance(result, ExtractionResult)

    def test_missing_required_fields(self):
        """Test handling of missing required fields."""
        config_with_required = self.minimal_config.copy()
        config_with_required["extraction_rules"] = [
            {
                "name": "required_field_test",
                "description": "Test required field",
                "extraction_type": "candidate_key",
                "method": "regex",
                "pattern": r"\bP[-_]?\d{1,6}[A-Z]?\b",
                "priority": 50,
                "enabled": True,
                "min_confidence": 0.7,
                "case_sensitive": False,
                "source_fields": [{"field_name": "name", "required": True}],
                "config": {"pattern": "P[-_]?\d{1,6}[A-Z]?"},
            }
        ]

        engine = KeyExtractionEngine(config_with_required)

        # Asset without required field
        incomplete_asset = {"id": "test", "description": "No name field"}
        result = engine.extract_keys(incomplete_asset, "asset")

        self.assertIsInstance(result, ExtractionResult)
        self.assertEqual(len(result.candidate_keys), 0)


if __name__ == "__main__":
    unittest.main()
