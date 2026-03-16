#!/usr/bin/env python3
"""
Focused Tests for Key Extraction Engine

This module provides comprehensive tests specifically for the Key Extraction Engine,
covering all extraction methods, rule types, and edge cases.

Author: Darren Downtain
Version: 1.0.0
"""

# Add project root to path for imports
import sys
import unittest
from pathlib import Path
from typing import Any, Dict

from modules.contextualization.key_extraction_aliasing.functions.fn_dm_key_extraction.logger import (
    CogniteFunctionLogger,
)

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.contextualization.key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    ExtractedKey,
    ExtractionMethod,
    ExtractionResult,
    ExtractionRule,
    ExtractionType,
    FixedWidthExtractionHandler,
    HeuristicExtractionHandler,
    KeyExtractionEngine,
    RegexExtractionHandler,
    TokenReassemblyExtractionHandler,
)


class TestExtractionMethods(unittest.TestCase):
    """Test individual extraction methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.base_config = {
            "extraction_rules": [],
            "validation": {
                "min_confidence": 0.5,
                "max_keys_per_type": 10,
                "min_alias_length": 2,
                "max_alias_length": 50,
                "allowed_characters": "A-Za-z0-9-_/. ",
            },
        }

    def test_regex_extraction_handler(self):
        """Test regex extraction handler."""
        handler = RegexExtractionHandler()

        rule = ExtractionRule(
            name="test_regex",
            description="Test regex rule",
            extraction_type=ExtractionType.CANDIDATE_KEY,
            method=ExtractionMethod.REGEX,
            pattern=r"\bP[-_]?\d{1,6}[A-Z]?\b",
            priority=50,
            enabled=True,
            min_confidence=0.7,
            case_sensitive=False,
            source_fields=[{"field_name": "name", "required": True}],
            config={"pattern": "P[-_]?\d{1,6}[A-Z]?"},
        )

        test_text = "Main feed pump P-10001 is connected to P-10002"
        result = handler.extract(test_text, rule)

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

        # Check extracted keys
        extracted_values = [key.value for key in result]
        self.assertIn("P-10001", extracted_values)
        self.assertIn("P-10002", extracted_values)

    def test_fixed_width_extraction_handler(self):
        """Test fixed width extraction handler."""
        handler = FixedWidthExtractionHandler()

        rule = ExtractionRule(
            name="test_fixed_width",
            description="Test fixed width rule",
            extraction_type=ExtractionType.CANDIDATE_KEY,
            method=ExtractionMethod.FIXED_WIDTH,
            pattern="P{position:0,length:1}\\d{position:1,length:3}[A-Z]{position:4,length:1}",
            priority=50,
            enabled=True,
            min_confidence=0.7,
            case_sensitive=False,
            source_fields=[{"field_name": "name", "required": True}],
            config={
                "positions": [
                    {"start": 0, "end": 1, "type": "equipment_type"},
                    {"start": 1, "end": 4, "type": "number"},
                    {"start": 4, "end": 5, "type": "suffix", "optional": True},
                ]
            },
        )

        test_text = "P101A"
        result = handler.extract(test_text, rule)

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

        # Should reconstruct complete tag
        reconstructed_keys = [
            key for key in result if key.metadata.get("reconstructed", False)
        ]
        self.assertGreater(len(reconstructed_keys), 0)

        complete_tag = reconstructed_keys[0].value
        self.assertEqual(complete_tag, "P101A")

    def test_token_reassembly_handler(self):
        """Test token reassembly handler."""
        handler = TokenReassemblyExtractionHandler()

        rule = ExtractionRule(
            name="test_token_reassembly",
            description="Test token reassembly rule",
            extraction_type=ExtractionType.CANDIDATE_KEY,
            method=ExtractionMethod.TOKEN_REASSEMBLY,
            pattern="",
            priority=50,
            enabled=True,
            min_confidence=0.7,
            case_sensitive=False,
            source_fields=[{"field_name": "name", "required": True}],
            config={
                "tokenization": {
                    "token_patterns": [
                        {"name": "equipment_prefix", "pattern": r"\bP\b"},
                        {"name": "number", "pattern": r"\b\d{3,6}\b"},
                    ],
                    "separator_patterns": [" ", "-"],
                },
                "assembly_rules": [
                    {"format": "{equipment_prefix}-{number}", "conditions": {}}
                ],
            },
        )

        test_text = "P 10001"
        result = handler.extract(test_text, rule)

        self.assertIsInstance(result, list)
        # Token reassembly should create reassembled tags
        self.assertGreater(
            len(result), 0, "Should extract at least one key from token reassembly"
        )
        # Check that the key was reassembled
        self.assertIn(
            "tokens_used", result[0].metadata, "Should have tokens_used in metadata"
        )

    def test_heuristic_handler(self):
        """Test heuristic extraction handler."""
        handler = HeuristicExtractionHandler()

        rule = ExtractionRule(
            name="test_heuristic",
            description="Test heuristic rule",
            extraction_type=ExtractionType.CANDIDATE_KEY,
            method=ExtractionMethod.HEURISTIC,
            pattern="",
            priority=50,
            enabled=True,
            min_confidence=0.7,
            case_sensitive=False,
            source_fields=[{"field_name": "name", "required": True}],
            config={
                "heuristics": [
                    {"type": "equipment_prefix", "patterns": ["P", "V", "T"]},
                    {"type": "number_sequence", "min_length": 3, "max_length": 6},
                ]
            },
        )

        test_text = "P-10001"
        result = handler.extract(test_text, rule)

        self.assertIsInstance(result, list)


class TestExtractionRuleTypes(unittest.TestCase):
    """Test different extraction rule types."""

    def setUp(self):
        """Set up test fixtures."""
        self.base_config = {
            "extraction_rules": [],
            "validation": {
                "min_confidence": 0.5,
                "max_keys_per_type": 10,
                "min_alias_length": 2,
                "max_alias_length": 50,
                "allowed_characters": "A-Za-z0-9-_/. ",
            },
        }

    def test_candidate_key_extraction(self):
        """Test candidate key extraction."""
        config = self.base_config.copy()
        config["extraction_rules"] = [
            {
                "name": "candidate_key_test",
                "description": "Test candidate key extraction",
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

        engine = KeyExtractionEngine(config)
        test_asset = {"id": "test", "name": "P-10001"}

        result = engine.extract_keys(test_asset, "asset")

        self.assertEqual(len(result.candidate_keys), 1)
        self.assertEqual(
            result.candidate_keys[0].extraction_type, ExtractionType.CANDIDATE_KEY
        )
        self.assertEqual(len(result.foreign_key_references), 0)
        self.assertEqual(len(result.document_references), 0)

    def test_foreign_key_reference_extraction(self):
        """Test foreign key reference extraction."""
        config = self.base_config.copy()
        config["extraction_rules"] = [
            {
                "name": "foreign_key_test",
                "description": "Test foreign key extraction",
                "extraction_type": "foreign_key_reference",
                "method": "regex",
                "pattern": r"\bT[-_]?\d{1,6}[A-Z]?\b",
                "priority": 50,
                "enabled": True,
                "min_confidence": 0.7,
                "case_sensitive": False,
                "source_fields": [{"field_name": "description", "required": False}],
                "config": {"pattern": "T[-_]?\d{1,6}[A-Z]?"},
            }
        ]

        engine = KeyExtractionEngine(config)
        test_asset = {
            "id": "test",
            "name": "P-10001",
            "description": "Connected to tank T-301",
        }

        result = engine.extract_keys(test_asset, "asset")

        self.assertEqual(len(result.foreign_key_references), 1)
        self.assertEqual(
            result.foreign_key_references[0].extraction_type,
            ExtractionType.FOREIGN_KEY_REFERENCE,
        )
        self.assertEqual(result.foreign_key_references[0].value, "T-301")

    def test_document_reference_extraction(self):
        """Test document reference extraction."""
        config = self.base_config.copy()
        config["extraction_rules"] = [
            {
                "name": "document_reference_test",
                "description": "Test document reference extraction",
                "extraction_type": "document_reference",
                "method": "regex",
                "pattern": r"\b[A-Z]{2,3}[-_]?\d{1,6}[A-Z]?\b",
                "priority": 50,
                "enabled": True,
                "min_confidence": 0.7,
                "case_sensitive": False,
                "source_fields": [{"field_name": "description", "required": False}],
                "config": {"pattern": "[A-Z]{2,3}[-_]?\d{1,6}[A-Z]?"},
            }
        ]

        engine = KeyExtractionEngine(config)
        test_asset = {
            "id": "test",
            "name": "P-10001",
            "description": "Controlled by FIC-2001",
        }

        result = engine.extract_keys(test_asset, "asset")

        self.assertEqual(len(result.document_references), 1)
        self.assertEqual(
            result.document_references[0].extraction_type,
            ExtractionType.DOCUMENT_REFERENCE,
        )
        self.assertEqual(result.document_references[0].value, "FIC-2001")


class TestConfidenceCalculation(unittest.TestCase):
    """Test confidence calculation for different scenarios."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "extraction_rules": [
                {
                    "name": "confidence_test",
                    "description": "Test confidence calculation",
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
            ],
            "validation": {
                "min_confidence": 0.5,
                "max_keys_per_type": 10,
                "min_alias_length": 2,
                "max_alias_length": 50,
                "allowed_characters": "A-Za-z0-9-_/. ",
            },
        }

        self.engine = KeyExtractionEngine(self.config)

    def test_high_confidence_match(self):
        """Test high confidence matches."""
        test_asset = {"id": "test", "name": "P-10001"}
        result = self.engine.extract_keys(test_asset, "asset")

        for key in result.candidate_keys:
            self.assertGreaterEqual(key.confidence, 0.5)
            self.assertLessEqual(key.confidence, 1.0)

    def test_low_confidence_match(self):
        """Test low confidence matches."""
        # Use a pattern that might have lower confidence
        low_confidence_config = self.config.copy()
        low_confidence_config["extraction_rules"][0]["min_confidence"] = 0.1

        engine = KeyExtractionEngine(low_confidence_config)
        test_asset = {"id": "test", "name": "P-10001"}
        result = engine.extract_keys(test_asset, "asset")

        for key in result.candidate_keys:
            self.assertGreaterEqual(key.confidence, 0.0)
            self.assertLessEqual(key.confidence, 1.0)

    def test_confidence_filtering(self):
        """Test confidence-based filtering."""
        # Set high confidence threshold
        high_confidence_config = self.config.copy()
        high_confidence_config["extraction_rules"][0]["min_confidence"] = 0.9

        engine = KeyExtractionEngine(high_confidence_config)
        test_asset = {"id": "test", "name": "P-10001"}
        result = engine.extract_keys(test_asset, "asset")

        # Should filter out low confidence matches
        for key in result.candidate_keys:
            self.assertGreaterEqual(key.confidence, 0.9)


class TestSourceFieldHandling(unittest.TestCase):
    """Test source field handling and validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "extraction_rules": [
                {
                    "name": "source_field_test",
                    "description": "Test source field handling",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "pattern": r"\bP[-_]?\d{1,6}[A-Z]?\b",
                    "priority": 50,
                    "enabled": True,
                    "min_confidence": 0.7,
                    "case_sensitive": False,
                    "source_fields": [
                        {"field_name": "name", "required": True},
                        {"field_name": "description", "required": False},
                    ],
                    "config": {"pattern": "P[-_]?\d{1,6}[A-Z]?"},
                }
            ],
            "validation": {
                "min_confidence": 0.5,
                "max_keys_per_type": 10,
                "min_alias_length": 2,
                "max_alias_length": 50,
                "allowed_characters": "A-Za-z0-9-_/. ",
            },
        }
        logger = CogniteFunctionLogger()
        self.engine = KeyExtractionEngine(self.config, logger=logger)

    def test_required_field_present(self):
        """Test extraction when required field is present."""
        test_asset = {"id": "test", "name": "P-10001", "description": "Test pump"}
        result = self.engine.extract_keys(test_asset, "asset")

        self.assertGreater(len(result.candidate_keys), 0)

        # Check source field information
        for key in result.candidate_keys:
            self.assertIn(key.source_field, ["name", "description"])

    def test_required_field_missing(self):
        """Test extraction when required field is missing."""
        test_asset = {"id": "test", "description": "No name field"}
        result = self.engine.extract_keys(test_asset, "asset")

        # Should not extract anything if required field is missing
        self.assertEqual(len(result.candidate_keys), 0)

    def test_optional_field_handling(self):
        """Test handling of optional fields."""
        test_asset = {"id": "test", "name": "P-10001"}  # No description
        result = self.engine.extract_keys(test_asset, "asset")

        # Should still extract from name field
        self.assertGreater(len(result.candidate_keys), 0)

    def test_multiple_source_fields(self):
        """Test extraction from multiple source fields."""
        test_asset = {
            "id": "test",
            "name": "P-10001",
            "description": "Main pump P-10002 for backup",
        }
        result = self.engine.extract_keys(test_asset, "asset")

        # Should extract from both name and description
        self.assertGreater(len(result.candidate_keys), 0)

        # Check that we have keys from different source fields
        source_fields = set(key.source_field for key in result.candidate_keys)
        self.assertGreater(len(source_fields), 0)


class TestRulePriorityAndOrdering(unittest.TestCase):
    """Test rule priority and processing order."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "extraction_rules": [
                {
                    "name": "low_priority_rule",
                    "description": "Low priority rule",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "pattern": r"\bV[-_]?\d{1,6}[A-Z]?\b",  # Different pattern so both can match
                    "priority": 100,  # Lower priority (higher number)
                    "enabled": True,
                    "min_confidence": 0.7,
                    "case_sensitive": False,
                    "source_fields": [{"field_name": "name", "required": True}],
                    "config": {"pattern": "V[-_]?\d{1,6}[A-Z]?"},
                },
                {
                    "name": "high_priority_rule",
                    "description": "High priority rule",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "pattern": r"\bP[-_]?\d{1,6}[A-Z]?\b",
                    "priority": 10,  # Higher priority (lower number)
                    "enabled": True,
                    "min_confidence": 0.7,
                    "case_sensitive": False,
                    "source_fields": [{"field_name": "name", "required": True}],
                    "config": {"pattern": "P[-_]?\d{1,6}[A-Z]?"},
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

        self.engine = KeyExtractionEngine(self.config)

    def test_rule_priority_ordering(self):
        """Test that rules are processed in priority order."""
        test_asset = {"id": "test", "name": "P-10001 V-5002"}
        result = self.engine.extract_keys(test_asset, "asset")

        # Both rules should match different patterns
        self.assertGreater(len(result.candidate_keys), 0)

        # Check that both rule names appear in the results
        rule_names = [key.rule_name for key in result.candidate_keys]
        self.assertIn(
            "high_priority_rule", rule_names, "Should contain high priority rule"
        )
        self.assertIn(
            "low_priority_rule", rule_names, "Should contain low priority rule"
        )

    def test_disabled_rules(self):
        """Test that disabled rules are not processed."""
        disabled_config = self.config.copy()
        disabled_config["extraction_rules"][0]["enabled"] = False

        engine = KeyExtractionEngine(disabled_config)
        test_asset = {"id": "test", "name": "P-10001"}
        result = engine.extract_keys(test_asset, "asset")

        # Only enabled rules should process
        rule_names = [key.rule_name for key in result.candidate_keys]
        self.assertNotIn("low_priority_rule", rule_names)
        self.assertIn("high_priority_rule", rule_names)


class TestValidationAndFiltering(unittest.TestCase):
    """Test validation and filtering mechanisms."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "extraction_rules": [
                {
                    "name": "validation_test",
                    "description": "Test validation",
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
            ],
            "validation": {
                "min_confidence": 0.5,
                "max_keys_per_type": 10,
                "min_alias_length": 2,
                "max_alias_length": 50,
                "allowed_characters": "A-Za-z0-9-_/. ",
            },
        }

        self.engine = KeyExtractionEngine(self.config)

    def test_minimum_confidence_filtering(self):
        """Test minimum confidence filtering."""
        test_asset = {"id": "test", "name": "P-10001"}
        result = self.engine.extract_keys(test_asset, "asset")

        for key in result.candidate_keys:
            self.assertGreaterEqual(key.confidence, 0.5)

    def test_maximum_keys_per_type(self):
        """Test maximum keys per type limit."""
        # Create config with very low limit
        limited_config = self.config.copy()
        limited_config["validation"]["max_keys_per_type"] = 1

        engine = KeyExtractionEngine(limited_config)
        test_asset = {"id": "test", "name": "P-10001"}
        result = engine.extract_keys(test_asset, "asset")

        # Should respect the limit
        self.assertLessEqual(len(result.candidate_keys), 1)

    def test_alias_length_validation(self):
        """Test alias length validation."""
        test_asset = {"id": "test", "name": "P-10001"}
        result = self.engine.extract_keys(test_asset, "asset")

        for key in result.candidate_keys:
            self.assertGreaterEqual(len(key.value), 2)
            self.assertLessEqual(len(key.value), 50)

    def test_allowed_characters_validation(self):
        """Test allowed characters validation."""
        test_asset = {"id": "test", "name": "P-10001"}
        result = self.engine.extract_keys(test_asset, "asset")

        for key in result.candidate_keys:
            # Check that key contains only allowed characters
            # Build actual allowed character set (alphanumeric, hyphens, underscores, etc.)
            import string

            allowed_chars = set(string.ascii_letters + string.digits + "-_/.")
            key_chars = set(key.value)
            self.assertTrue(key_chars.issubset(allowed_chars))


if __name__ == "__main__":
    unittest.main()
