#!/usr/bin/env python3
"""
Integration Tests for Tag Pattern Library

This module provides integration tests for the Tag Pattern Library,
testing YAML loading, pattern registration, and pattern search.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.contextualization.key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_pattern_library import (
    DocumentPattern,
    DocumentPatternRegistry,
    DocumentType,
    EquipmentType,
    StandardTagPatternRegistry,
    TagPattern,
)


class TestTagPatternLibrary(unittest.TestCase):
    """Test cases for the YAML-based Tag Pattern Library."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary YAML file for testing
        self.temp_yaml = tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        )

        test_yaml_content = {
            "equipment_types": {
                "PUMP": "pump",
                "VALVE": "valve",
                "INSTRUMENT": "instrument",
            },
            "tag_patterns": {
                "pump_patterns": [
                    {
                        "name": "test_pump",
                        "pattern": "\\bP[-_]?\\d{1,6}[A-Z]?\\b",
                        "description": "Test pump pattern",
                        "equipment_type": "PUMP",
                        "examples": ["P-101", "P101A", "P-10001"],
                        "priority": 50,
                        "industry_standard": "ISA",
                        "validation_rules": [],
                    }
                ],
                "valve_patterns": [
                    {
                        "name": "test_valve",
                        "pattern": "\\bV[-_]?\\d{1,6}[A-Z]?\\b",
                        "description": "Test valve pattern",
                        "equipment_type": "VALVE",
                        "examples": ["V-101", "V101A"],
                        "priority": 50,
                        "industry_standard": "ISA",
                        "validation_rules": [],
                    }
                ],
            },
            "document_patterns": [
                {
                    "name": "test_pid",
                    "pattern": "\\bP&?ID[-_]?\\d{4,6}[-_]?[A-Z0-9]*\\b",
                    "description": "Test P&ID pattern",
                    "document_type": "PID",
                    "examples": ["P&ID-2001", "PID_2001_A"],
                    "priority": 30,
                    "required_elements": ["drawing_number"],
                    "optional_elements": ["revision"],
                }
            ],
        }

        yaml.dump(test_yaml_content, self.temp_yaml)
        self.temp_yaml.close()

    def tearDown(self):
        """Clean up test fixtures."""
        os.unlink(self.temp_yaml.name)

    def test_yaml_loading(self):
        """Test loading patterns from YAML file."""
        registry = StandardTagPatternRegistry(self.temp_yaml.name)

        self.assertIsInstance(registry, StandardTagPatternRegistry)
        self.assertGreater(len(registry.patterns), 0)

    def test_pattern_registration(self):
        """Test pattern registration from YAML."""
        registry = StandardTagPatternRegistry(self.temp_yaml.name)

        # Should have loaded test patterns
        self.assertIn("test_pump", registry.patterns)
        self.assertIn("test_valve", registry.patterns)

        pump_pattern = registry.patterns["test_pump"]
        self.assertEqual(pump_pattern.equipment_type, EquipmentType.PUMP)
        self.assertEqual(pump_pattern.priority, 50)

    def test_equipment_type_indexing(self):
        """Test equipment type indexing."""
        registry = StandardTagPatternRegistry(self.temp_yaml.name)

        pump_patterns = registry.get_patterns_by_type(EquipmentType.PUMP)
        self.assertGreater(len(pump_patterns), 0)

        valve_patterns = registry.get_patterns_by_type(EquipmentType.VALVE)
        self.assertGreater(len(valve_patterns), 0)

    def test_pattern_search(self):
        """Test pattern search functionality."""
        registry = StandardTagPatternRegistry(self.temp_yaml.name)

        results = registry.search_patterns("pump")
        self.assertGreater(len(results), 0)

        pump_result = next((r for r in results if r.name == "test_pump"), None)
        self.assertIsNotNone(pump_result)

    def test_document_pattern_loading(self):
        """Test loading document patterns from YAML."""
        doc_registry = DocumentPatternRegistry(self.temp_yaml.name)

        self.assertIsInstance(doc_registry, DocumentPatternRegistry)
        self.assertGreater(len(doc_registry.patterns), 0)

    def test_document_type_indexing(self):
        """Test document type indexing."""
        doc_registry = DocumentPatternRegistry(self.temp_yaml.name)

        pid_patterns = doc_registry.get_patterns_by_type(DocumentType.PID)
        self.assertGreater(len(pid_patterns), 0)

    def test_missing_yaml_file(self):
        """Test handling of missing YAML file."""
        registry = StandardTagPatternRegistry("nonexistent.yaml")

        # Should fall back to default patterns
        self.assertIsInstance(registry, StandardTagPatternRegistry)
        self.assertGreater(len(registry.patterns), 0)

    def test_invalid_yaml_file(self):
        """Test handling of invalid YAML file."""
        # Create invalid YAML file
        invalid_yaml = tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        )
        invalid_yaml.write("invalid: yaml: content: [")
        invalid_yaml.close()

        try:
            registry = StandardTagPatternRegistry(invalid_yaml.name)

            # Should fall back to default patterns
            self.assertIsInstance(registry, StandardTagPatternRegistry)
            self.assertGreater(len(registry.patterns), 0)
        finally:
            os.unlink(invalid_yaml.name)


if __name__ == "__main__":
    unittest.main()
