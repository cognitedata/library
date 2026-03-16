#!/usr/bin/env python3
"""
Tests for Key Extraction Scenarios

This module provides tests for specific key extraction scenarios including:
- ISO standard instrument tag extraction
- Fixed width parsing on timeseries metadata
- Token reassembly extraction

Author: Darren Downtain
Version: 1.0.0
"""

import re

# Add project root to path for imports
import sys
import unittest
from pathlib import Path
from typing import Any, Dict, List

project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.contextualization.key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)


class TestRegexExtraction(unittest.TestCase):
    """Test key extraction on ISO standard instrument names."""

    def setUp(self):
        """Set up test fixtures with ISO standard assets."""
        self.sample_assets = [
            {
                "id": "asset_001",
                "externalId": "ARC-1001",
                "name": "FIC-1001",
                "description": "Flow Indicator Controller for process line P-101 feeding Tank T-201",
                "metadata": {
                    "site": "Plant_A",
                    "unit": "Unit_100",
                    "equipmentType": "instrument",
                    "instrumentType": "flow_control",
                },
            },
            {
                "id": "asset_002",
                "externalId": "ARC-1002",
                "name": "PIC-2001",
                "description": "Pressure Indicator Controller monitoring V-301, operated by FIC-2001",
                "metadata": {
                    "site": "Plant_A",
                    "unit": "Unit_200",
                    "equipmentType": "instrument",
                    "instrumentType": "pressure_control",
                },
            },
            {
                "id": "asset_003",
                "externalId": "ARC-1003",
                "name": "TIC-3001",
                "description": "Temperature Indicator Controller for Reactor R-401 setpoint reference",
                "metadata": {
                    "site": "Plant_B",
                    "unit": "Unit_300",
                    "equipmentType": "instrument",
                    "instrumentType": "temperature_control",
                },
            },
            {
                "id": "asset_004",
                "externalId": "ARC-1004",
                "name": "LIC-4001",
                "description": "Level Indicator Controller maintaining level in Tank T-501, linked to pump P-601",
                "metadata": {
                    "site": "Plant_B",
                    "unit": "Unit_400",
                    "equipmentType": "instrument",
                    "instrumentType": "level_control",
                },
            },
            {
                "id": "asset_005",
                "externalId": "ARC-1005",
                "name": "A-FIC-1001",
                "description": "Flow Indicator Controller for Unit A, process line A-P-101",
                "metadata": {
                    "site": "Plant_A",
                    "unit": "Unit_A",
                    "equipmentType": "instrument",
                    "instrumentType": "flow_control",
                },
            },
            {
                "id": "asset_006",
                "externalId": "ARC-1006",
                "name": "B-PIC-2001",
                "description": "Pressure Indicator Controller for Unit B, monitoring B-V-301",
                "metadata": {
                    "site": "Plant_B",
                    "unit": "Unit_B",
                    "equipmentType": "instrument",
                    "instrumentType": "pressure_control",
                },
            },
        ]

        self.config = {
            "extraction_rules": [
                {
                    "name": "isa_instrument_tag_extraction",
                    "description": "Extracts ISA standard instrument tags (FIC, PIC, TIC, LIC, etc.)",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "pattern": r"\b[A-Z]{1,2}[-]?[A-Z]{2,3}[-_]?\d{4}[A-Z]?\b",
                    "priority": 50,
                    "enabled": True,
                    "min_confidence": 0.8,
                    "case_sensitive": False,
                    "source_fields": [
                        {
                            "field_name": "name",
                            "field_type": "string",
                            "required": True,
                            "priority": 1,
                        }
                    ],
                    "config": {"pattern": "[A-Z]{1,2}[-]?[A-Z]{2,3}[-_]?\d{4}[A-Z]?"},
                },
                {
                    "name": "pump_tag_extraction",
                    "description": "Extracts pump tags (P-XXX)",
                    "extraction_type": "foreign_key_reference",
                    "method": "regex",
                    "pattern": r"\bP[-_]?\d{3}[A-Z]?\b",
                    "priority": 40,
                    "enabled": True,
                    "min_confidence": 0.7,
                    "case_sensitive": False,
                    "source_fields": [
                        {
                            "field_name": "description",
                            "field_type": "string",
                            "required": False,
                            "priority": 2,
                        }
                    ],
                    "config": {"pattern": "P[-_]?\d{3}[A-Z]?"},
                },
            ],
            "validation": {"min_confidence": 0.5, "max_keys_per_type": 20},
        }

    def test_regex_extraction(self):
        """Test extraction on ISO standard assets."""
        engine = KeyExtractionEngine(self.config)

        results = []
        for asset in self.sample_assets:
            result = engine.extract_keys(asset, "asset")
            results.append((asset, result))

        # Check all assets were processed
        self.assertEqual(len(results), 6)

        # Check that all assets extracted at least one candidate key
        for asset, result in results:
            self.assertGreater(
                len(result.candidate_keys),
                0,
                f"Asset {asset['name']} should extract at least one key",
            )

        # Verify specific extractions
        fic_result = next((r for _, r in results if r.entity_id == "asset_001"), None)
        self.assertIsNotNone(fic_result)
        extracted_values = [k.value for k in fic_result.candidate_keys]
        self.assertIn("FIC-1001", extracted_values)

    def test_contains_instrument_name(self):
        """Test that extracted keys contain the instrument name."""
        engine = KeyExtractionEngine(self.config)

        for asset in self.sample_assets:
            result = engine.extract_keys(asset, "asset")
            extracted_values = [k.value for k in result.candidate_keys]

            # The asset's own name should be extracted
            self.assertIn(
                asset["name"],
                extracted_values,
                f"Asset {asset['id']} should extract its own name: {asset['name']}",
            )

    def test_unit_prefix_extraction(self):
        """Test that unit-prefixed tags are extracted correctly."""
        engine = KeyExtractionEngine(self.config)

        # Test asset with unit prefix
        unit_asset = self.sample_assets[4]  # A-FIC-1001
        result = engine.extract_keys(unit_asset, "asset")

        extracted_values = [k.value for k in result.candidate_keys]

        # Should extract the unit-prefixed tag
        self.assertIn("A-FIC-1001", extracted_values)

        # Test another unit-prefixed asset
        unit_asset2 = self.sample_assets[5]  # B-PIC-2001
        result2 = engine.extract_keys(unit_asset2, "asset")
        extracted_values2 = [k.value for k in result2.candidate_keys]
        self.assertIn("B-PIC-2001", extracted_values2)


class TestFixedWidthExtraction(unittest.TestCase):
    """Test fixed width parsing on timeseries metadata."""

    def setUp(self):
        """Set up test fixtures with fixed width timeseries."""
        self.timeseries_records = [
            {
                "id": "ts_001",
                "externalId": "TS-FIC-1001_VALUE",
                "name": "FIC1001         FIC-1001       FLOW INDICATOR CONTROLLER VALUE",
                "description": "Timeseries for Flow Indicator Controller FIC-1001",
                "metadata": {"unit": "L/h", "dataType": "DOUBLE", "isStep": False},
            },
            {
                "id": "ts_002",
                "externalId": "TS-PIC-2001_VALUE",
                "name": "PIC2001         PIC-2001       PRESSURE INDICATOR CONTROLLER VALUE",
                "description": "Timeseries for Pressure Indicator Controller PIC-2001",
                "metadata": {"unit": "bar", "dataType": "DOUBLE", "isStep": False},
            },
            {
                "id": "ts_003",
                "externalId": "TS-TIC-3001_VALUE",
                "name": "TIC3001         TIC-3001       TEMPERATURE INDICATOR CONTROLLER VALUE",
                "description": "Timeseries for Temperature Indicator Controller TIC-3001",
                "metadata": {"unit": "Celsius", "dataType": "DOUBLE", "isStep": False},
            },
            {
                "id": "ts_004",
                "externalId": "TS-A-FIC-1001_VALUE",
                "name": "AFIC1001        A-FIC-1001     UNIT A FLOW INDICATOR CONTROLLER VALUE",
                "description": "Timeseries for Unit A Flow Indicator Controller",
                "metadata": {"unit": "L/h", "dataType": "DOUBLE", "isStep": False},
            },
        ]

        self.config = {
            "extraction_rules": [
                {
                    "name": "position_based_timeseries_tag",
                    "description": "Extract tags using position-based fixed width configuration",
                    "extraction_type": "candidate_key",
                    "method": "fixed_width",
                    "pattern": ".*",
                    "priority": 70,
                    "enabled": True,
                    "min_confidence": 0.9,
                    "case_sensitive": False,
                    "source_fields": [
                        {
                            "field_name": "externalId",
                            "field_type": "string",
                            "required": True,
                            "priority": 1,
                        }
                    ],
                    "config": {
                        "positions": [
                            {
                                "start": 3,
                                "end": 11,
                                "type": "tag_with_separator",
                                "optional": False,
                            }
                        ],
                        "padding": "space",
                        "line_pattern": None,
                        "skip_lines": 0,
                        "stop_on_empty": False,
                    },
                }
            ],
            "validation": {"min_confidence": 0.5, "max_keys_per_type": 10},
        }

    def test_fixed_width_extraction(self):
        """Test fixed width parsing extracts tags correctly."""
        engine = KeyExtractionEngine(self.config)

        results = []
        for ts in self.timeseries_records:
            result = engine.extract_keys(ts, "timeseries")
            results.append((ts, result))

        # Check all records were processed
        self.assertEqual(len(results), 4)

        # Check that all records extracted at least one key
        for ts, result in results:
            self.assertGreater(
                len(result.candidate_keys),
                0,
                f"Timeseries {ts['id']} should extract at least one key",
            )

        # Verify specific extractions
        fic_result = next((r for _, r in results if r.entity_id == "ts_001"), None)
        self.assertIsNotNone(fic_result)
        extracted_values = [k.value for k in fic_result.candidate_keys]
        self.assertIn("FIC-1001", extracted_values)

    def test_fixed_width_position_accuracy(self):
        """Test that fixed width parsing extracts from correct position."""
        engine = KeyExtractionEngine(self.config)

        # Position 3-11 should extract "FIC-1001" from externalId "TS-FIC-1001_VALUE"
        result = engine.extract_keys(self.timeseries_records[0], "timeseries")
        extracted_values = [k.value for k in result.candidate_keys]
        self.assertIn("FIC-1001", extracted_values)


class TestTokenReassemblyExtraction(unittest.TestCase):
    """Test token reassembly extraction on timeseries externalId."""

    def setUp(self):
        """Set up test fixtures with timeseries for token reassembly."""
        self.timeseries_records = [
            {
                "id": "ts_001",
                "externalId": "TS-FIC-1001_VALUE",
                "name": "FIC1001         FIC-1001       FLOW INDICATOR CONTROLLER VALUE",
                "description": "Timeseries for Flow Indicator Controller FIC-1001",
                "metadata": {"unit": "L/h", "dataType": "DOUBLE", "isStep": False},
            },
            {
                "id": "ts_002",
                "externalId": "TS-PIC-2001_VALUE",
                "name": "PIC2001         PIC-2001       PRESSURE INDICATOR CONTROLLER VALUE",
                "description": "Timeseries for Pressure Indicator Controller PIC-2001",
                "metadata": {"unit": "bar", "dataType": "DOUBLE", "isStep": False},
            },
            {
                "id": "ts_003",
                "externalId": "TS-TIC-3001_VALUE",
                "name": "TIC3001         TIC-3001       TEMPERATURE INDICATOR CONTROLLER VALUE",
                "description": "Timeseries for Temperature Indicator Controller TIC-3001",
                "metadata": {"unit": "Celsius", "dataType": "DOUBLE", "isStep": False},
            },
            {
                "id": "ts_004",
                "externalId": "TS-LIC-4001_VALUE",
                "name": "LIC4001         LIC-4001       LEVEL INDICATOR CONTROLLER VALUE",
                "description": "Timeseries for Level Indicator Controller LIC-4001",
                "metadata": {"unit": "mm", "dataType": "DOUBLE", "isStep": False},
            },
            {
                "id": "ts_005",
                "externalId": "TS-FCV-5001_VALUE",
                "name": "FCV5001         FCV-5001       FLOW CONTROL VALVE POSITION VALUE",
                "description": "Timeseries for Flow Control Valve FCV-5001",
                "metadata": {"unit": "%", "dataType": "DOUBLE", "isStep": False},
            },
            {
                "id": "ts_006",
                "externalId": "UNIT-A-FIC-1001_VALUE",
                "name": "UNITAFIC1001     UNIT-A-FIC-1001 UNIT A FLOW INDICATOR CONTROLLER VALUE",
                "description": "Timeseries for Unit A Flow Indicator Controller",
                "metadata": {"unit": "L/h", "dataType": "DOUBLE", "isStep": False},
            },
        ]

        self.config = {
            "extraction_rules": [
                {
                    "name": "individual_token_extraction",
                    "description": "Extract individual tokens as keys",
                    "extraction_type": "candidate_key",
                    "method": "token_reassembly",
                    "pattern": "",
                    "priority": 40,
                    "enabled": True,
                    "min_confidence": 0.80,
                    "case_sensitive": False,
                    "source_fields": [
                        {
                            "field_name": "externalId",
                            "field_type": "string",
                            "required": True,
                            "priority": 1,
                        }
                    ],
                    "config": {
                        "tokenization": {
                            "token_patterns": [
                                {
                                    "name": "tag_prefix",
                                    "pattern": r"^(FIC|PIC|TIC|LIC|FCV)$",
                                    "position": 1,
                                    "required": True,
                                    "component_type": "instrument_tag",
                                },
                                {
                                    "name": "tag_number",
                                    "pattern": r"^\d+$",
                                    "position": 2,
                                    "required": True,
                                    "component_type": "number",
                                },
                            ],
                            "separator_patterns": ["-", "_"],
                        },
                        "assembly_rules": [
                            {
                                "format": "{tag_prefix}-{tag_number}",
                                "conditions": {"all_required_present": True},
                            }
                        ],
                        "validation": {
                            "validate_assembled": True,
                            "validation_pattern": r"^[A-Z]{2,4}-\d{4}$",
                        },
                    },
                }
            ],
            "validation": {"min_confidence": 0.5, "max_keys_per_type": 10},
        }

    def test_token_reassembly_extraction(self):
        """Test token reassembly on externalId extracts tags at [1,2]."""
        engine = KeyExtractionEngine(self.config)

        results = []
        for ts in self.timeseries_records:
            result = engine.extract_keys(ts, "timeseries")
            results.append((ts, result))

        # Check all records were processed
        self.assertEqual(len(results), 6)

        # Check that all records extracted at least one key
        for ts, result in results:
            self.assertGreater(
                len(result.candidate_keys),
                0,
                f"Timeseries {ts['id']} should extract at least one key",
            )

        # Verify specific extractions
        fic_result = next((r for _, r in results if r.entity_id == "ts_001"), None)
        self.assertIsNotNone(fic_result)
        extracted_values = [k.value for k in fic_result.candidate_keys]
        self.assertIn("FIC-1001", extracted_values)

    def test_token_reassembly_format(self):
        """Test that token reassembly produces appropriately formatted tags."""
        engine = KeyExtractionEngine(self.config)

        result = engine.extract_keys(self.timeseries_records[0], "timeseries")

        # Check that extracted tag matches pattern [A-Z]{2,4}-\d{4}
        for key in result.candidate_keys:
            self.assertTrue(
                re.match(r"^[A-Z]{2,4}-\d{4}$", key.value),
                f"Extracted tag {key.value} should match pattern [A-Z]{{2,4}}-\\d{{4}}",
            )


class TestHeuristicExtraction(unittest.TestCase):
    """Test heuristic key extraction using multiple strategies."""

    def setUp(self):
        """Set up test fixtures with assets for heuristic extraction."""
        from ....tests.fixtures.key_extraction.sample_data import (
            _extract_properties_from_cdm,
            get_heuristic_test_assets,
        )

        # Get assets and flatten them (convert from CDM structure to flat structure)
        raw_assets = get_heuristic_test_assets()
        self.test_assets = [_extract_properties_from_cdm(asset) for asset in raw_assets]

        # Configure extraction engine with heuristic extraction rules
        self.config = {
            "extraction_rules": [
                {
                    "name": "heuristic_positional_detection",
                    "description": "Extract tags using positional detection heuristics",
                    "extraction_type": "foreign_key_reference",
                    "method": "heuristic",
                    "priority": 50,
                    "enabled": True,
                    "min_confidence": 0.4,
                    "source_fields": [{"field_name": "description", "required": True}],
                    "config": {
                        "heuristic_strategies": [
                            {
                                "name": "positional_detection",
                                "weight": 0.4,
                                "rules": [
                                    {
                                        "position": "after_keyword",
                                        "pattern": r"[A-Z0-9-_]{3,15}",
                                        "keywords": [
                                            "tag:",
                                            "Tag:",
                                            "measurement point:",
                                            "uses pump",
                                        ],
                                        "confidence_boost": 0.1,
                                    },
                                    {
                                        "position": "in_parentheses",
                                        "pattern": r"[A-Z0-9-_]{3,15}",
                                        "confidence_boost": 0.05,
                                    },
                                    {
                                        "position": "start_of_field",
                                        "pattern": r"[A-Z]{2,4}[-]?\d{3,5}[A-Z]?",
                                        "confidence_boost": 0.15,
                                    },
                                ],
                            },
                            {
                                "name": "frequency_analysis",
                                "weight": 0.3,
                                "rules": [{"analyze_corpus": True}],
                            },
                            {
                                "name": "context_inference",
                                "weight": 0.2,
                                "rules": [
                                    {
                                        "surrounding_keywords": {
                                            "positive": [
                                                "pump",
                                                "valve",
                                                "tank",
                                                "control",
                                                "vessel",
                                                "equipment",
                                            ],
                                            "negative": [
                                                "document",
                                                "see",
                                                "reference",
                                            ],
                                            "keyword_proximity_bonus": 0.1,
                                        },
                                        "context_window": 30,
                                    }
                                ],
                            },
                            {
                                "name": "example_based_learning",
                                "weight": 0.1,
                                "rules": [{"learning_mode": "similarity"}],
                            },
                        ],
                        "scoring": {
                            "confidence_modifiers": [
                                {
                                    "condition": "extracted_value_length",
                                    "range": [3, 12],
                                    "modifier": "+0.05",
                                }
                            ]
                        },
                    },
                }
            ],
            "validation": {"min_confidence": 0.4, "max_keys_per_type": 20},
        }

    def test_positional_detection_after_keyword(self):
        """Test heuristic extraction using positional detection after keywords."""
        engine = KeyExtractionEngine(self.config)

        # Test asset with "Tag:" keyword
        result = engine.extract_keys(self.test_assets[0], "asset")

        # Should extract P1001
        extracted_values = [k.value for k in result.foreign_key_references]
        self.assertIn(
            "P1001", extracted_values, "Should extract P1001 after 'Equipment tag:'"
        )

    def test_positional_detection_in_parentheses(self):
        """Test heuristic extraction from text in parentheses."""
        engine = KeyExtractionEngine(self.config)

        # Test asset with tag in parentheses
        result = engine.extract_keys(self.test_assets[1], "asset")

        # Should extract FCV-2001 from (FCV-2001)
        extracted_values = [k.value for k in result.foreign_key_references]
        self.assertIn(
            "FCV-2001", extracted_values, "Should extract FCV-2001 from parentheses"
        )

    def test_positional_detection_keyword_variants(self):
        """Test heuristic extraction with various keyword patterns."""
        engine = KeyExtractionEngine(self.config)

        # Test asset with "Measurement point:" keyword
        result = engine.extract_keys(self.test_assets[3], "asset")

        # Should extract LIC-301 after "Measurement point:"
        extracted_values = [k.value for k in result.foreign_key_references]
        self.assertIn(
            "LIC-301",
            extracted_values,
            "Should extract LIC-301 after 'Measurement point:'",
        )

    def test_frequency_analysis_strategy(self):
        """Test frequency analysis heuristic strategy."""
        engine = KeyExtractionEngine(self.config)

        # Test asset with multiple tags
        result = engine.extract_keys(self.test_assets[4], "asset")

        # Should extract multiple tags using frequency analysis
        self.assertGreater(
            len(result.foreign_key_references),
            0,
            "Should extract at least one key using frequency analysis",
        )

        # Verify specific extractions
        extracted_values = [k.value for k in result.foreign_key_references]
        # Should identify common patterns like FIC-1001, PIC-2020
        self.assertTrue(
            any("FIC" in v or "PIC" in v for v in extracted_values),
            "Should extract instrument tags",
        )

    def test_context_inference_strategy(self):
        """Test context inference heuristic strategy."""
        engine = KeyExtractionEngine(self.config)

        # Test with context-rich description
        result = engine.extract_keys(self.test_assets[2], "asset")

        # Context inference should extract tags near equipment-related keywords
        extracted_values = [k.value for k in result.foreign_key_references]

        # Should extract pump and vessel tags
        self.assertTrue(
            any("P-5001" in v or "V-4001" in v for v in extracted_values),
            "Should extract equipment tags based on context",
        )

    def test_multiple_strategy_combination(self):
        """Test that multiple heuristic strategies work together."""
        engine = KeyExtractionEngine(self.config)

        # Process all test assets
        results = []
        for asset in self.test_assets:
            result = engine.extract_keys(asset, "asset")
            results.append((asset, result))

        # Verify all assets extract at least one key
        for asset, result in results:
            self.assertGreater(
                len(result.foreign_key_references),
                0,
                f"Asset {asset.get('id', asset.get('externalId', 'unknown'))} should extract at least one key",
            )

        # Verify confidence scores are within expected range
        for asset, result in results:
            for key in result.foreign_key_references:
                self.assertGreaterEqual(
                    key.confidence,
                    0.4,
                    f"Key {key.value} should meet confidence threshold",
                )
                self.assertLessEqual(
                    key.confidence,
                    1.0,
                    f"Key {key.value} should not exceed max confidence",
                )

    def test_confidence_modifiers(self):
        """Test that confidence modifiers are applied correctly."""
        engine = KeyExtractionEngine(self.config)

        result = engine.extract_keys(self.test_assets[0], "asset")

        # Check that extracted keys have metadata about scoring
        for key in result.foreign_key_references:
            self.assertIn(
                "base_score", key.metadata, "Should include base_score in metadata"
            )
            self.assertIn(
                "adjusted_score",
                key.metadata,
                "Should include adjusted_score in metadata",
            )
            self.assertIn(
                "strategies_applied",
                key.metadata,
                "Should include strategies_applied in metadata",
            )

            # Verify adjusted score matches confidence
            self.assertAlmostEqual(
                key.confidence,
                key.metadata["adjusted_score"],
                places=2,
                msg="Adjusted score should match confidence",
            )


if __name__ == "__main__":
    unittest.main()
