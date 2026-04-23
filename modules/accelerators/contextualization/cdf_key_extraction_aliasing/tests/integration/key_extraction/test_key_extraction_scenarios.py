#!/usr/bin/env python3
"""
Tests for Key Extraction Scenarios

This module provides tests for specific key extraction scenarios including:
- ISO standard instrument tag extraction
- Fixed width parsing on timeseries metadata
- Regex instrument tags from timeseries externalId

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

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
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
            "associations": [
                {
                    "kind": "source_view_to_extraction",
                    "source_view_index": 0,
                    "extraction_rule_name": "isa_instrument_tag_extraction",
                },
                {
                    "kind": "source_view_to_extraction",
                    "source_view_index": 0,
                    "extraction_rule_name": "pump_tag_extraction",
                },
            ],
            "extraction_rules": [
                {
                    "rule_id": "isa_instrument_tag_extraction",
                    "name": "isa_instrument_tag_extraction",
                    "description": "Extracts ISA standard instrument tags (FIC, PIC, TIC, LIC, etc.)",
                    "extraction_type": "candidate_key",
                    "handler": "regex_handler",
                    "field_results_mode": "merge_all",
                    "priority": 50,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "name",
                            "required": True,
                            "priority": 1,
                            "regex": r"\b[A-Z]{1,2}[-]?[A-Z]{2,3}[-_]?\d{4}[A-Z]?\b",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.8},
                },
                {
                    "rule_id": "pump_tag_extraction",
                    "name": "pump_tag_extraction",
                    "description": "Extracts pump tags (P-XXX)",
                    "extraction_type": "foreign_key_reference",
                    "handler": "regex_handler",
                    "field_results_mode": "merge_all",
                    "priority": 40,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "description",
                            "required": False,
                            "priority": 2,
                            "regex": r"\bP[-_]?\d{3}[A-Z]?\b",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.7},
                },
            ],
            "validation": {"min_confidence": 0.5, "max_keys_per_type": 20},
        }

    def test_regex_extraction(self):
        """Test extraction on ISO standard assets."""
        engine = KeyExtractionEngine(self.config)

        results = []
        for asset in self.sample_assets:
            result = engine.extract_keys(asset, "asset", source_view_index=0)
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
            result = engine.extract_keys(asset, "asset", source_view_index=0)
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
        result = engine.extract_keys(unit_asset, "asset", source_view_index=0)

        extracted_values = [k.value for k in result.candidate_keys]

        # Should extract the unit-prefixed tag
        self.assertIn("A-FIC-1001", extracted_values)

        # Test another unit-prefixed asset
        unit_asset2 = self.sample_assets[5]  # B-PIC-2001
        result2 = engine.extract_keys(unit_asset2, "asset", source_view_index=0)
        extracted_values2 = [k.value for k in result2.candidate_keys]
        self.assertIn("B-PIC-2001", extracted_values2)


class TestRegexInstrumentTagFromExternalId(unittest.TestCase):
    """Regex extraction of instrument-style tags from timeseries externalId."""

    def setUp(self):
        """Set up test fixtures with timeseries for externalId regex extraction."""
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
            "associations": [
                {
                    "kind": "source_view_to_extraction",
                    "source_view_index": 0,
                    "extraction_rule_name": "instrument_tag_from_external_id",
                }
            ],
            "extraction_rules": [
                {
                    "rule_id": "instrument_tag_from_external_id",
                    "name": "instrument_tag_from_external_id",
                    "description": "Extract instrument-style tags (e.g. FIC-1001) from externalId",
                    "extraction_type": "candidate_key",
                    "handler": "regex_handler",
                    "field_results_mode": "merge_all",
                    "priority": 40,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "externalId",
                            "required": True,
                            "priority": 1,
                            "regex": r"[A-Z]{2,4}-\d{4}",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.80},
                }
            ],
            "validation": {"min_confidence": 0.5, "max_keys_per_type": 10},
        }

    def test_regex_instrument_tag_from_external_id(self):
        """Test regex on externalId extracts instrument tags (e.g. FIC-1001)."""
        engine = KeyExtractionEngine(self.config)

        results = []
        for ts in self.timeseries_records:
            result = engine.extract_keys(ts, "timeseries", source_view_index=0)
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

    def test_regex_instrument_tag_format(self):
        """Test that regex extracts tags matching [A-Z]{2,4}-dddd."""
        engine = KeyExtractionEngine(self.config)

        result = engine.extract_keys(
            self.timeseries_records[0], "timeseries", source_view_index=0
        )

        # Check that extracted tag matches pattern [A-Z]{2,4}-\d{4}
        for key in result.candidate_keys:
            self.assertTrue(
                re.match(r"^[A-Z]{2,4}-\d{4}$", key.value),
                f"Extracted tag {key.value} should match pattern [A-Z]{{2,4}}-\\d{{4}}",
            )



if __name__ == "__main__":
    unittest.main()
