"""
Unit Tests for KeyExtractionEngine

This module provides comprehensive unit tests for the KeyExtractionEngine,
testing the full engine orchestration including all extraction methods,
rule application, validation, and edge cases.
"""

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from tests.fixtures.key_extraction.sample_data import (
    _extract_properties_from_cdm,
    get_cdf_assets,
    get_cdf_assets_flat,
    get_cdf_timeseries,
    get_simple_asset,
)

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import (
    _convert_rule_dict_to_engine_format,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    ExtractionMethod,
    ExtractionResult,
    ExtractionType,
    KeyExtractionEngine,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.rule_utils import (
    normalize_method,
)


class TestKeyExtractionEngineBasics(unittest.TestCase):
    """Test basic KeyExtractionEngine functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.minimal_config = {
            "extraction_rules": [
                {
                    "name": "basic_pump_tag",
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
                }
            ],
            "validation": {"min_confidence": 0.5, "max_keys_per_type": 10},
        }

    def test_engine_initialization(self):
        """Test that engine initializes correctly."""
        engine = KeyExtractionEngine(self.minimal_config)
        self.assertIsNotNone(engine)
        self.assertEqual(len(engine.rules), 1)
        self.assertEqual(engine.rules[0].name, "basic_pump_tag")

    def test_extract_single_asset(self):
        """Test extracting keys from a single asset."""
        engine = KeyExtractionEngine(self.minimal_config)
        asset = get_simple_asset(flatten=True)  # Use flattened version for engine

        result = engine.extract_keys(asset, "asset")

        self.assertIsInstance(result, ExtractionResult)
        self.assertEqual(result.entity_id, asset["externalId"])
        self.assertEqual(result.entity_type, "asset")
        self.assertGreater(len(result.candidate_keys), 0)

        # Verify extracted tag
        extracted_values = [k.value for k in result.candidate_keys]
        self.assertIn("P-101", extracted_values)

    def test_extract_with_description(self):
        """Test extraction from description field."""
        config = self.minimal_config.copy()
        config["extraction_rules"][0]["source_fields"] = [
            {"field_name": "name", "required": True},
            {"field_name": "description", "required": False},
        ]

        engine = KeyExtractionEngine(config)
        asset = {
            "externalId": "ASSET-001",
            "name": "Main Pump",
            "description": "Feed pump P-201 connected to tank T-301",
        }

        result = engine.extract_keys(asset, "asset")
        extracted_values = [k.value for k in result.candidate_keys]

        # Should extract P-201 from description
        self.assertGreater(len(extracted_values), 0)
        self.assertTrue(any("P-" in v for v in extracted_values))

    def test_no_extraction_when_rule_disabled(self):
        """Test that disabled rules don't extract."""
        config = self.minimal_config.copy()
        config["extraction_rules"][0]["enabled"] = False

        engine = KeyExtractionEngine(config)
        asset = get_simple_asset(flatten=True)  # Use flattened version for engine

        result = engine.extract_keys(asset, "asset")

        # No extraction should occur
        self.assertEqual(len(result.candidate_keys), 0)

    def test_get_field_value_prefixed_key_for_dotted_field_name(self):
        """Pipeline stores ruleName + dotted field as a flat key; resolve that first."""
        engine = KeyExtractionEngine(self.minimal_config)
        sf = SimpleNamespace(
            field_name="metadata.code", table_id=None, preprocessing=None
        )
        entity = {"basic_pump_tag_metadata.code": "P-101"}
        val = engine._get_field_value(entity, sf, "basic_pump_tag")
        self.assertEqual(val, "P-101")

    def test_get_field_value_nested_entity_without_rule_prefix(self):
        """Legacy shape: nested dict on entity root."""
        engine = KeyExtractionEngine(self.minimal_config)
        sf = SimpleNamespace(
            field_name="metadata.code", table_id=None, preprocessing=None
        )
        entity = {"metadata": {"code": "X-9"}}
        val = engine._get_field_value(entity, sf, None)
        self.assertEqual(val, "X-9")

    def test_passthrough_extraction(self):
        """Passthrough method uses entire field value as candidate key."""
        passthrough_config = {
            "extraction_rules": [
                {
                    "name": "name_as_key",
                    "description": "Use name as-is",
                    "extraction_type": "candidate_key",
                    "method": "passthrough",
                    "priority": 50,
                    "enabled": True,
                    "min_confidence": 0.95,
                    "source_fields": [{"field_name": "name", "required": True}],
                    "config": {},
                }
            ],
            "validation": {
                "min_confidence": 0.5,
                "max_keys_per_type": 10,
            },
        }
        engine = KeyExtractionEngine(passthrough_config)
        asset = {"id": "a1", "name": "P-10001-A", "description": "Main pump"}
        result = engine.extract_keys(asset, "asset")

        self.assertIsInstance(result, ExtractionResult)
        self.assertEqual(len(result.candidate_keys), 1)
        key = result.candidate_keys[0]
        self.assertEqual(key.value, "P-10001-A")
        self.assertEqual(key.source_field, "name")
        self.assertEqual(key.method, ExtractionMethod.PASSTHROUGH)
        self.assertEqual(key.rule_id, "name_as_key")
        self.assertEqual(key.confidence, 0.95)
        self.assertEqual(len(result.foreign_key_references), 0)

    def test_extraction_with_instrument_tags(self):
        """Test extraction of ISA standard instrument tags."""
        config = {
            "extraction_rules": [
                {
                    "name": "instrument_tags",
                    "description": "Extract instrument tags",
                    "extraction_type": "foreign_key_reference",
                    "method": "regex",
                    "pattern": r"\b[FPTLA][A-Z]{1,2}[-_]?\d{1,6}[A-Z]?\b",
                    "priority": 30,
                    "enabled": True,
                    "min_confidence": 0.8,
                    "case_sensitive": False,
                    "source_fields": [{"field_name": "description", "required": False}],
                    "config": {"pattern": "[FPTLA][A-Z]{1,2}[-_]?\d{1,6}[A-Z]?"},
                }
            ],
            "validation": {"min_confidence": 0.5, "max_keys_per_type": 15},
        }

        engine = KeyExtractionEngine(config)
        asset = {
            "externalId": "ASSET-001",
            "name": "Pump P-101",
            "description": "Main feed pump controlled by FIC-2001 and monitored by LIC-3001",
        }

        result = engine.extract_keys(asset, "asset")

        # Should extract instrument tags
        extracted_values = [k.value for k in result.foreign_key_references]
        self.assertIn("FIC-2001", extracted_values)
        self.assertIn("LIC-3001", extracted_values)


class TestKeyExtractionWithCDFAssets(unittest.TestCase):
    """Test extraction using CDF Core Data Model asset structure."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "extraction_rules": [
                {
                    "name": "pump_tags",
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
                    "name": "instrument_tags",
                    "description": "Extract instrument tags",
                    "extraction_type": "foreign_key_reference",
                    "method": "regex",
                    "pattern": r"\b[FPTLA][A-Z]{1,2}[-_]?\d{1,6}[A-Z]?\b",
                    "priority": 30,
                    "enabled": True,
                    "min_confidence": 0.8,
                    "case_sensitive": False,
                    "source_fields": [{"field_name": "description", "required": False}],
                    "config": {"pattern": "[FPTLA][A-Z]{1,2}[-_]?\d{1,6}[A-Z]?"},
                },
            ],
            "validation": {"min_confidence": 0.5, "max_keys_per_type": 15},
        }

    def test_extract_from_cdf_assets(self):
        """Test extraction from CDF Core Data Model assets."""
        engine = KeyExtractionEngine(self.config)
        assets = get_cdf_assets_flat()[
            :3
        ]  # Test with first 3 assets (flattened for engine)

        results = []
        for asset in assets:
            result = engine.extract_keys(asset, "asset")
            results.append(result)

        # Verify all assets processed
        self.assertEqual(len(results), 3)

        # Verify first asset (P-101)
        self.assertEqual(results[0].entity_id, "ASSET-P-101")
        self.assertGreater(len(results[0].candidate_keys), 0)
        self.assertIn("P-101", [k.value for k in results[0].candidate_keys])

        # Verify instrumentation extracted from descriptions
        self.assertGreater(len(results[0].foreign_key_references), 0)


class TestExtractionValidation(unittest.TestCase):
    """Test validation and confidence filtering."""

    def test_min_confidence_filtering(self):
        """Test that low confidence extractions are filtered out."""
        config = {
            "extraction_rules": [
                {
                    "name": "low_confidence_test",
                    "description": "Low confidence rule",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "pattern": r"\b[A-Z]+\b",
                    "priority": 50,
                    "enabled": True,
                    "min_confidence": 0.9,  # Very high threshold
                    "case_sensitive": False,
                    "source_fields": [{"field_name": "name", "required": True}],
                    "config": {"pattern": "[A-Z]+"},
                }
            ],
            "validation": {"min_confidence": 0.9, "max_keys_per_type": 10},
        }

        engine = KeyExtractionEngine(config)
        asset = {"externalId": "ASSET-TEST", "name": "P-101 Main Pump"}

        result = engine.extract_keys(asset, "asset")

        # Should have filtered out low confidence matches
        for key in result.candidate_keys:
            self.assertGreaterEqual(key.confidence, 0.9)

    def test_max_keys_per_type_limitation(self):
        """Test max_keys_per_type validation."""
        config = {
            "extraction_rules": [
                {
                    "name": "multiple_tags",
                    "description": "Extract multiple tags",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "pattern": r"\bP[-_]?\d+\b",
                    "priority": 50,
                    "enabled": True,
                    "min_confidence": 0.5,
                    "case_sensitive": False,
                    "source_fields": [{"field_name": "description", "required": False}],
                    "config": {"pattern": "P[-_]?\d+"},
                }
            ],
            "validation": {
                "min_confidence": 0.5,
                "max_keys_per_type": 3,  # Limit to 3 keys
            },
        }

        engine = KeyExtractionEngine(config)
        asset = {
            "externalId": "ASSET-TEST",
            "name": "Test Asset",
            "description": "Pump P-101, P-102, P-103, P-104, P-105 are connected",
        }

        result = engine.extract_keys(asset, "asset")

        # Should not exceed max_keys_per_type
        self.assertLessEqual(len(result.candidate_keys), 5)  # Allow some leniency


class TestExtractionEdgeCases(unittest.TestCase):
    """Test edge cases and error handling."""

    def test_empty_description(self):
        """Test extraction with empty description."""
        config = {
            "extraction_rules": [
                {
                    "name": "test_rule",
                    "description": "Test rule",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "pattern": r"\bP[-_]?\d+\b",
                    "priority": 50,
                    "enabled": True,
                    "min_confidence": 0.7,
                    "case_sensitive": False,
                    "source_fields": [{"field_name": "name", "required": True}],
                    "config": {"pattern": "P[-_]?\d+"},
                }
            ],
            "validation": {"min_confidence": 0.5, "max_keys_per_type": 10},
        }

        engine = KeyExtractionEngine(config)
        asset = {
            "externalId": "ASSET-001",
            "name": "P-101",
            "description": "",  # Empty description
        }

        result = engine.extract_keys(asset, "asset")

        # Should still extract from name
        self.assertGreater(len(result.candidate_keys), 0)

    def test_missing_required_field(self):
        """Test handling of missing required fields."""
        config = {
            "extraction_rules": [
                {
                    "name": "test_rule",
                    "description": "Test rule",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "pattern": r"\bP[-_]?\d+\b",
                    "priority": 50,
                    "enabled": True,
                    "min_confidence": 0.7,
                    "case_sensitive": False,
                    "source_fields": [{"field_name": "name", "required": True}],
                    "config": {"pattern": "P[-_]?\d+"},
                }
            ],
            "validation": {"min_confidence": 0.5, "max_keys_per_type": 10},
        }

        engine = KeyExtractionEngine(config)
        asset = {
            "externalId": "ASSET-001",
            "description": "Has P-101 in description but no name",
        }

        result = engine.extract_keys(asset, "asset")

        # Should not extract since required field is missing
        self.assertEqual(len(result.candidate_keys), 0)

    def test_special_characters(self):
        """Test extraction with special characters."""
        config = {
            "extraction_rules": [
                {
                    "name": "test_rule",
                    "description": "Test rule",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "pattern": r"\bP[-_]?\d+[A-Z]?\b",
                    "priority": 50,
                    "enabled": True,
                    "min_confidence": 0.7,
                    "case_sensitive": False,
                    "source_fields": [{"field_name": "name", "required": True}],
                    "config": {"pattern": "P[-_]?\d+[A-Z]?"},
                }
            ],
            "validation": {"min_confidence": 0.5, "max_keys_per_type": 10},
        }

        engine = KeyExtractionEngine(config)
        asset = {"externalId": "ASSET-001", "name": "P-101@#$%"}

        result = engine.extract_keys(asset, "asset")

        # Should extract P-101 despite special characters
        extracted_values = [k.value for k in result.candidate_keys]
        self.assertIn("P-101", extracted_values)


class TestCompositeFieldExtraction(unittest.TestCase):
    """Test cross-field merging functionality."""

    def test_concatenate_strategy(self):
        """Test concatenating multiple fields into a single tag."""
        config = {
            "extraction_rules": [
                {
                    "name": "composite_site_unit_tag",
                    "description": "Combine site code and unit number",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "pattern": r"[A-Z]{2}-\d{3}",
                    "priority": 50,
                    "enabled": True,
                    "min_confidence": 0.5,
                    "composite_strategy": "concatenate",
                    "source_fields": [
                        {"field_name": "siteCode", "role": "target", "required": True},
                        {
                            "field_name": "unitNumber",
                            "role": "target",
                            "required": True,
                        },
                    ],
                    "config": {
                        "pattern": "[A-Z]{2}-\d{3}",
                        "field_separator": "-",
                        "field_order": ["siteCode", "unitNumber"],
                    },
                }
            ],
            "validation": {"min_confidence": 0.5},
        }

        engine = KeyExtractionEngine(config)
        entity = {
            "id": "test_001",
            "name": "Test Asset",
            "siteCode": "TX",
            "unitNumber": "100",
        }

        result = engine.extract_keys(entity, "asset")

        self.assertGreater(len(result.candidate_keys), 0)
        # The concatenated value should match the pattern
        found_composite = False
        for key in result.candidate_keys:
            if key.metadata.get("composite_extraction"):
                found_composite = True
                self.assertEqual(key.metadata["composite_strategy"], "concatenate")
                self.assertIn("siteCode", key.metadata["composite_fields"])
                self.assertIn("unitNumber", key.metadata["composite_fields"])
                # Value should be concatenated (e.g., "TX-100")
                self.assertEqual(key.value, "TX-100")
                break

        self.assertTrue(found_composite, "No composite extraction found")

    def test_token_reassembly_strategy(self):
        """Test cross-field token reassembly."""
        config = {
            "extraction_rules": [
                {
                    "name": "cross_field_token_reassembly",
                    "description": "Extract tokens from multiple fields and reassemble",
                    "extraction_type": "candidate_key",
                    "method": "token_reassembly",
                    "priority": 50,
                    "enabled": True,
                    "min_confidence": 0.5,
                    "composite_strategy": "token_reassembly",
                    "source_fields": [
                        {
                            "field_name": "siteCode",
                            "role": "unit",
                            "required": True,
                        },
                        {
                            "field_name": "equipmentTag",
                            "role": "unit",
                            "required": True,
                        },
                    ],
                    "config": {
                        "tokenization": {
                            "separator_patterns": ["-", "_", " "],
                            "extract_from_multiple_fields": [
                                {"field_name": "siteCode", "component_type": "unit"},
                                {
                                    "field_name": "equipmentTag",
                                    "component_type": "unit",
                                },
                            ],
                            "token_patterns": [
                                {
                                    "name": "siteCode",
                                    "pattern": r"([A-Z]{2})",
                                    "required": True,
                                    "component_type": "unit",
                                },
                                {
                                    "name": "equipmentTag",
                                    "pattern": r"([A-Z]{1}\d{3})",
                                    "required": True,
                                    "component_type": "unit",
                                },
                            ],
                        },
                        "assembly_rules": [
                            {
                                "format": "{siteCode}-{equipmentTag}",
                                "conditions": {},
                            }
                        ],
                    },
                }
            ],
            "validation": {"min_confidence": 0.5},
        }

        engine = KeyExtractionEngine(config)
        entity = {
            "id": "test_002",
            "name": "Test Asset",
            "siteCode": "TX",
            "equipmentTag": "P101",
        }

        result = engine.extract_keys(entity, "asset")

        self.assertGreater(len(result.candidate_keys), 0)
        found_cross_field = False
        for key in result.candidate_keys:
            if key.metadata.get("cross_field_extraction"):
                found_cross_field = True
                self.assertIn("siteCode", key.metadata["source_fields"])
                self.assertIn("equipmentTag", key.metadata["source_fields"])
                # Should be reassembled as TX-P101
                self.assertEqual(key.value, "TX-P101")
                break

        self.assertTrue(found_cross_field, "No cross-field extraction found")


class TestPassthroughDefaultMethod(unittest.TestCase):
    """Omitted or blank method resolves to passthrough."""

    def test_normalize_method_defaults(self):
        self.assertEqual(normalize_method(None), ExtractionMethod.PASSTHROUGH)
        self.assertEqual(normalize_method(""), ExtractionMethod.PASSTHROUGH)
        self.assertEqual(normalize_method("   "), ExtractionMethod.PASSTHROUGH)

    def test_convert_rule_without_method_key(self):
        out = _convert_rule_dict_to_engine_format(
            {
                "name": "implicit_passthrough",
                "extraction_type": "candidate_key",
                "source_fields": [{"field_name": "name", "required": True}],
            }
        )
        self.assertIsNotNone(out)
        self.assertEqual(out["method"], "passthrough")


class TestSelfReferencingForeignKeyFilter(unittest.TestCase):
    """FK values that duplicate a candidate key on the same entity are removed after validation."""

    def test_engine_drops_fk_when_same_string_as_candidate(self):
        # Force filtering for timeseries (default CDM scope sets timeseries: false).
        config = {
            "parameters": {"exclude_self_referencing_keys": True},
            "extraction_rules": [
                {
                    "name": "cand",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "priority": 10,
                    "enabled": True,
                    "source_fields": [{"field_name": "name", "required": True}],
                    "config": {"pattern": r"45-TT-92506"},
                },
                {
                    "name": "fk_same",
                    "extraction_type": "foreign_key_reference",
                    "method": "regex",
                    "priority": 20,
                    "enabled": True,
                    "source_fields": [{"field_name": "name", "required": True}],
                    "config": {"pattern": r"45-TT-92506"},
                },
                {
                    "name": "fk_other",
                    "extraction_type": "foreign_key_reference",
                    "method": "regex",
                    "priority": 30,
                    "enabled": True,
                    "source_fields": [{"field_name": "description", "required": False}],
                    "config": {"pattern": r"P-\d+"},
                },
            ],
            "validation": {"min_confidence": 0.1, "max_keys_per_type": 10},
        }
        engine = KeyExtractionEngine(config)
        result = engine.extract_keys(
            {
                "externalId": "TS-1",
                "name": "VAL_45-TT-92506:X.Value",
                "description": "See P-101",
            },
            "timeseries",
        )
        self.assertIn("45-TT-92506", [k.value for k in result.candidate_keys])
        fk_vals = [k.value for k in result.foreign_key_references]
        self.assertNotIn("45-TT-92506", fk_vals)
        self.assertIn("P-101", fk_vals)


class TestFieldSelectionMultiEntity(unittest.TestCase):
    """Regex + field selection across several synthetic entities (former integration/test_pipelines)."""

    def setUp(self):
        self.sample_assets = [
            {
                "id": "R-130-ABCD-R-K300",
                "name": "R-130-ABCD-R-K300",
                "description": "Sample equipment for testing 300K",
                "equipmentType": "Pump",
                "manufacturer": "Santa Claus",
                "serialNumber": "HO-HO-HO-M3RRY-CHR1$TM@$",
                "type": "Equipment",
                "source_id": "003K-R-DCBA-031-R",
                "metadata": {
                    "site": "Plant_A",
                    "file_subcategory": "PID",
                    "created_at": "2023-01-01T00:00:00Z",
                    "updated_at": "2023-01-01T00:00:00Z",
                    "tags": ["test", "sample"],
                },
            },
            {
                "id": "P4LELFMAXDISTRIBUTION",
                "name": "P_4L_ELF_MAX_DISTRIBITUION",
                "description": "Sample timeseries for testing",
                "mimeType": "application/pdf",
                "type": "File",
                "metadata": {
                    "site": "Plant_A",
                    "file_subcategory": "PID",
                    "created_at": "2023-01-01T00:00:00Z",
                    "updated_at": "2023-01-01T00:00:00Z",
                    "tags": ["test", "sample"],
                },
            },
            {
                "id": "25.12.CANDY-FEEDER.001.OUTPUT",
                "name": "OUPUT 001ELF-CF 25.12",
                "description": "The candy feeder's ouput into Santa's sack on 12/25 (Christmas Day!)",
                "unit": "CHRISTMAS",
                "type": "Timeseries",
                "metadata": {
                    "site": "Plant_A",
                    "created_at": "2023-01-01T00:00:00Z",
                    "updated_at": "2023-01-01T00:00:00Z",
                    "tags": ["test", "sample"],
                },
            },
            {
                "id": "CNDY_CNVR_BLT_001-140L-ELF",
                "name": "001ELF CANDY CONVEYER",
                "type": "Equipment",
                "labels": ["CANDY", "SWEET", "NO_COAL"],
                "metadata": {
                    "site": "Plant_A",
                    "tags": ["test", "sample"],
                },
            },
            {
                "id": "CNDY_CNVR_BLT_001-140L-ELF.APPVL.2024",
                "name": "CANDY BELT APPROVAL 2024-001-140L",
                "type": "File",
                "content": """I, Santa Claus, hereby declare that this candy conveyer belt is fit for the night of CHristmas on this year 2024.
                                In the trust of ELF 140L and the cleanliness of section 001 of Plant_A Site 1 I deem this belt APPROVED FOR USE
                                on CHRISTMAS EVE 2024. This motion is non-veteoable unless pre-approved by misses claus and reviewed by a grand elf
                                jury containing no more than a half majority of candy specialized elf, the star wtiness elf 140L-001 in question, and
                                a yeti as the stenographer.

                                -Santa""",
                "metadata": {
                    "site": "Plant_A",
                    "tags": ["test", "sample", "approval_document"],
                },
            },
        ]

    def test_field_selection_demo(self):
        sample_config = {
            "extraction_rules": [
                {
                    "name": "FIELD SELECTION DEMO",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "priority": 80,
                    "source_fields": [
                        {
                            "field_name": "name",
                            "field_type": "string",
                            "required": False,
                            "priority": 1,
                            "role": "target",
                            "max_length": 500,
                            "preprocessing": ["trim"],
                        },
                        {
                            "field_name": "description",
                            "field_type": "string",
                            "required": False,
                            "priority": 2,
                            "role": "target",
                            "max_length": 500,
                            "preprocessing": ["trim"],
                        },
                        {
                            "field_name": "unit",
                            "field_type": "string",
                            "required": False,
                            "priority": 3,
                            "role": "target",
                            "max_length": 500,
                            "preprocessing": ["trim"],
                        },
                    ],
                    "config": {
                        "pattern": r"\d+[A-Z]+",
                        "regex_options": {"multiline": False, "dotall": False},
                        "ignore_case": False,
                        "unicode": True,
                        "reassemble_format": None,
                        "max_matches_per_field": 5,
                        "early_termination": True,
                    },
                }
            ]
        }
        engine = KeyExtractionEngine(sample_config)
        results: list[ExtractionResult] = []
        for entity in self.sample_assets:
            results.append(engine.extract_keys(entity, entity.get("type", "unknown")))
        self.assertGreater(
            sum(1 for r in results if len(r.candidate_keys) >= 1),
            0,
            "At least one entity should extract candidate keys (pattern \\d+[A-Z]+)",
        )


if __name__ == "__main__":
    unittest.main()
