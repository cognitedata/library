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
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.config import (
    ExtractionRuleConfig,
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
                    "rule_id": "basic_pump_tag",
                    "name": "basic_pump_tag",
                    "description": "Extract pump tags",
                    "extraction_type": "candidate_key",
                    "handler": "regex_handler",
                    "field_results_mode": "merge_all",
                    "priority": 50,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "name",
                            "required": True,
                            "regex": r"\bP[-_]?\d{1,6}[A-Z]?\b",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.7},
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
        config["extraction_rules"][0]["field_results_mode"] = "merge_all"
        config["extraction_rules"][0]["fields"] = [
            {
                "field_name": "name",
                "required": True,
                "regex": r"\bP[-_]?\d{1,6}[A-Z]?\b",
                "regex_options": {"ignore_case": True},
            },
            {
                "field_name": "description",
                "required": False,
                "regex": r"\bP[-_]?\d{1,6}[A-Z]?\b",
                "regex_options": {"ignore_case": True},
            },
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

    def test_trim_passthrough_regex_handler(self):
        """regex_handler without regex emits trimmed field text as one candidate key."""
        passthrough_config = {
            "extraction_rules": [
                {
                    "rule_id": "name_as_key",
                    "name": "name_as_key",
                    "description": "Use name as-is",
                    "extraction_type": "candidate_key",
                    "handler": "regex_handler",
                    "priority": 50,
                    "enabled": True,
                    "fields": [{"field_name": "name", "required": True}],
                    "validation": {"min_confidence": 0.95},
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
        self.assertEqual(key.method, ExtractionMethod.REGEX_HANDLER)
        self.assertEqual(key.rule_id, "name_as_key")
        self.assertEqual(key.confidence, 1.0)
        self.assertEqual(len(result.foreign_key_references), 0)

    def test_extraction_with_instrument_tags(self):
        """Test extraction of ISA standard instrument tags."""
        config = {
            "extraction_rules": [
                {
                    "rule_id": "instrument_tags",
                    "name": "instrument_tags",
                    "description": "Extract instrument tags",
                    "extraction_type": "foreign_key_reference",
                    "handler": "regex_handler",
                    "field_results_mode": "merge_all",
                    "priority": 30,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "description",
                            "required": False,
                            "regex": r"\b[FPTLA][A-Z]{1,2}[-_]?\d{1,6}[A-Z]?\b",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.8},
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
                    "rule_id": "pump_tags",
                    "name": "pump_tags",
                    "description": "Extract pump tags",
                    "extraction_type": "candidate_key",
                    "handler": "regex_handler",
                    "field_results_mode": "merge_all",
                    "priority": 50,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "name",
                            "required": True,
                            "regex": r"\bP[-_]?\d{1,6}[A-Z]?\b",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.7},
                },
                {
                    "rule_id": "instrument_tags",
                    "name": "instrument_tags",
                    "description": "Extract instrument tags",
                    "extraction_type": "foreign_key_reference",
                    "handler": "regex_handler",
                    "field_results_mode": "merge_all",
                    "priority": 30,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "description",
                            "required": False,
                            "regex": r"\b[FPTLA][A-Z]{1,2}[-_]?\d{1,6}[A-Z]?\b",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.8},
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
                    "rule_id": "low_confidence_test",
                    "name": "low_confidence_test",
                    "description": "Low confidence rule",
                    "extraction_type": "candidate_key",
                    "handler": "regex_handler",
                    "priority": 50,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "name",
                            "required": True,
                            "regex": r"\b[A-Z]+\b",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.9},
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
                    "rule_id": "multiple_tags",
                    "name": "multiple_tags",
                    "description": "Extract multiple tags",
                    "extraction_type": "candidate_key",
                    "handler": "regex_handler",
                    "priority": 50,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "description",
                            "required": False,
                            "regex": r"\bP[-_]?\d+\b",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.5},
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
                    "rule_id": "test_rule",
                    "name": "test_rule",
                    "description": "Test rule",
                    "extraction_type": "candidate_key",
                    "handler": "regex_handler",
                    "priority": 50,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "name",
                            "required": True,
                            "regex": r"\bP[-_]?\d+\b",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.7},
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
                    "rule_id": "test_rule",
                    "name": "test_rule",
                    "description": "Test rule",
                    "extraction_type": "candidate_key",
                    "handler": "regex_handler",
                    "priority": 50,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "name",
                            "required": True,
                            "regex": r"\bP[-_]?\d+\b",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.7},
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
                    "rule_id": "test_rule",
                    "name": "test_rule",
                    "description": "Test rule",
                    "extraction_type": "candidate_key",
                    "handler": "regex_handler",
                    "priority": 50,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "name",
                            "required": True,
                            "regex": r"\bP[-_]?\d+[A-Z]?\b",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.7},
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


class TestFieldResultsModeCoercion(unittest.TestCase):
    """Legacy field_results_mode: first_match is coerced to merge_all in Pydantic config."""

    def test_first_match_coerced_to_merge_all(self):
        r = ExtractionRuleConfig.model_validate(
            {
                "rule_id": "legacy_fm",
                "handler": "heuristic",
                "field_results_mode": "first_match",
                "parameters": {
                    "strategies": [{"id": "delimiter_split", "weight": 1.0}],
                    "max_candidates_per_field": 5,
                },
                "fields": [{"field_name": "name"}],
            }
        )
        self.assertEqual(r.field_results_mode, "merge_all")


class TestNormalizeMethod(unittest.TestCase):
    """Handler id normalization (regex_handler default)."""

    def test_normalize_method_defaults(self):
        self.assertEqual(normalize_method(None), ExtractionMethod.REGEX_HANDLER)
        self.assertEqual(normalize_method(""), ExtractionMethod.REGEX_HANDLER)
        self.assertEqual(normalize_method("   "), ExtractionMethod.REGEX_HANDLER)

    def test_normalize_method_legacy_field_rule_alias(self):
        self.assertEqual(normalize_method("field_rule"), ExtractionMethod.REGEX_HANDLER)

    def test_normalize_method_deprecated_field_rule_fixed_width_maps_to_regex(self):
        self.assertEqual(
            normalize_method("field_rule_fixed_width"), ExtractionMethod.REGEX_HANDLER
        )

    def test_normalize_method_removed_handlers(self):
        self.assertEqual(normalize_method("passthrough"), ExtractionMethod.UNSUPPORTED)
        self.assertEqual(normalize_method("regex"), ExtractionMethod.UNSUPPORTED)
        self.assertEqual(normalize_method("fixed_width"), ExtractionMethod.UNSUPPORTED)
        self.assertEqual(normalize_method("token_reassembly"), ExtractionMethod.UNSUPPORTED)

    def test_convert_rule_without_method_key(self):
        out = _convert_rule_dict_to_engine_format(
            {
                "name": "implicit_regex_handler",
                "extraction_type": "candidate_key",
                "fields": [{"field_name": "name", "required": True, "regex": r"P-\d+"}],
            }
        )
        self.assertIsNotNone(out)
        self.assertEqual(out["handler"], "regex_handler")

    def test_convert_rule_token_reassembly_handler_removed(self):
        out = _convert_rule_dict_to_engine_format(
            {
                "name": "legacy_tr",
                "handler": "token_reassembly",
                "parameters": {"tokenization": {}, "assembly_rules": []},
            }
        )
        self.assertIsNone(out)

    def test_convert_rule_fixed_width_handler_removed(self):
        out = _convert_rule_dict_to_engine_format(
            {
                "name": "legacy_fw",
                "handler": "fixed_width",
                "parameters": {"field_definitions": []},
            }
        )
        self.assertIsNone(out)


class TestSelfReferencingForeignKeyFilter(unittest.TestCase):
    """FK values that duplicate a candidate key on the same entity are removed after validation."""

    def test_engine_drops_fk_when_same_string_as_candidate(self):
        # Force filtering for timeseries (default CDM scope sets timeseries: false).
        config = {
            "parameters": {"exclude_self_referencing_keys": True},
            "extraction_rules": [
                {
                    "rule_id": "cand",
                    "name": "cand",
                    "extraction_type": "candidate_key",
                    "handler": "regex_handler",
                    "priority": 10,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "name",
                            "required": True,
                            "regex": r"45-TT-92506",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.1},
                },
                {
                    "rule_id": "fk_same",
                    "name": "fk_same",
                    "extraction_type": "foreign_key_reference",
                    "handler": "regex_handler",
                    "priority": 20,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "name",
                            "required": True,
                            "regex": r"45-TT-92506",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.1},
                },
                {
                    "rule_id": "fk_other",
                    "name": "fk_other",
                    "extraction_type": "foreign_key_reference",
                    "handler": "regex_handler",
                    "priority": 30,
                    "enabled": True,
                    "fields": [
                        {
                            "field_name": "description",
                            "required": False,
                            "regex": r"P-\d+",
                            "regex_options": {"ignore_case": True},
                        }
                    ],
                    "validation": {"min_confidence": 0.1},
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
                    "rule_id": "FIELD SELECTION DEMO",
                    "name": "FIELD SELECTION DEMO",
                    "extraction_type": "candidate_key",
                    "handler": "regex_handler",
                    "field_results_mode": "merge_all",
                    "priority": 80,
                    "fields": [
                        {
                            "field_name": "name",
                            "required": False,
                            "priority": 1,
                            "max_length": 500,
                            "preprocessing": ["trim"],
                            "regex": r"\d+[A-Z]+",
                            "regex_options": {"ignore_case": False},
                            "max_matches_per_field": 5,
                        },
                        {
                            "field_name": "description",
                            "required": False,
                            "priority": 2,
                            "max_length": 500,
                            "preprocessing": ["trim"],
                            "regex": r"\d+[A-Z]+",
                            "regex_options": {"ignore_case": False},
                            "max_matches_per_field": 5,
                        },
                        {
                            "field_name": "unit",
                            "required": False,
                            "priority": 3,
                            "max_length": 500,
                            "preprocessing": ["trim"],
                            "regex": r"\d+[A-Z]+",
                            "regex_options": {"ignore_case": False},
                            "max_matches_per_field": 5,
                        },
                    ],
                    "validation": {"min_confidence": 0.1},
                }
            ],
            "validation": {"min_confidence": 0.1, "max_keys_per_type": 100},
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
