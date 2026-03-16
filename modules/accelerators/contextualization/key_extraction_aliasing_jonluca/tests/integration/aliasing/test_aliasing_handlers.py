"""
Unit Tests for Individual Aliasing Handlers

This module provides comprehensive unit tests for individual alias handler classes,
testing each handler in isolation with direct instantiation.

Handlers tested:
- CharacterSubstitutionHandler
- PrefixSuffixHandler
- RegexSubstitutionHandler
- CaseTransformationHandler
- LeadingZeroNormalizationHandler
- EquipmentTypeExpansionHandler
- RelatedInstrumentsHandler
- HierarchicalExpansionHandler
- DocumentAliasesHandler
- PatternRecognitionHandler
- PatternBasedExpansionHandler
"""

import sys
import unittest
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.contextualization.key_extraction_aliasing.functions.fn_dm_aliasing.engine.handlers import (
    CaseTransformationHandler,
    CharacterSubstitutionHandler,
    DocumentAliasesHandler,
    EquipmentTypeExpansionHandler,
    HierarchicalExpansionHandler,
    LeadingZeroNormalizationHandler,
    PrefixSuffixHandler,
    RegexSubstitutionHandler,
    RelatedInstrumentsHandler,
)


class TestCharacterSubstitutionTransformer(unittest.TestCase):
    """Test CharacterSubstitutionTransformer."""

    def test_basic_substitution(self):
        """Test basic character substitution."""
        transformer = CharacterSubstitutionHandler()
        config = {"substitutions": {"_": "-", " ": "-"}}

        aliases = {"P_10001"}
        result = transformer.transform(aliases, config)

        self.assertIn("P_10001", result)  # Original preserved
        self.assertIn("P-10001", result)

    def test_multiple_substitutions(self):
        """Test multiple character substitutions."""
        transformer = CharacterSubstitutionHandler()
        config = {"substitutions": {"-": ["_", " "]}}

        aliases = {"P-10001"}
        result = transformer.transform(aliases, config)

        self.assertIn("P-10001", result)
        self.assertIn("P_10001", result)
        self.assertIn("P 10001", result)


class TestPrefixSuffixTransformer(unittest.TestCase):
    """Test PrefixSuffixTransformer."""

    def test_add_prefix(self):
        """Test adding prefix to aliases."""
        transformer = PrefixSuffixHandler()
        config = {
            "operation": "add_prefix",
            "context_mapping": {"Plant_A": {"prefix": "PA-"}},
            "resolve_from": "site",
        }
        context = {"site": "Plant_A"}

        aliases = {"P-10001"}
        result = transformer.transform(aliases, config, context)

        self.assertIn("PA-P-10001", result)

    def test_remove_prefix(self):
        """Test removing prefix from aliases."""
        transformer = PrefixSuffixHandler()
        config = {"operation": "remove_prefix", "prefix": "PA-"}

        aliases = {"PA-P-10001"}
        result = transformer.transform(aliases, config)

        self.assertIn("P-10001", result)


class TestRegexSubstitutionTransformer(unittest.TestCase):
    """Test RegexSubstitutionTransformer."""

    def test_regex_pattern_replacement(self):
        """Test regex pattern replacement."""
        transformer = RegexSubstitutionHandler()
        config = {"patterns": [{"pattern": r"^P-(\d+)$", "replacement": r"PUMP-\1"}]}

        aliases = {"P-10001"}
        result = transformer.transform(aliases, config)

        self.assertIn("PUMP-10001", result)


class TestCaseTransformationTransformer(unittest.TestCase):
    """Test CaseTransformationTransformer."""

    def test_to_uppercase(self):
        """Test converting to uppercase."""
        transformer = CaseTransformationHandler()
        config = {"operation": "upper"}

        aliases = {"p-10001"}
        result = transformer.transform(aliases, config)

        self.assertIn("P-10001", result)

    def test_to_lowercase(self):
        """Test converting to lowercase."""
        transformer = CaseTransformationHandler()
        config = {"operation": "lower"}

        aliases = {"P-10001"}
        result = transformer.transform(aliases, config)

        self.assertIn("p-10001", result)


class TestLeadingZeroNormalizationTransformer(unittest.TestCase):
    """Test LeadingZeroNormalizationTransformer."""

    def test_strip_leading_zeros(self):
        """Test stripping leading zeros from numeric tokens."""
        transformer = LeadingZeroNormalizationHandler()
        config = {}

        aliases = {"P-001", "V-0201", "FIC-0500", "T-0000"}
        result = transformer.transform(aliases, config)

        self.assertIn("P-1", result)
        self.assertIn("V-201", result)
        self.assertIn("FIC-500", result)
        self.assertIn("T-0", result)

    def test_preserve_single_zero(self):
        """Test preserving single zero when configured."""
        transformer = LeadingZeroNormalizationHandler()
        config = {"preserve_single_zero": True}

        aliases = {"P-000"}
        result = transformer.transform(aliases, config)

        self.assertIn("P-0", result)

    def test_min_length_configuration(self):
        """Test min_length configuration option."""
        transformer = LeadingZeroNormalizationHandler()
        config = {"min_length": 3}

        aliases = {"P-01", "P-001"}
        result = transformer.transform(aliases, config)

        # P-01 should not be modified (length 2 < min_length 3)
        self.assertIn("P-01", result)
        # P-001 should be modified (length 3 >= min_length 3)
        self.assertIn("P-1", result)

    def test_multiple_numeric_tokens(self):
        """Test handling multiple numeric tokens in a single tag."""
        transformer = LeadingZeroNormalizationHandler()
        config = {}

        aliases = {"P-001-V-0201"}
        result = transformer.transform(aliases, config)

        self.assertIn("P-1-V-201", result)

    def test_original_preserved(self):
        """Test that original aliases are preserved."""
        transformer = LeadingZeroNormalizationHandler()
        config = {}

        aliases = {"P-10001"}
        result = transformer.transform(aliases, config)

        self.assertIn("P-10001", result)


class TestEquipmentTypeExpansionTransformer(unittest.TestCase):
    """Test EquipmentTypeExpansionTransformer."""

    def test_equipment_type_expansion(self):
        """Test expanding equipment type abbreviations."""
        transformer = EquipmentTypeExpansionHandler()
        config = {
            "type_mappings": {"P": ["PUMP", "PMP"]},
            "format_templates": ["{type}-{tag}"],
        }
        context = {"equipment_type": "pump"}

        aliases = {"P-10001"}
        result = transformer.transform(aliases, config, context)

        self.assertIn("PUMP-10001", result)
        self.assertIn("PMP-10001", result)


class TestRelatedInstrumentsTransformer(unittest.TestCase):
    """Test RelatedInstrumentsTransformer."""

    def test_generate_related_instruments(self):
        """Test generating related instrument tags."""
        transformer = RelatedInstrumentsHandler()
        config = {
            "instrument_types": [
                {"prefix": "FI", "applicable_to": ["pump"]},
                {"prefix": "FIC", "applicable_to": ["pump"]},
            ],
            "format_rules": {"separator": "-"},
        }
        context = {"equipment_type": "pump"}

        aliases = {"P-10001"}
        result = transformer.transform(aliases, config, context)

        # Should generate instrument tags
        self.assertTrue(len(result) > len(aliases))
        instrument_aliases = [a for a in result if "FI" in a or "FIC" in a]
        self.assertGreater(len(instrument_aliases), 0)


class TestHierarchicalExpansionTransformer(unittest.TestCase):
    """Test HierarchicalExpansionTransformer."""

    def test_hierarchical_expansion(self):
        """Test hierarchical tag expansion."""
        transformer = HierarchicalExpansionHandler()
        config = {
            "hierarchy_levels": [
                {"level": "site", "format": "{site}"},
                {"level": "unit", "format": "{site}-{unit}"},
                {"level": "equipment", "format": "{site}-{unit}-{equipment}"},
            ]
        }
        context = {"site": "Plant_A", "unit": "Unit_100"}

        aliases = {"P-10001"}
        result = transformer.transform(aliases, config, context)

        # Should generate hierarchical paths
        self.assertIn("Plant_A", result)
        self.assertIn("Plant_A-Unit_100", result)
        self.assertIn("Plant_A-Unit_100-P-10001", result)


class TestDocumentAliasesTransformer(unittest.TestCase):
    """Test DocumentAliasesTransformer."""

    def test_pid_aliases(self):
        """Test P&ID document aliases."""
        transformer = DocumentAliasesHandler()
        config = {
            "pid_rules": {
                "remove_ampersand": True,
                "add_spaces": True,
                "revision_variants": True,
            }
        }

        aliases = {"P&ID-2001-Rev-C"}
        result = transformer.transform(aliases, config)

        # Should generate variants
        self.assertTrue(len(result) > len(aliases))


if __name__ == "__main__":
    unittest.main()
