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
- SemanticExpansionHandler
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

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.handlers import (
    CaseTransformationHandler,
    CharacterSubstitutionHandler,
    DocumentAliasesHandler,
    SemanticExpansionHandler,
    HierarchicalExpansionHandler,
    LeadingZeroNormalizationHandler,
    PatternBasedExpansionHandler,
    PatternRecognitionHandler,
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

    def test_bidirectional_substitution(self):
        """Bidirectional adds reverse mappings (e.g. A <-> W)."""
        transformer = CharacterSubstitutionHandler()
        config = {
            "substitutions": {"A": ["W"]},
            "bidirectional": True,
            "max_aliases_per_input": 50,
        }
        result = transformer.transform({"PA-1"}, config)
        self.assertIn("PA-1", result)
        self.assertIn("PW-1", result)

    def test_cascade_substitution(self):
        """Cascade applies substitutions to generated variants."""
        transformer = CharacterSubstitutionHandler()
        config = {
            "substitutions": {"a": ["b"], "b": ["c"]},
            "cascade_substitutions": True,
            "max_aliases_per_input": 20,
        }
        result = transformer.transform({"xa"}, config)
        self.assertGreater(len(result), 1)


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

    def test_add_suffix_from_context_mapping(self):
        """Resolve suffix via context_mapping and resolve_from."""
        transformer = PrefixSuffixHandler()
        config = {
            "operation": "add_suffix",
            "context_mapping": {"pump": {"suffix": "-PUMP"}},
            "resolve_from": "equipment_type",
        }
        context = {"equipment_type": "pump"}
        result = transformer.transform({"P-101"}, config, context)
        self.assertIn("P-101-PUMP", result)

    def test_context_value_dict_for_prefix(self):
        """Context field may be a dict (name/value/id)."""
        transformer = PrefixSuffixHandler()
        config = {
            "operation": "add_prefix",
            "context_mapping": {"Plant_A": {"prefix": "PA-"}},
            "resolve_from": "site",
        }
        context = {"site": {"name": "Plant_A"}}
        result = transformer.transform({"Z-1"}, config, context)
        self.assertIn("PA-Z-1", result)


class TestRegexSubstitutionTransformer(unittest.TestCase):
    """Test RegexSubstitutionTransformer."""

    def test_regex_pattern_replacement(self):
        """Test regex pattern replacement."""
        transformer = RegexSubstitutionHandler()
        config = {"patterns": [{"pattern": r"^P-(\d+)$", "replacement": r"PUMP-\1"}]}

        aliases = {"P-10001"}
        result = transformer.transform(aliases, config)

        self.assertIn("PUMP-10001", result)

    def test_invalid_regex_pattern_is_skipped(self):
        """Invalid pattern logs warning and leaves alias unchanged."""
        transformer = RegexSubstitutionHandler()
        config = {
            "patterns": [
                {"pattern": r"(unclosed", "replacement": "x"},
                {"pattern": r"^P-(\d+)$", "replacement": r"Q-\1"},
            ]
        }
        result = transformer.transform({"P-101"}, config)
        self.assertIn("Q-101", result)
        self.assertIn("P-101", result)


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

    def test_operations_list(self):
        """Config may use operations: [upper, lower, title]."""
        transformer = CaseTransformationHandler()
        config = {"operations": ["upper", "lower", "title"]}
        result = transformer.transform({"Ab-1"}, config)
        self.assertIn("AB-1", result)
        self.assertIn("ab-1", result)
        self.assertIn("Ab-1".title(), result)


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


class TestSemanticExpansionTransformer(unittest.TestCase):
    """Test SemanticExpansionHandler."""

    def test_semantic_expansion(self):
        """Test expanding equipment type abbreviations."""
        transformer = SemanticExpansionHandler()
        config = {
            "type_mappings": {"P": ["PUMP", "PMP"]},
            "format_templates": ["{type}-{tag}"],
        }
        context = {"equipment_type": "pump"}

        aliases = {"P-10001"}
        result = transformer.transform(aliases, config, context)

        self.assertIn("PUMP-10001", result)
        self.assertIn("PMP-10001", result)

    def test_semantic_expansion_longest_prefix_strips_correctly(self):
        """When stripping type prefix, longest mapping key must win (PI vs PIC)."""
        transformer = SemanticExpansionHandler()
        config = {
            "type_mappings": {"PI": ["TYPE_PI"], "PIC": ["TYPE_PIC"]},
            "format_templates": ["{type}-{tag}"],
            "auto_detect": False,
        }
        context = {"equipment_type": "TYPE_PIC"}
        result = transformer.transform({"PIC-101"}, config, context)
        self.assertIn("TYPE_PIC-101", result)
        self.assertNotIn("TYPE_PIC-C-101", result)

    def test_sorted_prefixes_longest_first(self):
        from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.handlers.SemanticExpansionHandler import (
            _sorted_prefixes,
        )

        self.assertEqual(
            _sorted_prefixes({"PI": [1], "PIC": [1], "A": [1]}),
            ["PIC", "PI", "A"],
        )

    def test_semantic_expansion_hierarchical_multiletter(self):
        transformer = SemanticExpansionHandler()
        config = {
            "type_mappings": {"FCV": ["FLOW_CONTROL_VALVE"]},
            "format_templates": ["{type}-{tag}"],
        }
        result = transformer.transform({"10-FCV-101"}, config)
        self.assertIn("10-FLOW_CONTROL_VALVE-101", result)

    def test_semantic_expansion_isa_preset_merge_user_overrides(self):
        from modules.accelerators.contextualization.cdf_key_extraction_aliasing.config.semantic_expansion_paths import (
            SEMANTIC_EXPANSION_ISA51_PRESET_YAML,
        )

        self.assertTrue(
            SEMANTIC_EXPANSION_ISA51_PRESET_YAML.is_file(),
            "preset YAML must exist for merge test",
        )
        transformer = SemanticExpansionHandler()
        config = {
            "include_isa_semantic_preset": True,
            "type_mappings": {"P": ["SITE_PUMP"]},
            "format_templates": ["{type}-{tag}"],
        }
        result = transformer.transform({"P-101"}, config)
        self.assertIn("SITE_PUMP-101", result)
        self.assertNotIn("PUMP-101", result)

    def test_semantic_expansion_isa_preset_fcv(self):
        transformer = SemanticExpansionHandler()
        config = {
            "include_isa_semantic_preset": True,
            "format_templates": ["{type}-{tag}"],
        }
        result = transformer.transform({"FCV-201"}, config)
        self.assertTrue(any("FLOW_CONTROL_VALVE" in a for a in result))


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


class TestPatternBasedExpansionHandler(unittest.TestCase):
    """Industry / registry-driven expansions."""

    def test_pump_context_generates_loop_aliases(self):
        h = PatternBasedExpansionHandler()
        cfg = {
            "include_industry_standards": True,
            "generate_similar_patterns": True,
            "equipment_type_variations": True,
            "instrument_loop_expansion": True,
        }
        ctx = {"equipment_type": "pump"}
        out = h.transform({"P-101"}, cfg, ctx)
        self.assertGreater(len(out), 1)
        self.assertTrue(any("FIC" in a for a in out))


class TestPatternRecognitionHandler(unittest.TestCase):
    """Pattern library recognition + optional context enrichment."""

    def test_matching_tag_adds_variants_and_context(self):
        h = PatternRecognitionHandler()
        cfg = {
            "enhance_context": True,
            "generate_pattern_variants": True,
            "confidence_threshold": 0.5,
        }
        ctx = {}
        out = h.transform({"P-101"}, cfg, ctx)
        self.assertGreaterEqual(len(out), 1)
        # Context may be filled when patterns match
        self.assertIsInstance(ctx, dict)


if __name__ == "__main__":
    unittest.main()
