"""Pattern recognition transformer handler."""

from typing import Any, Dict, Set

from ..transformer_utils import PatternMatchMixin

# Import pattern library components conditionally
try:
    from ..tag_pattern_library import (
        DocumentPatternRegistry,
        StandardTagPatternRegistry,
    )

    PATTERN_LIBRARY_AVAILABLE = True
except ImportError:
    PATTERN_LIBRARY_AVAILABLE = False
    StandardTagPatternRegistry = None
    DocumentPatternRegistry = None

from .AliasTransformerHandler import AliasTransformerHandler


class PatternRecognitionHandler(AliasTransformerHandler, PatternMatchMixin):
    """Uses pattern library to identify equipment types and generate context-aware aliases."""

    def __init__(self, logger=None, client=None):
        super().__init__(logger, client)
        if PATTERN_LIBRARY_AVAILABLE:
            self.tag_registry = StandardTagPatternRegistry()
            self.doc_registry = DocumentPatternRegistry()
        else:
            self.tag_registry = None
            self.doc_registry = None

        # Initialize PatternMatchMixin with tag_registry
        PatternMatchMixin.__init__(self, self.tag_registry)

    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """
        Recognize patterns in tags and enhance context information.

        Example config:
        {
            "enhance_context": True,
            "generate_pattern_variants": True,
            "confidence_threshold": 0.7,
            "equipment_type_mapping": True
        }
        """
        if not PATTERN_LIBRARY_AVAILABLE or not self.tag_registry:
            self.logger.verbose(
                "WARNING", "Pattern library not available for pattern recognition"
            )
            return aliases

        new_aliases = set(aliases)
        enhance_context = config.get("enhance_context", True)
        generate_variants = config.get("generate_pattern_variants", True)
        confidence_threshold = config.get("confidence_threshold", 0.7)

        for alias in aliases:
            # Try to match against known patterns
            matched_patterns = self._match_patterns(alias)

            if matched_patterns and enhance_context and context is not None:
                # Update context with recognized equipment type
                best_match = max(matched_patterns, key=lambda p: p.priority)

                # Only update context if we don't already have equipment type
                if not context.get("equipment_type"):
                    context["equipment_type"] = best_match.equipment_type.value
                    context["pattern_matched"] = best_match.name
                    context["industry_standard"] = best_match.industry_standard

                # Add instrument type if applicable
                if best_match.instrument_type:
                    context["instrument_type"] = best_match.instrument_type.value

            if generate_variants and matched_patterns:
                # Generate pattern-based variants
                for pattern in matched_patterns:
                    variants = self._generate_pattern_variants(alias, pattern)
                    new_aliases.update(variants)

        return new_aliases

    def _generate_pattern_variants(self, tag: str, pattern) -> Set[str]:
        """Generate variants based on pattern examples."""
        variants = set()

        # Add pattern examples as inspiration for variants
        for example in pattern.examples[:3]:  # Limit to first 3 examples
            # Use mixin methods to extract structure
            example_structure = self.extract_structure(example)
            tag_structure = self.extract_structure(tag)

            if example_structure and tag_structure:
                # Generate variant by combining structures using mixin method
                variant = self.apply_structure_variant(tag, example_structure)
                if variant and variant != tag:
                    variants.add(variant)

        return variants
