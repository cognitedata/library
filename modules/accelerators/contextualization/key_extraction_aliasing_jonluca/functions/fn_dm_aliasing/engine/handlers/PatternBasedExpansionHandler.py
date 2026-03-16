"""Pattern-based expansion transformer handler."""

import re
from typing import Any, Dict, List, Optional, Set

# Import pattern library components conditionally
try:
    from ..tag_pattern_library import EquipmentType, StandardTagPatternRegistry

    PATTERN_LIBRARY_AVAILABLE = True
except ImportError:
    PATTERN_LIBRARY_AVAILABLE = False
    StandardTagPatternRegistry = None
    EquipmentType = None

from .AliasTransformerHandler import AliasTransformerHandler


class PatternBasedExpansionHandler(AliasTransformerHandler):
    """Generate comprehensive aliases based on recognized patterns and industry standards."""

    def __init__(self, logger=None, client=None):
        super().__init__(logger, client)
        if PATTERN_LIBRARY_AVAILABLE:
            self.tag_registry = StandardTagPatternRegistry()
        else:
            self.tag_registry = None

    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """
        Generate pattern-based expansion aliases.

        Example config:
        {
            "include_industry_standards": True,
            "generate_similar_patterns": True,
            "equipment_type_variations": True,
            "instrument_loop_expansion": True
        }
        """
        if not PATTERN_LIBRARY_AVAILABLE or not self.tag_registry:
            return aliases

        new_aliases = set(aliases)
        include_standards = config.get("include_industry_standards", True)
        similar_patterns = config.get("generate_similar_patterns", True)
        equipment_variations = config.get("equipment_type_variations", True)
        loop_expansion = config.get("instrument_loop_expansion", True)

        for alias in aliases:
            # Get equipment type from context or pattern recognition
            equipment_type = None
            if context and context.get("equipment_type"):
                try:
                    equipment_type = EquipmentType(context["equipment_type"])
                except ValueError:
                    pass

            # If no equipment type in context, try pattern recognition
            if not equipment_type:
                matched_patterns = self._match_tag_patterns(alias)
                if matched_patterns:
                    equipment_type = matched_patterns[0].equipment_type

            if equipment_type and similar_patterns:
                # Generate aliases based on similar equipment patterns
                similar_aliases = self._generate_similar_equipment_aliases(
                    alias, equipment_type
                )
                new_aliases.update(similar_aliases)

            # Generate instrument loop aliases if applicable
            if (
                loop_expansion
                and equipment_type
                and EquipmentType
                and equipment_type
                in [
                    EquipmentType.PUMP,
                    EquipmentType.COMPRESSOR,
                    EquipmentType.TANK,
                ]
            ):
                loop_aliases = self._generate_instrument_loop_aliases(
                    alias, equipment_type
                )
                new_aliases.update(loop_aliases)

        return new_aliases

    def _match_tag_patterns(self, tag: str) -> List:
        """Match tag against patterns to determine equipment type."""
        matches = []

        for pattern in self.tag_registry.get_all_patterns():
            try:
                compiled_pattern = re.compile(pattern.pattern)
                if compiled_pattern.search(tag):
                    matches.append(pattern)
            except re.error:
                continue

        return sorted(matches, key=lambda p: p.priority)

    def _generate_similar_equipment_aliases(
        self, tag: str, equipment_type: EquipmentType
    ) -> Set[str]:
        """Generate aliases based on similar equipment patterns."""
        aliases = set()

        # Get patterns for the same equipment type
        patterns = self.tag_registry.get_patterns_by_type(equipment_type)

        for pattern in patterns[:3]:  # Limit to top 3 patterns
            for example in pattern.examples[:2]:  # Use first 2 examples
                # Try to adapt the example format to our tag
                adapted = self._adapt_format(tag, example)
                if adapted and adapted != tag:
                    aliases.add(adapted)

        return aliases

    def _generate_instrument_loop_aliases(
        self, tag: str, equipment_type: EquipmentType
    ) -> Set[str]:
        """Generate instrument loop aliases based on equipment type."""
        aliases = set()

        # Extract number from tag
        number_match = re.search(r"(\d+)", tag)
        if not number_match:
            return aliases

        number = number_match.group(1)

        # Define instrument prefixes based on equipment type
        instrument_prefixes = {
            EquipmentType.PUMP: ["FE", "FT", "FI", "FIC", "PE", "PT", "PI", "PIC"],
            EquipmentType.COMPRESSOR: [
                "PE",
                "PT",
                "PI",
                "PIC",
                "TE",
                "TT",
                "TI",
                "TIC",
            ],
            EquipmentType.TANK: ["LE", "LT", "LI", "LIC", "PE", "PT", "PI", "PIC"],
        }

        prefixes = instrument_prefixes.get(equipment_type, [])

        for prefix in prefixes:
            # Generate different separator formats
            aliases.add(f"{prefix}-{number}")
            aliases.add(f"{prefix}_{number}")
            aliases.add(f"{prefix}{number}")

        return aliases

    def _adapt_format(self, source_tag: str, example_format: str) -> Optional[str]:
        """Adapt an example format to match the source tag structure."""
        # Extract numeric part from source tag
        source_match = re.search(r"(\d+)", source_tag)
        if not source_match:
            return None

        source_number = source_match.group(1)

        # Try to extract structure from example format
        # Simple approach: replace numbers in example with source number
        adapted = re.sub(r"\d+", source_number, example_format)

        return adapted if adapted != source_tag else None
