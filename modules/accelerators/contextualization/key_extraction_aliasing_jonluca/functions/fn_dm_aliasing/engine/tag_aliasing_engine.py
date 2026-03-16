"""
Tag Aliasing Engine for Cognite Data Fusion (CDF)

This module implements a comprehensive tag aliasing system that generates multiple
alternative representations of asset tags, equipment identifiers, and document names
to improve entity matching and contextualization accuracy.

Features:
- 10 transformation types (character substitution, prefix/suffix ops, regex, etc.)
- Support for related instrument tag generation
- Equipment type expansion for semantic matching
- Hierarchical tag expansion
- Document-specific aliasing
- Composite transformations

Author: Darren Downtain
Version: 1.0.0
"""

import re
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union

import yaml

from ..common.logger import CogniteFunctionLogger
from .transformer_utils import (
    STANDARD_TAG_PATTERN,
    PatternMatchMixin,
    extract_equipment_number,
    extract_tag_structure,
    generate_separator_variants,
)

# Import pattern library components
try:
    from .tag_pattern_library import (
        DocumentPatternRegistry,
        DocumentType,
        EquipmentType,
        InstrumentType,
        PatternValidator,
        StandardTagPatternRegistry,
    )

    PATTERN_LIBRARY_AVAILABLE = True
except ImportError:
    PATTERN_LIBRARY_AVAILABLE = False
    # Logger will be initialized in __init__, use print for now
    print(
        "[WARNING] Pattern library not available. Pattern-based transformations disabled."
    )

# Import handlers
from .handlers import (
    AliasTransformerHandler,
    CaseTransformationHandler,
    CharacterSubstitutionHandler,
    DocumentAliasesHandler,
    EquipmentTypeExpansionHandler,
    HierarchicalExpansionHandler,
    LeadingZeroNormalizationHandler,
    PatternBasedExpansionHandler,
    PatternRecognitionHandler,
    PrefixSuffixHandler,
    RegexSubstitutionHandler,
    RelatedInstrumentsHandler,
)


class TransformationType(Enum):
    """Enumeration of available aliasing transformation types."""

    CHARACTER_SUBSTITUTION = "character_substitution"
    PREFIX_SUFFIX = "prefix_suffix"
    REGEX_SUBSTITUTION = "regex_substitution"
    CASE_TRANSFORMATION = "case_transformation"
    EQUIPMENT_TYPE_EXPANSION = "equipment_type_expansion"
    RELATED_INSTRUMENTS = "related_instruments"
    HIERARCHICAL_EXPANSION = "hierarchical_expansion"
    DOCUMENT_ALIASES = "document_aliases"
    LEADING_ZERO_NORMALIZATION = "leading_zero_normalization"
    COMPOSITE = "composite"
    PATTERN_RECOGNITION = "pattern_recognition"
    PATTERN_BASED_EXPANSION = "pattern_based_expansion"


@dataclass
class AliasRule:
    """Configuration for an individual aliasing rule."""

    name: str
    type: TransformationType
    enabled: bool = True
    priority: int = 50
    preserve_original: bool = True
    config: Dict[str, Any] = field(default_factory=dict)
    scope_filters: Dict[str, Any] = field(default_factory=dict)
    conditions: Dict[str, Any] = field(default_factory=dict)
    description: str = ""


@dataclass
class AliasingResult:
    """Result of aliasing operation."""

    original_tag: str
    aliases: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)


# Transformer classes have been moved to handlers module
# All transformer functionality is now in separate handler files under engine/handlers/


class AliasingEngine:
    """Main engine for generating tag aliases."""

    def __init__(
        self,
        config: Dict[str, Any],
        logger: CogniteFunctionLogger = CogniteFunctionLogger("INFO", True),
    ):
        """Initialize the aliasing engine with configuration."""
        self.config = config
        self.logger = logger
        self.rules = self._load_rules()
        self.transformers = self._initialize_transformers()
        self.validation_config = config.get("validation", {})

    def _load_rules(self) -> List[AliasRule]:
        """Load and parse aliasing rules from configuration."""
        rules = []
        rules_config = self.config.get("rules", [])

        for rule_config in rules_config:
            try:
                rule = AliasRule(
                    name=rule_config["name"],
                    type=TransformationType(rule_config["type"]),
                    enabled=rule_config.get("enabled", True),
                    priority=rule_config.get("priority", 50),
                    preserve_original=rule_config.get("preserve_original", True),
                    config=rule_config.get("config", {}),
                    scope_filters=rule_config.get("scope_filters", {}),
                    conditions=rule_config.get("conditions", {}),
                    description=rule_config.get("description", ""),
                )
                rules.append(rule)
            except (KeyError, ValueError) as e:
                self.logger.error(f"Invalid rule configuration: {e}")

        return sorted(rules, key=lambda r: r.priority)

    def _initialize_transformers(
        self,
    ) -> Dict[TransformationType, AliasTransformerHandler]:
        """Initialize transformer handler instances."""
        transformers = {
            TransformationType.CHARACTER_SUBSTITUTION: CharacterSubstitutionHandler(
                self.logger
            ),
            TransformationType.PREFIX_SUFFIX: PrefixSuffixHandler(self.logger),
            TransformationType.REGEX_SUBSTITUTION: RegexSubstitutionHandler(
                self.logger
            ),
            TransformationType.CASE_TRANSFORMATION: CaseTransformationHandler(
                self.logger
            ),
            TransformationType.LEADING_ZERO_NORMALIZATION: LeadingZeroNormalizationHandler(
                self.logger
            ),
            TransformationType.EQUIPMENT_TYPE_EXPANSION: EquipmentTypeExpansionHandler(
                self.logger
            ),
            TransformationType.RELATED_INSTRUMENTS: RelatedInstrumentsHandler(
                self.logger
            ),
            TransformationType.HIERARCHICAL_EXPANSION: HierarchicalExpansionHandler(
                self.logger
            ),
            TransformationType.DOCUMENT_ALIASES: DocumentAliasesHandler(self.logger),
        }

        # Add pattern-based transformers if pattern library is available
        if PATTERN_LIBRARY_AVAILABLE:
            transformers.update(
                {
                    TransformationType.PATTERN_RECOGNITION: PatternRecognitionHandler(
                        self.logger
                    ),
                    TransformationType.PATTERN_BASED_EXPANSION: PatternBasedExpansionHandler(
                        self.logger
                    ),
                }
            )

        return transformers

    def generate_aliases(
        self, tag: str, entity_type: str = None, context: Dict[str, Any] = None
    ) -> AliasingResult:
        """
        Generate all aliases for a given tag.

        Args:
            tag: Base tag to generate aliases for
            entity_type: Type of entity (asset, file, etc.)
            context: Additional context (site, unit, equipment_type, etc.)

        Returns:
            AliasingResult with generated aliases and metadata
        """
        aliases = {tag}  # Start with original
        applied_rules = []  # Use list to preserve order
        applied_rules_set = set()  # Track unique rule names

        for rule in self.rules:
            if not rule.enabled:
                continue

            # Check if rule conditions are met
            if not self._check_conditions(rule, entity_type, context):
                continue

            # Get appropriate transformer
            transformer = self.transformers.get(rule.type)
            if not transformer:
                self.logger.verbose(
                    "WARNING", f"No transformer for rule type {rule.type}"
                )
                continue

            # Apply transformer
            try:
                current_aliases = set(aliases)
                new_aliases = transformer.transform(
                    current_aliases, rule.config, context
                )

                if rule.preserve_original:
                    aliases.update(new_aliases)
                else:
                    aliases = new_aliases

                # Only add rule name if not already added (remove duplicates)
                if rule.name not in applied_rules_set:
                    applied_rules.append(rule.name)
                    applied_rules_set.add(rule.name)

            except Exception as e:
                self.logger.verbose("ERROR", f"Error applying rule {rule.name}: {e}")

        # Apply validation
        validated_aliases = self._validate_aliases(list(aliases))

        return AliasingResult(
            original_tag=tag,
            aliases=validated_aliases,
            metadata={
                "applied_rules": applied_rules,
                "total_aliases": len(validated_aliases),
                "entity_type": entity_type,
                "context": context,
            },
        )

    def _check_conditions(
        self, rule: AliasRule, entity_type: str, context: Dict[str, Any]
    ) -> bool:
        """Check if rule conditions are satisfied."""
        # Check scope filters first (aligned with key extraction)
        if rule.scope_filters:
            # Check entity_type in scope_filters (primary method, aligned with key extraction)
            if "entity_type" in rule.scope_filters:
                allowed_types = rule.scope_filters["entity_type"]
                if isinstance(allowed_types, str):
                    allowed_types = [allowed_types]
                if entity_type not in allowed_types:
                    return False

            # Check other scope filters
            if context:
                for key, expected_value in rule.scope_filters.items():
                    if key == "entity_type":
                        continue
                    actual_value = context.get(key)
                    if isinstance(expected_value, list):
                        if actual_value not in expected_value:
                            return False
                    else:
                        if actual_value != expected_value:
                            return False

        # Fallback to conditions for backward compatibility
        if rule.conditions:
            # Check entity_type in conditions (backward compatibility)
            if "entity_type" in rule.conditions:
                allowed_types = rule.conditions["entity_type"]
                if isinstance(allowed_types, str):
                    allowed_types = [allowed_types]
                if entity_type not in allowed_types:
                    return False

            # Check other context conditions
            if context:
                for key, expected_value in rule.conditions.items():
                    if key == "entity_type":
                        continue

                    actual_value = context.get(key)
                    if isinstance(expected_value, list):
                        if actual_value not in expected_value:
                            return False
                    else:
                        if actual_value != expected_value:
                            return False

        return True

    def _validate_aliases(self, aliases: List[str]) -> List[str]:
        """Apply validation rules to generated aliases."""
        validated = []

        min_length = self.validation_config.get("min_alias_length", 1)
        max_length = self.validation_config.get("max_alias_length", 100)
        max_aliases = self.validation_config.get("max_aliases_per_tag", 50)
        allowed_chars = self.validation_config.get(
            "allowed_characters", r"A-Za-z0-9-_/. "
        )

        # Create regex pattern for allowed characters - fix the escaping issue
        if allowed_chars:
            # Don't escape if it's already a character class pattern
            if allowed_chars.startswith("[") and allowed_chars.endswith("]"):
                char_pattern = f"^{allowed_chars}+$"
            else:
                # For simple string, create character class but handle spaces properly
                char_pattern = f"^[{allowed_chars}]+$"
        else:
            char_pattern = None

        for alias in aliases:
            # Skip empty aliases
            if not alias:
                continue

            # Check length
            if len(alias) < min_length or len(alias) > max_length:
                self.logger.verbose(
                    "DEBUG", f"Alias '{alias}' rejected due to length: {len(alias)}"
                )
                continue

            # Check allowed characters
            if char_pattern and not re.match(char_pattern, alias):
                self.logger.verbose(
                    "DEBUG",
                    f"Alias '{alias}' rejected due to invalid characters. Pattern: {char_pattern}",
                )
                continue

            validated.append(alias)

        # Remove duplicates while preserving order
        seen = set()
        validated = [x for x in validated if not (x in seen or seen.add(x))]

        # Limit number of aliases
        if len(validated) > max_aliases:
            # Sort and take the first max_aliases items
            validated = sorted(validated)[:max_aliases]

        return validated


def load_config_from_yaml(file_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    from ...common.config_utils import load_config_from_yaml as _load_config_from_yaml

    return _load_config_from_yaml(file_path)


def main():
    """Example usage of the AliasingEngine."""

    # Example configuration
    config = {
        "rules": [
            {
                "name": "normalize_separators",
                "type": "character_substitution",
                "enabled": True,
                "priority": 10,
                "preserve_original": True,
                "config": {"substitutions": {"_": "-", " ": "-"}},
            },
            {
                "name": "generate_separator_variants",
                "type": "character_substitution",
                "enabled": True,
                "priority": 15,
                "preserve_original": True,
                "config": {
                    "substitutions": {"-": ["_", " ", ""]},
                    "cascade_substitutions": False,
                    "max_aliases_per_input": 20,
                },
            },
            {
                "name": "add_site_prefix",
                "type": "prefix_suffix",
                "enabled": True,
                "priority": 20,
                "preserve_original": True,
                "config": {
                    "operation": "add_prefix",
                    "context_mapping": {
                        "Plant_A": {"prefix": "PA-"},
                        "Plant_B": {"prefix": "PB-"},
                    },
                    "resolve_from": "site",
                    "conditions": {"missing_prefix": True},
                },
            },
            {
                "name": "equipment_type_expansion",
                "type": "equipment_type_expansion",
                "enabled": True,
                "priority": 30,
                "preserve_original": True,
                "config": {
                    "type_mappings": {
                        "P": ["PUMP", "PMP"],
                        "V": ["VALVE", "VLV"],
                        "T": ["TANK", "TNK"],
                    },
                    "format_templates": ["{type}-{tag}", "{type}_{tag}"],
                    "auto_detect": True,
                },
            },
            {
                "name": "generate_instruments",
                "type": "related_instruments",
                "enabled": True,
                "priority": 40,
                "preserve_original": True,
                "config": {
                    "applicable_equipment_types": ["pump", "compressor", "tank"],
                    "instrument_types": [
                        {"prefix": "FIC", "applicable_to": ["pump", "compressor"]},
                        {
                            "prefix": "PI",
                            "applicable_to": ["pump", "compressor", "tank"],
                        },
                        {"prefix": "TI", "applicable_to": ["pump"]},
                        {"prefix": "LIC", "applicable_to": ["tank"]},
                    ],
                    "format_rules": {"separator": "-", "case": "upper"},
                },
            },
        ],
        "validation": {
            "max_aliases_per_tag": 30,
            "min_alias_length": 2,
            "max_alias_length": 50,
        },
    }

    # Initialize engine with default logger
    default_logger = CogniteFunctionLogger("INFO", True)
    engine = AliasingEngine(config, default_logger)

    # Test cases
    test_cases = [
        {
            "tag": "P_101",
            "entity_type": "asset",
            "context": {"site": "Plant_A", "equipment_type": "pump"},
        },
        {
            "tag": "FCV-2001-A",
            "entity_type": "asset",
            "context": {"site": "Plant_B", "equipment_type": "valve"},
        },
        {
            "tag": "T-201",
            "entity_type": "asset",
            "context": {"site": "Plant_A", "equipment_type": "tank"},
        },
    ]

    print("Tag Aliasing Engine - Test Results")
    print("=" * 50)

    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest Case {i}:")
        print(f"Input: {test_case['tag']}")
        print(f"Type: {test_case['entity_type']}")
        print(f"Context: {test_case['context']}")

        result = engine.generate_aliases(**test_case)

        print(f"Generated {len(result.aliases)} aliases:")
        for alias in sorted(result.aliases):
            print(f"  - {alias}")

        print(f"Applied rules: {result.metadata['applied_rules']}")


if __name__ == "__main__":
    main()
