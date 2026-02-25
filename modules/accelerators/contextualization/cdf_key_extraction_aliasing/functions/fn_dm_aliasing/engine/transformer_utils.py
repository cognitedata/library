"""
Utility functions for tag alias transformers.

This module provides shared utility functions to eliminate code duplication
across different transformer implementations.
"""

import logging
import re
from abc import ABC
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# Standard tag structure pattern: prefix-number-suffix
# Example: "P-10001", "FIC_001-A", "PUMP10001"
STANDARD_TAG_PATTERN = re.compile(r"^([A-Z]+)[-_]?(\d+)([A-Z]?)$")

# Hierarchical tag pattern: unit-prefix-number
# Example: "10-P-10001", "20_V-2001"
HIERARCHICAL_TAG_PATTERN = re.compile(r"^(\d+[-_])([A-Z][-_]?)(\d+)$")


def extract_equipment_number(tag: str) -> Optional[str]:
    """
    Extract the first numeric sequence from a tag.

    Args:
        tag: Tag string to extract number from

    Returns:
        First numeric sequence found, or None if no number found

    Examples:
        >>> extract_equipment_number("P-10001")
        '10001'
        >>> extract_equipment_number("PUMP_001-A")
        '001'
        >>> extract_equipment_number("NO-NUMBER")
        None
    """
    match = re.search(r"(\d+)", tag)
    return match.group(1) if match else None


def extract_tag_structure(tag: str) -> Optional[dict]:
    """
    Extract structural elements from a standard tag format.

    Args:
        tag: Tag string to parse (e.g., "P-10001", "FIC_001-A")

    Returns:
        Dictionary with 'prefix', 'number', 'suffix' keys, or None if no match

    Examples:
        >>> extract_tag_structure("P-10001")
        {'prefix': 'P', 'number': '10001', 'suffix': ''}
        >>> extract_tag_structure("FIC_001-A")
        {'prefix': 'FIC', 'number': '001', 'suffix': 'A'}
        >>> extract_tag_structure("invalid")
        None
    """
    match = STANDARD_TAG_PATTERN.match(tag)
    if match:
        return {
            "prefix": match.group(1),
            "number": match.group(2),
            "suffix": match.group(3) or "",
        }
    return None


def generate_separator_variants(
    tag: str,
    separators: Optional[List[str]] = None,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
) -> Set[str]:
    """
    Generate tag variants with different separator characters.

    Args:
        tag: Base tag string
        separators: List of separators to use (default: ["-", "_", ""])
        prefix: Optional prefix to prepend to all variants
        suffix: Optional suffix to append to all variants

    Returns:
        Set of tag variants with different separators

    Examples:
        >>> generate_separator_variants("P-10001")
        {'P-10001', 'P_10001', 'P10001'}
        >>> generate_separator_variants("A B C", separators=["-", "_"])
        {'A-B-C', 'A_B_C'}
        >>> generate_separator_variants("P-10001", prefix="10-")
        {'10-P-10001', '10-P_10001', '10-P10001'}
    """
    if separators is None:
        separators = ["-", "_", ""]

    variants = set()

    # Find existing separators in the tag
    existing_separators = set(re.findall(r"[-_\s/.]", tag))

    # Generate variants for each target separator
    for target_sep in separators:
        variant = tag
        for existing_sep in existing_separators:
            variant = variant.replace(existing_sep, target_sep)

        # Add prefix/suffix if provided
        if prefix:
            variant = f"{prefix}{variant}"
        if suffix:
            variant = f"{variant}{suffix}"

        variants.add(variant)

    return variants


def extract_hierarchical_structure(tag: str) -> Optional[dict]:
    """
    Extract hierarchical elements from a hierarchical tag format.

    Args:
        tag: Hierarchical tag string (e.g., "10-P-10001", "20_V-2001")

    Returns:
        Dictionary with 'unit', 'equipment', 'number' keys, or None if no match

    Examples:
        >>> extract_hierarchical_structure("10-P-10001")
        {'unit': '10', 'equipment': 'P', 'number': '10001'}
        >>> extract_hierarchical_structure("20_V-2001")
        {'unit': '20', new 'equipment': 'V', 'number': '2001'}
        >>> extract_hierarchical_structure("P-10001")
        None
    """
    match = HIERARCHICAL_TAG_PATTERN.match(tag)
    if match:
        unit = match.group(1).rstrip("-_")
        equipment = match.group(2).strip("-_")
        number = match.group(3)
        return {
            "unit": unit,
            "equipment": equipment,
            "number": number,
        }
    return None


def normalize_separators(tag: str, target_separator: str = "-") -> str:
    """
    Normalize all separator characters in a tag to a single target separator.

    Args:
        tag: Tag string to normalize
        target_separator: Separator character to use (default: "-")

    Returns:
        Tag with normalized separators

    Examples:
        >>> normalize_separators("P_10001")
        'P-10001'
        >>> normalize_separators("A B C", target_separator="_")
        'A_B_C'
    """
    # Replace all common separator characters
    for sep in ["-", "_", " ", "/", "."]:
        tag = tag.replace(sep, target_separator)
    return tag


class PatternMatchMixin(ABC):
    """
    Mixin class providing shared pattern matching functionality for transformers.

    This class consolidates duplicate pattern matching logic used across
    PatternRecognitionTransformer and PatternBasedExpansionTransformer.
    """

    def __init__(self, tag_registry=None):
        """Initialize with an optional tag registry."""
        self.tag_registry = tag_registry

    def match_patterns(self, tag: str) -> List:
        """
        Match tag against known patterns in the registry.

        Args:
            tag: Tag string to match

        Returns:
            List of matching patterns, sorted by priority
        """
        if not self.tag_registry:
            return []

        matches = []

        for pattern in self.tag_registry.get_all_patterns():
            try:
                compiled_pattern = re.compile(pattern.pattern)
                if compiled_pattern.search(tag):
                    matches.append(pattern)
            except re.error as e:
                # Use handler's logger if available (from CogniteFunctionLogger), otherwise fall back to standard logger
                if hasattr(self, "logger"):
                    self.logger.verbose(
                        "DEBUG", f"Invalid regex pattern {pattern.pattern}: {e}"
                    )
                else:
                    logger.debug(f"Invalid regex pattern {pattern.pattern}: {e}")
                continue

        # Sort by priority (lower number = higher priority)
        return sorted(matches, key=lambda p: p.priority)

    def extract_structure(self, tag: str) -> Optional[Dict[str, str]]:
        """
        Extract structural elements from a tag.

        Uses the shared STANDARD_TAG_PATTERN for consistency.

        Args:
            tag: Tag string to parse

        Returns:
            Dictionary with 'prefix', 'number', 'suffix' keys, or None
        """
        return extract_tag_structure(tag)

    def apply_structure_variant(
        self, tag: str, structure: Dict[str, str]
    ) -> Optional[str]:
        """
        Apply structure variant to generate new alias.

        Args:
            tag: Source tag
            structure: Target structure dictionary

        Returns:
            Generated variant or None if not applicable
        """
        tag_structure = self.extract_structure(tag)
        if not tag_structure:
            return None

        # Try different separator combinations
        separators = ["-", "_", ""]
        for sep in separators:
            if sep:
                variant = f"{tag_structure['prefix']}{sep}{tag_structure['number']}{tag_structure['suffix']}"
            else:
                variant = f"{tag_structure['prefix']}{tag_structure['number']}{tag_structure['suffix']}"

            if variant != tag:
                return variant

        return None
