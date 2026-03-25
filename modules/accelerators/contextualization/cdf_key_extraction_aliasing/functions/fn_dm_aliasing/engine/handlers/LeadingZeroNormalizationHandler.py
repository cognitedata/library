"""Leading zero normalization transformer handler."""

import re
from typing import Any, Dict, Set

from .AliasTransformerHandler import AliasTransformerHandler


class LeadingZeroNormalizationHandler(AliasTransformerHandler):
    """Handles stripping leading zeros from numeric tokens in tags."""

    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """
        Strip leading zeros from numeric tokens.

        Example config:
        {
            "preserve_single_zero": false,  # Whether to preserve "0" as-is
            "min_length": 1  # Minimum length of numeric token to process
        }

        Examples:
        - P-001 → P-1
        - V-0201 → V-201
        - FIC-0500 → FIC-500
        - 0000A remains 0000A (not all numeric)
        """
        new_aliases = set()
        preserve_single_zero = config.get("preserve_single_zero", False)
        min_length = config.get("min_length", 1)

        for alias in aliases:
            new_alias = alias
            replacements = []

            # Find all numeric sequences (groups of one or more digits)
            # that are not preceded by a word character and followed by non-digit or end
            numeric_tokens = list(re.finditer(r"\b0+(\d+)\b", alias))

            for match in numeric_tokens:
                full_match = match.group(0)  # e.g., "001"
                numeric_part = match.group(1)  # e.g., "1"
                start_pos = match.start()
                end_pos = match.end()

                # Skip if length requirement not met
                if len(full_match) < min_length:
                    continue

                # Special case: if preserve_single_zero is True and result is "0", keep original
                if preserve_single_zero and numeric_part == "" and full_match == "0":
                    continue

                # Build the replacement
                if numeric_part:
                    replacement = numeric_part
                else:
                    # All zeros - preserve at least one if preserve_single_zero
                    replacement = "0" if preserve_single_zero else ""
                    if not replacement:
                        continue

                replacements.append((start_pos, end_pos, replacement))

            # Apply replacements in reverse order to maintain positions
            for start_pos, end_pos, replacement in reversed(replacements):
                new_alias = new_alias[:start_pos] + replacement + new_alias[end_pos:]

            new_aliases.add(new_alias)

        return new_aliases
