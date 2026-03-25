"""Related instruments transformer handler."""

from typing import Any, Dict, Set

from ..transformer_utils import extract_equipment_number, generate_separator_variants
from .AliasTransformerHandler import AliasTransformerHandler


class RelatedInstrumentsHandler(AliasTransformerHandler):
    """Generates related instrument tags for equipment."""

    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """
        Generate related instrument tag aliases.

        Example config:
        {
            "applicable_equipment_types": ["pump", "compressor"],
            "instrument_types": [
                {"prefix": "FIC", "applicable_to": ["pump"]},
                {"prefix": "PI", "applicable_to": ["pump", "compressor"]}
            ],
            "format_rules": {"separator": "-", "case": "upper"}
        }
        """
        new_aliases = set(aliases)  # Include original aliases

        if not context or not context.get("equipment_type"):
            return new_aliases

        equipment_type = context["equipment_type"].lower()
        applicable_types = config.get("applicable_equipment_types", [])

        # If no applicable_equipment_types specified, assume all types are applicable
        if applicable_types and equipment_type not in applicable_types:
            return new_aliases

        instrument_types = config.get("instrument_types", [])
        format_rules = config.get("format_rules", {"separator": "-", "case": "upper"})
        separator = format_rules.get("separator", "-")

        for alias in aliases:
            # Extract equipment number using utility function
            equipment_number = extract_equipment_number(alias)
            if not equipment_number:
                continue

            # Generate instrument tags
            for instrument in instrument_types:
                prefix = instrument.get("prefix")
                applicable_to = instrument.get("applicable_to", [])

                if equipment_type in applicable_to:
                    # Generate base instrument tag with preferred separator
                    base_tag = f"{prefix}{separator}{equipment_number}"
                    new_aliases.add(base_tag)

                    # Generate separator variants using utility function
                    variants = generate_separator_variants(base_tag, ["-", "_", ""])
                    new_aliases.update(variants)

        return new_aliases
