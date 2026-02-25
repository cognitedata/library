"""Equipment type expansion transformer handler."""

import re
from typing import Any, Dict, Set

from ..transformer_utils import extract_equipment_number
from .AliasTransformerHandler import AliasTransformerHandler


class EquipmentTypeExpansionHandler(AliasTransformerHandler):
    """Handles equipment type expansion for semantic matching."""

    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """
        Add equipment type prefixes for semantic matching.

        Example config:
        {
            "type_mappings": {
                "P": ["PUMP", "PMP"],
                "V": ["VALVE", "VLV"],
                "T": ["TANK", "TNK"]
            },
            "format_templates": ["{type}-{tag}", "{type}_{tag}"],
            "auto_detect": True
        }
        """
        new_aliases = set()
        type_mappings = config.get("type_mappings", {})
        format_templates = config.get("format_templates", ["{type}-{tag}"])
        # Default to auto_detect if type_mappings are provided
        auto_detect = config.get("auto_detect", bool(type_mappings))

        for alias in aliases:
            # Try to detect equipment type from alias
            equipment_types = []

            if auto_detect:
                # Auto-detect based on patterns
                for prefix, types in type_mappings.items():
                    # Try basic pattern first (e.g., ^P[-_]?\d+)
                    pattern = f"^{re.escape(prefix)}[-_]?\\d+"
                    if re.match(pattern, alias):
                        equipment_types.extend(types)
                    else:
                        # Try hierarchical pattern (e.g., \d+-P[-_]?\d+)
                        hierarchical_pattern = f"\\d+-{re.escape(prefix)}[-_]?\\d+"
                        if re.search(hierarchical_pattern, alias):
                            equipment_types.extend(types)

            # Also check if equipment type is provided in context
            if context and context.get("equipment_type"):
                context_type = context["equipment_type"].upper()
                # Find matching type mappings
                for prefix, types in type_mappings.items():
                    if context_type in [t.upper() for t in types]:
                        equipment_types.extend(types)

            # Generate aliases with equipment types
            for eq_type in set(equipment_types):
                # Check if this is a hierarchical tag (e.g., 10-P-10001)
                hierarchical_prefix = None
                tag_part = alias

                # Try to detect hierarchical pattern first
                for prefix in type_mappings.keys():
                    hierarchical_pattern = (
                        f"^(\\d+[-_])({re.escape(prefix)}[-_]?)(\\d+)$"
                    )
                    match = re.match(hierarchical_pattern, alias)
                    if match:
                        hierarchical_prefix = match.group(1)  # e.g., "10-"
                        tag_part = match.group(3)  # e.g., "10001"
                        break

                # If not hierarchical, use standard extraction
                if hierarchical_prefix is None:
                    for prefix in type_mappings.keys():
                        pattern = f"^{re.escape(prefix)}[-_]?"
                        if re.match(pattern, alias):
                            # Remove the prefix and separator
                            tag_part = re.sub(pattern, "", alias)
                            break

                # Generate expanded aliases
                for template in format_templates:
                    try:
                        expanded_alias = template.format(type=eq_type, tag=tag_part)
                        # Add hierarchical prefix if this was a hierarchical tag
                        if hierarchical_prefix:
                            expanded_alias = hierarchical_prefix + expanded_alias
                        new_aliases.add(expanded_alias)
                    except KeyError:
                        self.logger.verbose(
                            "WARNING", f"Invalid template {template} for alias {alias}"
                        )

        # Always include original aliases
        new_aliases.update(aliases)
        return new_aliases
