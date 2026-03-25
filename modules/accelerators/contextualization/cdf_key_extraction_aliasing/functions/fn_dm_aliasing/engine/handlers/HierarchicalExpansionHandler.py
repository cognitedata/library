"""Hierarchical expansion transformer handler."""

import re
from typing import Any, Dict, Set

from .AliasTransformerHandler import AliasTransformerHandler


class HierarchicalExpansionHandler(AliasTransformerHandler):
    """Handles hierarchical tag expansion."""

    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """
        Generate hierarchical tag relationships.

        Skips alias generation if any required context values (site, unit, etc.)
        are None, null, or empty to avoid generating aliases with missing segments.

        Example config:
        {
            "hierarchy_levels": [
                {"level": "site", "format": "{site}"},
                {"level": "unit", "format": "{site}-{unit}"},
                {"level": "equipment", "format": "{site}-{unit}-{equipment}"}
            ]
        }
        """
        new_aliases = set(aliases)

        if not context:
            return new_aliases

        hierarchy_levels = config.get("hierarchy_levels", [])

        for alias in aliases:
            # Build hierarchical aliases based on context
            for level_config in hierarchy_levels:
                level_format = level_config.get("format", "{equipment}")

                try:
                    # Add the equipment tag to context for formatting
                    format_context = dict(context)
                    format_context["equipment"] = alias

                    # Check if any required context values are None or null before generating alias
                    # Extract all format placeholders (e.g., {site}, {unit}, {equipment})
                    placeholders = re.findall(r"\{(\w+)\}", level_format)

                    # Check if any placeholder values are None, null, or empty
                    skip_alias = False
                    for placeholder in placeholders:
                        value = format_context.get(placeholder)
                        if value is None or str(value).lower() in [
                            "null",
                            "none",
                            "n/a",
                            "na",
                            "",
                        ]:
                            skip_alias = True
                            self.logger.verbose(
                                "DEBUG",
                                f"Skipping hierarchical alias generation: {placeholder} is None/null "
                                f"for format '{level_format}'",
                            )
                            break

                    if skip_alias:
                        continue

                    hierarchical_alias = level_format.format(**format_context)
                    new_aliases.add(hierarchical_alias)

                except KeyError as e:
                    self.logger.verbose(
                        "DEBUG", f"Missing context key {e} for hierarchical expansion"
                    )

        return new_aliases
