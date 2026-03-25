"""Regex substitution transformer handler."""

import re
from typing import Any, Dict, Set

from .AliasTransformerHandler import AliasTransformerHandler


class RegexSubstitutionHandler(AliasTransformerHandler):
    """Handles regex-based pattern substitutions."""

    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """
        Apply regex substitution patterns.

        Example config:
        {
            "patterns": [
                {
                    "pattern": "^([A-Z]+)(\\d+)([A-Z]?)$",
                    "replacement": "\\1-\\2\\3"
                }
            ]
        }
        """
        new_aliases = set()
        patterns = config.get("patterns", [])

        # Handle single pattern format
        if "pattern" in config:
            patterns = [
                {
                    "pattern": config["pattern"],
                    "replacement": config.get("replacement", ""),
                }
            ]

        for alias in aliases:
            alias_variants = {alias}

            for pattern_config in patterns:
                pattern = pattern_config["pattern"]
                replacement = pattern_config["replacement"]

                try:
                    compiled_pattern = re.compile(pattern)
                    if compiled_pattern.match(alias):
                        new_variant = compiled_pattern.sub(replacement, alias)
                        alias_variants.add(new_variant)
                except re.error as e:
                    self.logger.verbose(
                        "WARNING", f"Invalid regex pattern {pattern}: {e}"
                    )

            new_aliases.update(alias_variants)

        return new_aliases
