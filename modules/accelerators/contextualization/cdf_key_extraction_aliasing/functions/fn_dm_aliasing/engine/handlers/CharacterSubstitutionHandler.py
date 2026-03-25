"""Character substitution transformer handler."""

from typing import Any, Dict, Set

from .AliasTransformerHandler import AliasTransformerHandler


class CharacterSubstitutionHandler(AliasTransformerHandler):
    """Handles character substitution transformations."""

    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """
        Apply character substitution with support for multiple targets.

        Example config:
        {
            "substitutions": {
                "-": ["_", " ", ""],  # Multiple targets
                "_": "-"              # Single target
            },
            "cascade_substitutions": False,
            "max_aliases_per_input": 20,
            "bidirectional": False
        }
        """
        new_aliases = set()
        substitutions = config.get("substitutions", {})
        cascade = config.get("cascade_substitutions", False)
        max_aliases = config.get("max_aliases_per_input", 100)
        bidirectional = config.get("bidirectional", False)

        # Normalize substitutions to always be lists
        normalized_subs = {}
        for old_char, new_chars in substitutions.items():
            if isinstance(new_chars, str):
                normalized_subs[old_char] = [new_chars]
            else:
                normalized_subs[old_char] = new_chars

        # Add bidirectional substitutions
        if bidirectional:
            additional_subs = {}
            for old_char, new_chars in normalized_subs.items():
                for new_char in new_chars:
                    if new_char not in additional_subs:
                        additional_subs[new_char] = []
                    if old_char not in additional_subs[new_char]:
                        additional_subs[new_char].append(old_char)

            # Merge additional substitutions
            for old_char, new_chars in additional_subs.items():
                if old_char in normalized_subs:
                    normalized_subs[old_char].extend(new_chars)
                else:
                    normalized_subs[old_char] = new_chars

        # Process each alias
        for alias in aliases:
            variants = {alias}

            for old_char, new_chars in normalized_subs.items():
                if cascade:
                    # Apply substitutions to all current variants
                    new_variants = set()
                    for variant in variants:
                        if old_char in variant:
                            for new_char in new_chars:
                                new_variants.add(variant.replace(old_char, new_char))
                        new_variants.add(variant)  # Keep original variant
                    variants.update(new_variants)
                else:
                    # Apply substitutions only to original alias
                    if old_char in alias:
                        for new_char in new_chars:
                            variants.add(alias.replace(old_char, new_char))

            # Limit number of generated aliases per input
            if len(variants) > max_aliases:
                # Keep the most "standard" variants (prioritize by length and common patterns)
                variants = set(
                    sorted(variants, key=lambda x: (len(x), x))[:max_aliases]
                )

            new_aliases.update(variants)

        return new_aliases
