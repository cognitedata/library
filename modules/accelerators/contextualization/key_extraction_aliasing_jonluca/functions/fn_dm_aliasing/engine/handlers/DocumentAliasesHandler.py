"""Document aliases transformer handler."""

import re
from typing import Any, Dict, Set

from .AliasTransformerHandler import AliasTransformerHandler


class DocumentAliasesHandler(AliasTransformerHandler):
    """Handles document-specific aliasing."""

    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """
        Generate document name variants.

        Example config:
        {
            "pid_rules": {
                "remove_ampersand": True,
                "add_spaces": True,
                "revision_variants": True
            },
            "drawing_rules": {
                "zero_padding": {"enabled": True, "target_length": 6},
                "sheet_variants": True
            },
            "file_rules": {
                "remove_revision_numbers": True
            }
        }
        """
        new_aliases = set()
        pid_rules = config.get("pid_rules", {})
        drawing_rules = config.get("drawing_rules", {})
        file_rules = config.get("file_rules", {})

        for alias in aliases:
            variants = {alias}

            # Apply P&ID rules
            if pid_rules.get("remove_ampersand"):
                variants.add(alias.replace("P&ID", "PID"))

            if pid_rules.get("add_spaces"):
                # Convert hyphens to spaces
                variants.add(alias.replace("-", " "))

            if pid_rules.get("revision_variants"):
                # Remove revision suffixes
                rev_pattern = r"[-_]Rev[-_]?[A-Z0-9]+"
                variants.add(re.sub(rev_pattern, "", alias))

            # Apply drawing rules
            zero_padding = drawing_rules.get("zero_padding", {})
            if zero_padding.get("enabled"):
                target_length = zero_padding.get("target_length", 6)

                # Find numbers and pad them
                def pad_numbers(match):
                    number = match.group(0)
                    return number.zfill(target_length)

                padded = re.sub(r"\d+", pad_numbers, alias)
                variants.add(padded)

            if drawing_rules.get("sheet_variants"):
                # Generate sheet variants
                if re.search(r"-\d+$", alias):
                    sheet_match = re.search(r"(.+)-(\d+)$", alias)
                    if sheet_match:
                        base, sheet = sheet_match.groups()
                        variants.add(f"{base}-SH-{sheet}")
                        variants.add(f"{base}-Sheet-{sheet}")

            # Apply file rules
            if file_rules.get("remove_revision_numbers"):
                # Remove revision numbers from file names
                # Patterns match: -rev1, -revision2, -r3, _rev1.pdf, etc.
                # Handle both with and without file extensions

                # Check if file has extension
                has_extension = (
                    "." in alias and alias.rfind(".") > alias.rfind("/")
                    if "/" in alias
                    else "." in alias
                )

                if has_extension:
                    # For files with extension: remove revision before extension
                    # Example: "document-rev1.pdf" -> "document.pdf"
                    # Pattern matches: [-_\s](rev|revision|r)[-_\s]*\d+ followed by extension
                    rev_pattern_with_ext = (
                        r"[-_\s]+(?:rev|revision|r)[-_\s]*\d+(?=\.[^.]+$)"
                    )
                    variant = re.sub(
                        rev_pattern_with_ext, "", alias, flags=re.IGNORECASE
                    )
                    if variant != alias:
                        variants.add(variant)
                else:
                    # For files without extension: remove revision at end
                    # Example: "document-rev1" -> "document"
                    rev_pattern_no_ext = (
                        r"[-_\s]+(?:rev|revision|r)[-_\s]*\d+([-_\s].*)?$"
                    )
                    variant = re.sub(rev_pattern_no_ext, "", alias, flags=re.IGNORECASE)
                    if variant != alias:
                        variants.add(variant)

            new_aliases.update(variants)

        return new_aliases
