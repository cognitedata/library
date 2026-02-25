"""Case transformation transformer handler."""

from typing import Any, Dict, Set

from .AliasTransformerHandler import AliasTransformerHandler


class CaseTransformationHandler(AliasTransformerHandler):
    """Handles case transformation operations."""

    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """
        Apply case transformations.

        Example config:
        {
            "operations": ["upper", "lower", "title"]
        }
        or
        {
            "operation": "lower"  # single operation
        }
        """
        new_aliases = set()
        # Support both singular and plural forms
        if "operation" in config:
            operations = [config["operation"]]
        else:
            operations = config.get("operations", ["upper"])

        for alias in aliases:
            for operation in operations:
                if operation == "upper":
                    new_aliases.add(alias.upper())
                elif operation == "lower":
                    new_aliases.add(alias.lower())
                elif operation == "title":
                    new_aliases.add(alias.title())
                elif operation == "preserve":
                    new_aliases.add(alias)

        return new_aliases
