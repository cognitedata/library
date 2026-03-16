"""Prefix and suffix transformer handler."""

from typing import Any, Dict, Set

from .AliasTransformerHandler import AliasTransformerHandler


class PrefixSuffixHandler(AliasTransformerHandler):
    """Handles prefix and suffix operations."""

    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """
        Apply prefix/suffix operations.

        Example config:
        {
            "operation": "add_prefix",
            "prefix": "PA-",
            "context_mapping": {
                "Plant_A": {"prefix": "PA-"},
                "Plant_B": {"prefix": "PB-"}
            },
            "resolve_from": "site",
            "conditions": {"missing_prefix": True}
        }
        """
        new_aliases = set()
        operation = config.get("operation")

        if operation == "add_prefix":
            prefix = self._resolve_prefix(config, context)
            for alias in aliases:
                # Check if already has prefix
                if config.get("conditions", {}).get("missing_prefix"):
                    if not alias.startswith(prefix):
                        new_aliases.add(f"{prefix}{alias}")
                    else:
                        new_aliases.add(alias)
                else:
                    new_aliases.add(f"{prefix}{alias}")

        elif operation == "remove_prefix":
            prefix = config.get("prefix", "")
            for alias in aliases:
                if alias.startswith(prefix):
                    new_aliases.add(alias[len(prefix) :])
                else:
                    new_aliases.add(alias)

        elif operation == "add_suffix":
            suffix = self._resolve_suffix(config, context)
            for alias in aliases:
                new_aliases.add(f"{alias}{suffix}")

        elif operation == "remove_suffix":
            suffix = config.get("suffix", "")
            for alias in aliases:
                if alias.endswith(suffix):
                    new_aliases.add(alias[: -len(suffix)])
                else:
                    new_aliases.add(alias)

        return new_aliases

    def _resolve_prefix(self, config: Dict[str, Any], context: Dict[str, Any]) -> str:
        """Resolve prefix from configuration and context."""
        context_mapping = config.get("context_mapping")
        if context_mapping and context:
            resolve_from = config.get("resolve_from", "site")
            context_value = context.get(resolve_from)

            # Handle case where context_value might be a dict (extract value)
            if isinstance(context_value, dict):
                # Try common keys that might contain the actual value
                context_value = (
                    context_value.get("name")
                    or context_value.get("value")
                    or context_value.get("id")
                    or str(context_value)
                )
            elif context_value is not None:
                # Convert to string for comparison
                context_value = str(context_value)

            if context_value and context_value in context_mapping:
                return context_mapping[context_value].get("prefix", "")

        return config.get("prefix", "")

    def _resolve_suffix(self, config: Dict[str, Any], context: Dict[str, Any]) -> str:
        """Resolve suffix from configuration and context."""
        context_mapping = config.get("context_mapping")
        if context_mapping and context:
            resolve_from = config.get("resolve_from", "equipment_type")
            context_value = context.get(resolve_from)

            # Handle case where context_value might be a dict (extract value)
            if isinstance(context_value, dict):
                # Try common keys that might contain the actual value
                context_value = (
                    context_value.get("name")
                    or context_value.get("value")
                    or context_value.get("id")
                    or str(context_value)
                )
            elif context_value is not None:
                # Convert to string for comparison
                context_value = str(context_value)

            if context_value and context_value in context_mapping:
                return context_mapping[context_value].get("suffix", "")

        return config.get("suffix", "")
