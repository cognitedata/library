"""Instance alias reads and self-reference guards."""

from __future__ import annotations

from typing import Any

from inverted_index.extract import read_property_path
from inverted_index.normalize import normalize_term


def read_instance_aliases(instance: dict[str, Any]) -> list[str]:
    """Return alias strings from a DM instance dict."""
    props = instance.get("properties")
    if not isinstance(props, dict):
        props = instance if isinstance(instance, dict) else {}

    aliases = read_property_path(instance, "aliases")
    if aliases is None:
        aliases = props.get("aliases") or props.get("tags")
    if aliases is None:
        return []
    if isinstance(aliases, str):
        return [aliases] if aliases.strip() else []
    return [str(a) for a in aliases if a]


def normalized_instance_aliases(instance: dict[str, Any]) -> set[str]:
    """Normalized alias terms for the instance (for self-reference checks)."""
    return {
        normalized
        for alias in read_instance_aliases(instance)
        if (normalized := normalize_term(alias))
    }


def is_alias_term(term: str, instance: dict[str, Any]) -> bool:
    """True when ``term`` normalizes to one of the instance aliases."""
    normalized = normalize_term(term)
    return bool(normalized) and normalized in normalized_instance_aliases(instance)


def is_self_reference_hit(
    hit: dict[str, Any],
    instance_external_id: str,
    instance_space: str | None = None,
) -> bool:
    """True when an index hit points at the same instance being contextualized."""
    ref = hit.get("reference_external_id")
    if ref is None or str(ref) != str(instance_external_id):
        return False
    ref_space = hit.get("reference_space")
    if instance_space and ref_space and str(ref_space) != str(instance_space):
        return False
    return True
