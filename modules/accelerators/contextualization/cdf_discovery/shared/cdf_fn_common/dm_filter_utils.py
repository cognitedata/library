"""Helpers for building data modeling filter property references from config."""

from __future__ import annotations

from typing import Any, Union

PropertyRef = Union[tuple[str, str], list[str]]


def property_reference_for_filter(
    view_id: Any,
    target_property: str,
    property_scope: str = "view",
) -> PropertyRef:
    """Return a DM property reference for ``Equals`` / ``In`` / ``Exists`` / etc.

    ``property_scope`` is ``view`` (default) for view properties, or ``node`` for
    node metadata such as ``space`` or ``externalId`` (``("node", "space")``).
    """
    if str(property_scope or "view").lower() == "node":
        return ("node", target_property)
    return view_id.as_property_ref(target_property)
