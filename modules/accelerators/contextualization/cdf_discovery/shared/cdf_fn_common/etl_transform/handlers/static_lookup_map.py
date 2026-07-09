"""Handler: static_lookup_map."""

from __future__ import annotations

from typing import Any, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


class StaticLookupMapHandler(AbstractTransformHandler):
    handler_id = "static_lookup_map"
    description = (
        "Map the working string through an inline key→value table (map) or ordered prefix pairs. "
        "Unmatched values pass through unchanged unless configured otherwise."
    )

    @classmethod
    def apply(
        cls,
        working: str,
        block: Mapping[str, Any],
        *,
        field_values: Optional[Mapping[str, str]] = None,
        props: Optional[Mapping[str, Any]] = None,
    ) -> TransformResult:
        del field_values, props
        mapping = block.get("map")
        if isinstance(mapping, dict):
            if working in mapping:
                return str(mapping[working])
            for key, val in mapping.items():
                if working.startswith(str(key)):
                    return str(val)
        pairs = block.get("pairs")
        if isinstance(pairs, list):
            best_key = ""
            best_val = ""
            for item in pairs:
                if not isinstance(item, dict):
                    continue
                key = str(item.get("key") or item.get("from") or "")
                if key and working.startswith(key) and len(key) >= len(best_key):
                    best_key = key
                    best_val = str(item.get("value") or item.get("to") or "")
            if best_key:
                return best_val
        default = block.get("default")
        return str(default) if default is not None else working
