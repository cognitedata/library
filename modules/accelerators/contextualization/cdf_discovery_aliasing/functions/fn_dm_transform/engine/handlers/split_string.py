"""Handler: split_string."""

from __future__ import annotations

from typing import Any, List, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


class SplitStringHandler(AbstractTransformHandler):
    handler_id = "split_string"
    multi_value = True

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
        delimiter = str(block.get("delimiter") if block.get("delimiter") is not None else ",")
        max_splits = int(block.get("max_splits") if block.get("max_splits") is not None else -1)
        trim_parts = block.get("trim", True) is not False
        parts = working.split(delimiter, max_splits) if max_splits >= 0 else working.split(delimiter)
        if trim_parts:
            parts = [p.strip() for p in parts]
        return parts
