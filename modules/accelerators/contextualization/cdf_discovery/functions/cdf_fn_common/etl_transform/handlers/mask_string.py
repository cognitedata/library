"""Handler: mask_string."""

from __future__ import annotations

from typing import Any, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


class MaskStringHandler(AbstractTransformHandler):
    handler_id = "mask_string"
    description = (
        "Mask the working string while keeping the last keep_last characters visible (mask_char for "
        "hidden positions). Use for PII redaction in preview or downstream properties."
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
        keep_last = max(0, int(block.get("keep_last") or 4))
        mask_char = str(block.get("mask_char") or "*")[:1] or "*"
        if len(working) <= keep_last:
            return mask_char * len(working)
        return mask_char * (len(working) - keep_last) + working[-keep_last:]
