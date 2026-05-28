"""Handler: sequential_literal_replace."""

from __future__ import annotations

from typing import Any, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


class SequentialLiteralReplaceHandler(AbstractTransformHandler):
    handler_id = "sequential_literal_replace"
    description = (
        "Apply ordered literal find → replace steps on the working string. Each replacement runs "
        "globally before the next step. Use when regex is unnecessary and you need predictable "
        "string substitutions."
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
        result = working
        replacements = block.get('replacements') or []
        if not isinstance(replacements, list):
            return result
        for item in replacements:
            if not isinstance(item, dict):
                continue
            src = str(item.get('from') or '')
            dst = str(item.get('to') or '')
            if src:
                result = result.replace(src, dst)
        return result
