"""Handler: default_if_empty."""

from __future__ import annotations

from typing import Any, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


class DefaultIfEmptyHandler(AbstractTransformHandler):
    handler_id = "default_if_empty"

    @classmethod
    def apply(
        cls,
        working: str,
        block: Mapping[str, Any],
        *,
        field_values: Optional[Mapping[str, str]] = None,
        props: Optional[Mapping[str, Any]] = None,
    ) -> TransformResult:
        del field_values
        if not cls.is_blank(working):
            return working
        literal = block.get("literal")
        if literal is not None and str(literal).strip():
            return str(literal)
        field = cls.first_nonempty(block.get("field"), block.get("fallback_field"))
        if field and props is not None:
            val = props.get(field)
            if val is not None and not (isinstance(val, str) and cls.is_blank(val)):
                return str(val)
        return str(block.get("default") or "")
