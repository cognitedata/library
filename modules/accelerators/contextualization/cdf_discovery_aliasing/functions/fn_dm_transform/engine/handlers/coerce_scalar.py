"""Handler: coerce_scalar."""

from __future__ import annotations

from typing import Any, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


class CoerceScalarHandler(AbstractTransformHandler):
    handler_id = "coerce_scalar"

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
        target = cls.first_nonempty(block.get("type"), "int").lower()
        strict = bool(block.get("strict"))
        empty_as_null = block.get("empty_as_null", True) is not False
        text = working.strip()
        if not text:
            if empty_as_null:
                return None
            text = "0" if target in ("int", "float") else "false"
        try:
            if target == "int":
                return int(text, 10)
            if target == "float":
                return float(text)
            if target == "bool":
                low = text.lower()
                if low in ("true", "1", "yes", "y"):
                    return True
                if low in ("false", "0", "no", "n"):
                    return False
                raise ValueError(f"not a bool: {text!r}")
            return text
        except (TypeError, ValueError) as ex:
            if strict:
                raise ValueError(f"coerce_scalar strict failed for {text!r}: {ex}") from ex
            return None
