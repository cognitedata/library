"""Handler: change_case."""

from __future__ import annotations

from typing import Any, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


class ChangeCaseHandler(AbstractTransformHandler):
    handler_id = "change_case"

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
        case = cls.first_nonempty(block.get("case"), "lower").lower()
        if case == "upper":
            return working.upper()
        if case == "title":
            return working.title()
        return working.lower()
