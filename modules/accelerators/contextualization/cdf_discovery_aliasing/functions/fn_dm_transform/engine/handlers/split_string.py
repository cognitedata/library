"""Handler: split_string."""

from __future__ import annotations

from typing import Any, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult
from .split_parts import split_working_parts


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
        return split_working_parts(working, block)
