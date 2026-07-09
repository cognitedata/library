"""Handler: trim_whitespace."""

from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


class TrimWhitespaceHandler(AbstractTransformHandler):
    handler_id = "trim_whitespace"
    description = (
        "Trim leading and trailing whitespace from the working string, or collapse internal whitespace "
        "runs to a single space (mode: ends_only or collapse_internal)."
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
        mode = cls.first_nonempty(block.get("mode"), "ends_only")
        if mode == "collapse_internal" or block.get("collapse_internal"):
            return re.sub(r"\s+", " ", working.strip())
        return working.strip()
