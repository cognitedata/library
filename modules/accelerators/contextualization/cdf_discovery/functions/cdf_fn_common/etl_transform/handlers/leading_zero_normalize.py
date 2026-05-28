"""Handler: leading_zero_normalize."""

from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


class LeadingZeroNormalizeHandler(AbstractTransformHandler):
    handler_id = "leading_zero_normalize"
    description = (
        "Strip leading zeros from digit runs in the working string. Optionally restrict matches with "
        "segment_regex and pad normalized segments to minimum_width. Useful for unit prefixes and "
        "instrument tag normalization."
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
        min_width = int(block.get('minimum_width') or 0)

        def _norm_run(match: re.Match[str]) -> str:
            digits = match.group(0)
            stripped = digits.lstrip('0') or '0'
            if min_width > 0 and len(stripped) < min_width:
                stripped = stripped.zfill(min_width)
            return stripped

        segment_re = cls.first_nonempty(block.get('segment_regex'))
        if segment_re:
            return re.sub(segment_re, _norm_run, working)
        return re.sub(r'\d+', _norm_run, working)
