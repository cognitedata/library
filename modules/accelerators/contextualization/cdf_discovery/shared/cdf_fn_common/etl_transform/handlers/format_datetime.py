"""Handler: format_datetime."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


class FormatDatetimeHandler(AbstractTransformHandler):
    handler_id = "format_datetime"
    description = (
        "Parse a datetime from the working string (input_format) and format it with output_format "
        "(strftime). Supports UTC normalization when configured."
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
        text = working.strip()
        if not text:
            return ""
        output_format = cls.first_nonempty(block.get("output_format"), "%Y-%m-%dT%H:%M:%SZ")
        input_format = cls.first_nonempty(block.get("input_format"))
        dt: Optional[datetime] = None
        if input_format:
            dt = datetime.strptime(text, input_format)
        else:
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"):
                    try:
                        dt = datetime.strptime(text, fmt)
                        break
                    except ValueError:
                        continue
        if dt is None:
            if block.get("strict"):
                raise ValueError(f"format_datetime: could not parse {text!r}")
            return ""
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime(output_format)
