"""Coerce transform handler output to config.output_field_type before writing properties."""

from __future__ import annotations

import json
from typing import Any, Mapping

OUTPUT_FIELD_TYPES = frozenset(
    {"auto", "string", "int", "float", "bool", "list", "object", "json"}
)


def validate_output_field_type(cfg: Mapping[str, Any]) -> None:
    raw = cfg.get("output_field_type")
    if raw is None or raw == "" or str(raw).strip().lower() == "auto":
        return
    t = str(raw).strip().lower()
    if t not in OUTPUT_FIELD_TYPES:
        raise ValueError(
            f"output_field_type must be one of {sorted(OUTPUT_FIELD_TYPES)}; got {raw!r}"
        )


def coerce_transform_output(value: Any, output_field_type: str) -> Any:
    t = str(output_field_type or "auto").strip().lower() or "auto"
    if t == "auto":
        return value
    if t == "string":
        return "" if value is None else str(value)
    if t == "int":
        if isinstance(value, bool):
            raise ValueError("cannot coerce bool to int for output_field_type=int")
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        s = str(value).strip()
        if not s:
            raise ValueError("empty string cannot coerce to int")
        return int(s, 10)
    if t == "float":
        if isinstance(value, bool):
            raise ValueError("cannot coerce bool to float for output_field_type=float")
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).strip()
        if not s:
            raise ValueError("empty string cannot coerce to float")
        return float(s)
    if t == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        s = str(value).strip().lower()
        if s in ("true", "1", "yes", "y"):
            return True
        if s in ("false", "0", "no", "n", ""):
            return False
        raise ValueError(f"cannot coerce {value!r} to bool")
    if t == "list":
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
    if t == "object":
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        raise ValueError(f"output_field_type=object requires dict, got {type(value).__name__}")
    if t == "json":
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return value
        if not isinstance(value, str):
            value = str(value)
        return json.loads(value)
    raise ValueError(f"unknown output_field_type {t!r}")
