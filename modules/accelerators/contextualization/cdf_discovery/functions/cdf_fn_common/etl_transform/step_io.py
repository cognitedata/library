"""Transform step input/output field validation (canvas compile + runtime config)."""

from __future__ import annotations

from typing import Any, List, Mapping


def transform_source_field_names(fields: Any) -> List[str]:
    if not isinstance(fields, list):
        return []
    names: List[str] = []
    for row in fields:
        if not isinstance(row, dict):
            continue
        name = str(row.get("field_name") or row.get("name") or "").strip()
        if name:
            names.append(name)
    return names


def validate_transform_step_io(step: Mapping[str, Any], *, context: str) -> None:
    """Require ``output_field`` and at least one source ``fields[].field_name`` when enabled."""
    if step.get("enabled") is False:
        return
    if not str(step.get("output_field") or "").strip():
        raise ValueError(f"{context}: output_field is required")
    if not transform_source_field_names(step.get("fields")):
        raise ValueError(f"{context}: at least one fields[].field_name is required")
