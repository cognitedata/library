"""Handler: split_join — split working string, reassemble selected token indexes via template."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Sequence

from ..field_template import apply_output_template
from .base import AbstractTransformHandler, TransformResult
from .split_parts import split_working_parts


def _normalize_indexes(raw: Any) -> List[Any]:
    """Accept indexes as list, scalar, or comma-separated string (UI / YAML)."""
    if raw is None:
        return []
    if isinstance(raw, bool):
        return []
    if isinstance(raw, (int, float)):
        return [int(raw)]
    if isinstance(raw, str):
        parts = [p.strip() for p in re.split(r"[,;]+", raw) if p.strip()]
        out: List[Any] = []
        for p in parts:
            try:
                out.append(int(p))
            except ValueError:
                continue
        return out
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        return list(raw)
    return []


def validate_split_join_block(block: Mapping[str, Any]) -> None:
    template = AbstractTransformHandler.first_nonempty(block.get("template"))
    indexes = _normalize_indexes(block.get("indexes"))
    if template:
        return
    if indexes:
        return
    raise ValueError("split_join: provide template and/or non-empty indexes[]")


def _resolve_part_index(raw: Any, n: int) -> Optional[int]:
    try:
        i = int(raw)
    except (TypeError, ValueError):
        return None
    if i < 0:
        i += n
    if 0 <= i < n:
        return i
    return None


def _token_field_values(parts: List[str], block: Mapping[str, Any]) -> Dict[str, str]:
    n = len(parts)
    out: Dict[str, str] = {}
    for i, p in enumerate(parts):
        out[str(i)] = p
        out[str(i - n)] = p
    labels = block.get("labels")
    if isinstance(labels, dict):
        for key, raw_idx in labels.items():
            name = str(key).strip()
            if not name:
                continue
            idx = _resolve_part_index(raw_idx, len(parts))
            if idx is not None:
                out[name] = parts[idx]
    return out


class SplitJoinHandler(AbstractTransformHandler):
    handler_id = "split_join"

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
        parts = split_working_parts(working, block)
        token_values = _token_field_values(parts, block)

        template = cls.first_nonempty(block.get("template"))
        indexes = _normalize_indexes(block.get("indexes"))
        if template and not indexes:
            return apply_output_template(template, token_values)

        if indexes:
            join = str(block.get("join") if block.get("join") is not None else "")
            selected: List[str] = []
            for raw_i in indexes:
                idx = _resolve_part_index(raw_i, len(parts))
                if idx is not None:
                    selected.append(parts[idx])
            return join.join(selected)

        if template:
            return apply_output_template(template, token_values)

        return working
