"""Handler: parse_json_extract."""

from __future__ import annotations

import json
from typing import Any, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


def _json_path_get(obj: Any, path: str) -> Any:
    cur = obj
    normalized = path.strip().lstrip("$").lstrip(".")
    if not normalized:
        return cur
    for part in normalized.split("."):
        if not part:
            continue
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list) and part.isdigit():
            idx = int(part)
            cur = cur[idx] if 0 <= idx < len(cur) else None
        else:
            return None
    return cur


class ParseJsonExtractHandler(AbstractTransformHandler):
    handler_id = "parse_json_extract"

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
        path = cls.first_nonempty(block.get("path"), block.get("json_path"))
        if not path:
            raise ValueError("parse_json_extract requires path or json_path")
        try:
            parsed = json.loads(working)
        except json.JSONDecodeError as ex:
            if block.get("strict"):
                raise ValueError(f"parse_json_extract: invalid JSON: {ex}") from ex
            return ""
        val = _json_path_get(parsed, path)
        if val is None:
            return ""
        if isinstance(val, (dict, list)):
            return json.dumps(val, sort_keys=True)
        return str(val)
