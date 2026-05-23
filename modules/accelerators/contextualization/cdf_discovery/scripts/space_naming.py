"""Resolve instance space external ids from optional Jinja templates or defaults."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

from governance_build.context import scope_id_to_snake
from governance_build.render import render_template_string


def merge_list_combo_into_context(
    flat_scope: Mapping[str, Any], combo: Mapping[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """Add ``{dim}_id`` / ``{dim}_name`` and ``scope_snake`` alias for templates."""
    ctx = dict(flat_scope)
    sid = ctx.get("scope_id_snake")
    if isinstance(sid, str):
        ctx.setdefault("scope_snake", sid)
    for name, item in combo.items():
        ctx[f"{name}_id"] = str(item.get("id", ""))
        ctx[f"{name}_name"] = str(item.get("name", ""))
    return ctx


def default_instance_space_external_id(
    scope_id: str, combo: Mapping[str, Dict[str, Any]], combine_names: Sequence[str]
) -> str:
    """``inst_`` + list-dimension ids (in ``combine_names`` order) + ``scope_id_snake``."""
    scope_part = scope_id_to_snake(scope_id)
    if not combine_names:
        return f"inst_{scope_part}"
    parts: List[str] = []
    for name in combine_names:
        it = combo.get(name) or {}
        raw_id = str(it.get("id", "x")).strip() or "x"
        parts.append(scope_id_to_snake(raw_id))
    body = "_".join([*parts, scope_part])
    return f"inst_{body}"


def _ensure_inst_prefix(rendered: str) -> str:
    s = rendered.strip()
    if s.startswith("inst_"):
        return s
    body = s.replace("inst_", "", 1) if s else s
    return f"inst_{body}"


def resolve_instance_space_external_id(
    *,
    template: Optional[str],
    ctx: Mapping[str, Any],
    scope_id: str,
    combo: Mapping[str, Dict[str, Any]],
    combine_names: Sequence[str],
) -> str:
    if template and str(template).strip():
        rendered = render_template_string(str(template), dict(ctx))
        return _ensure_inst_prefix(rendered)
    return default_instance_space_external_id(scope_id, combo, combine_names)
