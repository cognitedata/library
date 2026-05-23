"""Resolve instance space external ids from optional Jinja templates or defaults."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

from governance_build.context import scope_id_to_snake
from governance_build.dimensions_registry import (
    apply_dimension_aliases,
    data_type_from_combo_or_scalar,
    resolve_source_dimension_key,
)
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


def enrich_spaces_naming_context(
    ctx: Dict[str, Any],
    *,
    data_type: str,
    combine_names: Sequence[str],
    combo: Mapping[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Naming aliases: ``data_type`` / ``source`` / ``scope`` for space templates."""
    names = list(combine_names)
    dt = data_type_from_combo_or_scalar(combo, names, data_type or "dm")
    out = dict(ctx)
    out["data_type"] = dt
    out["data_type_id"] = scope_id_to_snake(dt)
    out["scope"] = str(out.get("scope_id_snake") or scope_id_to_snake(str(out.get("scope_id", ""))))
    src_key = next((k for k in names if k in ("source", "source_system")), None)
    if not src_key:
        src_key = resolve_source_dimension_key({})
    out["source"] = str(out.get(f"{src_key}_id") or (combo.get(src_key) or {}).get("id", ""))
    out["source_name"] = str(
        out.get(f"{src_key}_name") or (combo.get(src_key) or {}).get("name", "")
    )
    return apply_dimension_aliases(out, list(combine_names))


def default_instance_space_external_id(
    scope_id: str,
    combo: Mapping[str, Dict[str, Any]],
    combine_names: Sequence[str],
    *,
    data_type: str = "dm",
) -> str:
    """``inst_{data_type}_{source}_{scope}`` with snake_case segments (CDF external-id style)."""
    scope_part = scope_id_to_snake(scope_id)
    dt = scope_id_to_snake((data_type or "dm").strip() or "dm")
    if not combine_names:
        return f"inst_{dt}_{scope_part}"
    src_key = combine_names[0]
    it = combo.get(src_key) or {}
    src_part = scope_id_to_snake(str(it.get("id", "x")).strip() or "x")
    return f"inst_{dt}_{src_part}_{scope_part}"


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
    data_type: str = "dm",
) -> str:
    if template and str(template).strip():
        rendered = render_template_string(str(template), dict(ctx))
        return _ensure_inst_prefix(rendered)
    return default_instance_space_external_id(
        scope_id, combo, combine_names, data_type=data_type
    )
