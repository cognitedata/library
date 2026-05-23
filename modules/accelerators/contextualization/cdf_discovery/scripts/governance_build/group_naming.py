"""Resolve CDF access group names (``gp_*``) from CDF naming conventions."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

from governance_build.context import scope_id_to_snake
from governance_build.dimensions_registry import (
    apply_dimension_aliases,
    data_type_from_combo_or_scalar,
    first_hierarchy_level_id,
)
from governance_build.render import render_template_string


def enrich_groups_naming_context(
    ctx: Dict[str, Any],
    *,
    data_type: str,
    levels: Sequence[str],
    combine_names: Sequence[str],
    combo: Mapping[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Naming aliases: ``data_type``, ``location``, ``access_type`` for group templates."""
    names = list(combine_names)
    out = dict(ctx)
    for key in names:
        if key in combo:
            out.setdefault(f"{key}_id", str((combo.get(key) or {}).get("id", "")))
            out.setdefault(f"{key}_name", str((combo.get(key) or {}).get("name", "")))
    out = apply_dimension_aliases(out, names)
    dt = data_type_from_combo_or_scalar(combo, names, data_type or "asset")
    out["data_type"] = dt
    out["data_type_id"] = scope_id_to_snake(dt)
    loc_raw = first_hierarchy_level_id(out, list(levels)) or str(out.get("scope_id", ""))
    loc_snake = scope_id_to_snake(loc_raw) if loc_raw else str(out.get("scope_id_snake", ""))
    out["location_id"] = loc_snake
    out["location"] = loc_snake
    if not out.get("access_type_id"):
        for key in ("access_type", "access_level"):
            if key in combo:
                out["access_type_id"] = scope_id_to_snake(str((combo.get(key) or {}).get("id", "")))
                break
    return out


def default_group_name(
    *,
    data_type: str,
    location_id: str,
    access_type_id: str,
) -> str:
    """``gp_{data_type}_{location}_{access_type}`` (CDF authorization group pattern)."""
    dt = scope_id_to_snake((data_type or "asset").strip() or "asset")
    loc = scope_id_to_snake(location_id.strip()) if location_id.strip() else "all"
    access = scope_id_to_snake(access_type_id.strip()) if access_type_id.strip() else "read"
    return f"gp_{dt}_{loc}_{access}"


def resolve_group_name(
    *,
    template: Optional[str],
    ctx: Mapping[str, Any],
    data_type: str,
    levels: Sequence[str],
    combine_names: Sequence[str],
    combo: Mapping[str, Dict[str, Any]],
) -> str:
    enriched = enrich_groups_naming_context(
        dict(ctx),
        data_type=data_type,
        levels=levels,
        combine_names=combine_names,
        combo=combo,
    )
    if template and str(template).strip():
        return render_template_string(str(template).strip(), enriched).strip()
    access = str(enriched.get("access_type_id") or enriched.get("access_level_id") or "read")
    return default_group_name(
        data_type=data_type,
        location_id=str(enriched.get("location_id", "")),
        access_type_id=access,
    )
