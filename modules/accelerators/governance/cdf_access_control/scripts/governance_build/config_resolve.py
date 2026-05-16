"""Normalize spaces/groups YAML so both are driven by the same dimension + template model."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Tuple

from governance_build.dimensions import cartesian_list_combos, require_hierarchy
from governance_build.hierarchy import collect_scope_rows, parse_levels


def _strip_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _as_str_list(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        return []
    return [str(x).strip() for x in raw if str(x).strip()]


def resolve_scope_dimension(block: Mapping[str, Any], *, legacy_from_dimension: bool) -> str:
    sd = _strip_str(block.get("scope_dimension"))
    if sd:
        return sd
    if legacy_from_dimension:
        return _strip_str(block.get("from_dimension"))
    return ""


def merge_groups_build_config(groups: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(groups, Mapping):
        raise ValueError("groups must be a mapping")
    g = dict(groups)
    expansion = g.get("expansion")
    expansion = expansion if isinstance(expansion, dict) else {}
    glob = g.get("global")
    glob = glob if isinstance(glob, dict) else {}

    scope_dimension = _strip_str(g.get("scope_dimension")) or _strip_str(
        expansion.get("scope_dimension")
    )
    combine_with = _as_str_list(g.get("combine_with")) or _as_str_list(
        expansion.get("combine_with")
    )
    nodes = _strip_str(g.get("nodes")) or _strip_str(expansion.get("nodes")) or "leaves"
    template = _strip_str(g.get("template")) or _strip_str(glob.get("template")) or _strip_str(
        expansion.get("template")
    )
    output_dir = _strip_str(g.get("output_dir")) or "auth"
    name_template = (
        _strip_str(g.get("name_template"))
        or _strip_str(glob.get("name_template"))
        or _strip_str(expansion.get("name_template"))
    )
    return {
        "scope_dimension": scope_dimension,
        "combine_with": combine_with,
        "nodes": nodes,
        "template": template,
        "output_dir": output_dir,
        "name_template": name_template,
        "global": glob,
    }


def merge_spaces_build_config(spaces: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(spaces, Mapping):
        raise ValueError("spaces must be a mapping")
    s = dict(spaces)
    expansion = s.get("expansion")
    expansion = expansion if isinstance(expansion, dict) else {}
    glob = s.get("global")
    glob = glob if isinstance(glob, dict) else {}

    scope_dimension = _strip_str(s.get("scope_dimension")) or _strip_str(
        expansion.get("scope_dimension")
    )
    if not scope_dimension:
        scope_dimension = resolve_scope_dimension(s, legacy_from_dimension=True)
    combine_with = _as_str_list(s.get("combine_with")) or _as_str_list(
        expansion.get("combine_with")
    )
    nodes = _strip_str(s.get("nodes")) or _strip_str(expansion.get("nodes")) or "leaves"
    template = _strip_str(s.get("template")) or _strip_str(glob.get("template"))
    output_dir = _strip_str(s.get("output_dir")) or "spaces"
    instance_space_id_template = (
        _strip_str(s.get("instance_space_id_template"))
        or _strip_str(glob.get("instance_space_id_template"))
        or _strip_str(expansion.get("instance_space_id_template"))
    )
    name_template = (
        _strip_str(s.get("name_template"))
        or _strip_str(glob.get("name_template"))
        or _strip_str(expansion.get("name_template"))
    )
    return {
        "scope_dimension": scope_dimension,
        "combine_with": combine_with,
        "nodes": nodes,
        "template": template,
        "output_dir": output_dir,
        "instance_space_id_template": instance_space_id_template or None,
        "name_template": name_template or None,
        "global": glob,
    }


def shared_hierarchy_job(
    dimensions: Mapping[str, Any],
    *,
    scope_dimension: str,
    combine_with: List[str],
    nodes_mode: str,
) -> Tuple[List[str], list, list]:
    hblock = require_hierarchy(dimensions, scope_dimension)
    levels = parse_levels(hblock)
    rows = collect_scope_rows(hblock, nodes_mode=nodes_mode)
    combos = list(cartesian_list_combos(dimensions, combine_with))
    if not combos:
        combos = [{}]
    return levels, rows, combos
