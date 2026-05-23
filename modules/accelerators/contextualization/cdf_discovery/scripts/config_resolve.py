"""Normalize spaces/groups YAML from scope_hierarchy + list dimensions."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Tuple

from governance_build.dimensions import cartesian_list_combos, get_scope_hierarchy
from governance_build.hierarchy import collect_scope_rows, parse_levels


def _strip_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _as_str_list(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        return []
    return [str(x).strip() for x in raw if str(x).strip()]


def merge_groups_build_config(groups: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(groups, Mapping):
        raise ValueError("groups must be a mapping")
    g = dict(groups)
    expansion = g.get("expansion")
    expansion = expansion if isinstance(expansion, dict) else {}
    glob = g.get("global")
    glob = glob if isinstance(glob, dict) else {}

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
        "combine_with": combine_with,
        "nodes": nodes,
        "template": template,
        "output_dir": output_dir,
        "instance_space_id_template": instance_space_id_template or None,
        "name_template": name_template or None,
        "global": glob,
    }


def shared_hierarchy_job(
    doc: Mapping[str, Any],
    *,
    combine_with: List[str],
    nodes_mode: str,
) -> Tuple[List[str], list, list]:
    hblock = get_scope_hierarchy(doc)
    levels = parse_levels(hblock)
    rows = collect_scope_rows(hblock, nodes_mode=nodes_mode)
    dimensions = doc.get("dimensions")
    if not isinstance(dimensions, dict):
        dimensions = {}
    combos = list(cartesian_list_combos(dimensions, combine_with))
    if not combos:
        combos = [{}]
    return levels, rows, combos
