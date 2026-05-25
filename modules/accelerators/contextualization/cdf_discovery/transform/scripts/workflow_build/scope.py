"""Patch workflow definition documents per scope hierarchy leaf."""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Mapping, MutableMapping

from workflow_build.hierarchy_walker import cdf_external_id_suffix


def build_scope_block(scope_id: str, node_chain: List[Dict[str, Any]], *, levels: List[str]) -> Dict[str, Any]:
    leaf = node_chain[-1] if node_chain else {}
    path: List[Dict[str, Any]] = []
    for depth, node in enumerate(node_chain):
        level = levels[depth] if depth < len(levels) else f"level_{depth + 1}"
        seg = str(node.get("id") or node.get("name") or "").strip()
        path.append(
            {
                "level": level,
                "id": seg,
                "name": str(node.get("name") or seg),
                "description": str(node.get("description") or ""),
            }
        )
    return {
        "id": scope_id,
        "name": str(leaf.get("name") or scope_id),
        "description": str(leaf.get("description") or ""),
        "path": path,
    }


def patch_definition_for_scope(
    doc: Mapping[str, Any],
    *,
    scope_id: str,
    node_chain: List[Dict[str, Any]],
    levels: List[str],
) -> Dict[str, Any]:
    out = copy.deepcopy(dict(doc))
    suffix = cdf_external_id_suffix(scope_id)
    scope = build_scope_block(scope_id, node_chain, levels=levels)
    out["scope"] = scope
    params = out.get("parameters")
    if not isinstance(params, dict):
        params = {}
        out["parameters"] = params
    params["workflow_scope"] = suffix
    raw_db = str(params.get("raw_db") or "etl_staging").strip()
    if not raw_db.endswith(f"_{suffix}"):
        params["raw_db"] = f"{raw_db}_{suffix}" if raw_db != "etl_staging" else f"etl_staging_{suffix}"
    inst = params.get("instance_space")
    if isinstance(inst, str) and inst.strip() and not inst.endswith(f"_{suffix}"):
        params["instance_space"] = f"{inst.strip()}_{suffix}"
    return out
