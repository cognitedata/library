"""Patch v1 scope document dicts for a hierarchy leaf (trigger embedding, tests)."""

from __future__ import annotations

import copy
from typing import Any, Dict, List

from scope_build.context import ScopeBuildContext
from scope_build.naming import cdf_external_id_suffix


def build_scope_block(ctx: ScopeBuildContext) -> Dict[str, Any]:
    leaf = ctx.path[-1]
    return {
        "id": ctx.scope_id,
        "name": leaf.name,
        "description": leaf.description,
        "path": [
            {
                "level": step.level,
                "id": step.segment_id,
                "name": step.name,
                "description": step.description,
            }
            for step in ctx.path
        ],
    }


def _filter_is_node_space(f: Any) -> bool:
    if not isinstance(f, dict):
        return False
    scope = str(f.get("property_scope", "view")).lower()
    return scope == "node" and f.get("target_property") == "space"


def inject_leaf_instance_space_filters(doc: Dict[str, Any], ctx: ScopeBuildContext) -> None:
    """Prepend a node `space` filter on each top-level source_view (unless one already exists)."""
    views = doc.get("source_views")
    if not isinstance(views, list) or not views:
        return

    leaf_node: Dict[str, Any] = ctx.path[-1].node if ctx.path else {}
    inst = leaf_node.get("instance_space") if isinstance(leaf_node, dict) else None
    if isinstance(inst, str) and inst.strip():
        space_value = inst.strip()
    else:
        space_value = "{{instance_space}}"

    for v in views:
        if not isinstance(v, dict):
            continue
        raw_filters = v.get("filters")
        if raw_filters is None:
            flist: List[Any] = []
        elif isinstance(raw_filters, list):
            flist = list(raw_filters)
        else:
            flist = []
        if any(_filter_is_node_space(f) for f in flist):
            continue
        node_filter: Dict[str, Any] = {
            "operator": "EQUALS",
            "property_scope": "node",
            "target_property": "space",
            "values": [space_value],
        }
        v["filters"] = [node_filter] + flist


def _with_suffix(base: str, suffix: str) -> str:
    if base.endswith("_default"):
        stem = base[: -len("_default")]
        return f"{stem}_{suffix}"
    return f"{base}_{suffix}"


def patch_external_ids(doc: Dict[str, Any], suffix: str) -> None:
    ke = doc.get("key_extraction")
    if isinstance(ke, dict) and "externalId" in ke:
        base = str(ke["externalId"])
        ke["externalId"] = _with_suffix(base, suffix)
    al = doc.get("aliasing")
    if isinstance(al, dict) and "externalId" in al:
        base = str(al["externalId"])
        al["externalId"] = _with_suffix(base, suffix)


def prepare_scope_document_for_context(doc: Dict[str, Any], ctx: ScopeBuildContext) -> Dict[str, Any]:
    """Return a deep copy of ``doc`` with leaf patches (external ids, filters, scope block)."""
    out = copy.deepcopy(doc)
    suffix = cdf_external_id_suffix(ctx.scope_id)
    patch_external_ids(out, suffix)
    inject_leaf_instance_space_filters(out, ctx)
    out["scope"] = build_scope_block(ctx)
    ke = out.get("key_extraction")
    if isinstance(ke, dict):
        cfg = ke.get("config")
        if isinstance(cfg, dict):
            params = cfg.get("parameters")
            if isinstance(params, dict):
                params["workflow_scope"] = ctx.scope_id
    return out
