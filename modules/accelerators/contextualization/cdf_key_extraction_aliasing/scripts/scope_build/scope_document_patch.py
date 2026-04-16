"""Patch v1 scope document dicts for a hierarchy leaf (trigger embedding, tests)."""

from __future__ import annotations

import copy
from typing import Any, Dict, List

from scope_build.context import ScopeBuildContext
from scope_build.naming import cdf_external_id_suffix
from scope_build.scope_document_limits import assert_scope_document_within_limit


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


def strip_node_space_filters_from_top_level_source_views(doc: Dict[str, Any]) -> None:
    """Remove node ``space`` filters from each top-level ``source_views`` entry (mutates ``doc``).

    Used when copying a scope document from another leaf so
    :func:`inject_leaf_instance_space_filters` can prepend filters for the destination leaf.
    """
    views = doc.get("source_views")
    if not isinstance(views, list) or not views:
        return
    for v in views:
        if not isinstance(v, dict):
            continue
        raw_filters = v.get("filters")
        if not isinstance(raw_filters, list) or not raw_filters:
            continue
        kept = [f for f in raw_filters if not _filter_is_node_space(f)]
        if len(kept) != len(raw_filters):
            v["filters"] = kept


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


def _set_workflow_scope_parameter(doc: Dict[str, Any], ctx: ScopeBuildContext) -> None:
    ke = doc.get("key_extraction")
    if isinstance(ke, dict):
        cfg = ke.get("config")
        if isinstance(cfg, dict):
            params = cfg.get("parameters")
            if isinstance(params, dict):
                params["workflow_scope"] = ctx.scope_id


def prepare_scope_document_for_context(doc: Dict[str, Any], ctx: ScopeBuildContext) -> Dict[str, Any]:
    """Return a deep copy of ``doc`` with leaf patches (external ids, filters, scope block)."""
    out = copy.deepcopy(doc)
    suffix = cdf_external_id_suffix(ctx.scope_id)
    patch_external_ids(out, suffix)
    inject_leaf_instance_space_filters(out, ctx)
    out["scope"] = build_scope_block(ctx)
    _set_workflow_scope_parameter(out, ctx)
    return out


def reconcile_scope_document_for_destination(
    source_doc: Dict[str, Any],
    dest_ctx: ScopeBuildContext,
) -> Dict[str, Any]:
    """Deep-copy ``source_doc`` and apply destination leaf patches (copy workflow input between scopes).

    Same destination-specific fields as :func:`prepare_scope_document_for_context`, but starts from an
    arbitrary source document: strips existing node ``space`` filters first so instance-space injection
    matches the destination leaf.
    """
    out = copy.deepcopy(source_doc)
    suffix = cdf_external_id_suffix(dest_ctx.scope_id)
    patch_external_ids(out, suffix)
    strip_node_space_filters_from_top_level_source_views(out)
    inject_leaf_instance_space_filters(out, dest_ctx)
    out["scope"] = build_scope_block(dest_ctx)
    _set_workflow_scope_parameter(out, dest_ctx)
    assert_scope_document_within_limit(out, scope_id=dest_ctx.scope_id)
    return out
