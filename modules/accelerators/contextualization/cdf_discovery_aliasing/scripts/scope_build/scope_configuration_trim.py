"""Trim unified scope documents for ``workflow.input.configuration`` (deploy payload)."""

from __future__ import annotations

import copy
from typing import Any, Dict, Mapping, MutableMapping, Set, Tuple

from functions.cdf_fn_common.scope_document_dm import strip_legacy_rule_lists_from_scope_document
from functions.cdf_fn_common.source_view_index import coerce_association_source_view_index
from functions.cdf_fn_common.workflow_compile.canvas_dag import flatten_subgraphs_in_canvas

# Top-level scope lists unused at runtime (discovery config lives on canvas ``node.data``); cleared for deploy.
_DISCOVERY_SCOPE_LIST_KEYS: Tuple[str, ...] = (
    "view_queries",
    "raw_queries",
    "classic_queries",
    "transforms",
    "validations",
    "view_saves",
    "raw_saves",
    "classic_saves",
)


def _strip_positions_from_node(n: MutableMapping[str, Any]) -> None:
    n.pop("position", None)
    n.pop("selected", None)
    n.pop("dragging", None)
    data = n.get("data")
    if isinstance(data, dict):
        inner = data.get("inner_canvas")
        if isinstance(inner, dict):
            _strip_positions_from_canvas(inner)


def _strip_positions_from_canvas(canvas: MutableMapping[str, Any]) -> None:
    nodes = canvas.get("nodes")
    if isinstance(nodes, list):
        for n in nodes:
            if isinstance(n, dict):
                _strip_positions_from_node(n)


def _clear_discovery_scope_lists_for_deploy(doc: Dict[str, Any]) -> None:
    for lk in _DISCOVERY_SCOPE_LIST_KEYS:
        if lk in doc:
            doc[lk] = []


def _collect_source_view_indices_for_trim(
    doc: Mapping[str, Any], canvas: Mapping[str, Any]
) -> Set[int]:
    """Indices referenced by ``source_view`` canvas nodes."""
    out: Set[int] = set()
    nodes = canvas.get("nodes")
    if isinstance(nodes, list):
        for n in nodes:
            if not isinstance(n, dict):
                continue
            if str(n.get("kind") or "").strip() != "source_view":
                continue
            data = n.get("data")
            if not isinstance(data, dict):
                continue
            ref = data.get("ref")
            if not isinstance(ref, dict):
                continue
            ii = coerce_association_source_view_index(ref.get("source_view_index"))
            if ii is not None:
                out.add(ii)
    return out


def _prune_source_views_and_remap(
    doc: Dict[str, Any], canvas: MutableMapping[str, Any], keep_indices: Set[int]
) -> None:
    """Shrink ``source_views`` to *keep_indices*, remapping canvas ``source_view`` refs."""
    if not keep_indices:
        return
    svs = doc.get("source_views")
    if not isinstance(svs, list) or not svs:
        return
    keep_sorted = sorted(i for i in keep_indices if isinstance(i, int) and 0 <= i < len(svs))
    if not keep_sorted or len(keep_sorted) == len(svs):
        return
    doc["source_views"] = [copy.deepcopy(svs[i]) for i in keep_sorted]
    old_to_new = {old: new for new, old in enumerate(keep_sorted)}
    nodes = canvas.get("nodes")
    if isinstance(nodes, list):
        for n in nodes:
            if not isinstance(n, dict):
                continue
            if str(n.get("kind") or "").strip() != "source_view":
                continue
            data = n.get("data")
            if not isinstance(data, dict):
                continue
            ref = data.get("ref")
            if not isinstance(ref, dict):
                continue
            oi = coerce_association_source_view_index(ref.get("source_view_index"))
            if oi is None:
                continue
            ni = old_to_new.get(oi)
            if ni is not None:
                ref["source_view_index"] = ni


def trim_scope_document_for_trigger_input(scope_document: Mapping[str, Any]) -> Dict[str, Any]:
    """Deep-copy *scope_document*, flatten canvas subgraphs, strip layout, legacy rule lists.

    Used for ``workflow.input.configuration`` only. Omits root ``compiled_workflow`` when present.
    """
    out = copy.deepcopy(dict(scope_document))
    out.pop("compiled_workflow", None)
    canvas = out.get("canvas")
    if not isinstance(canvas, dict):
        strip_legacy_rule_lists_from_scope_document(out)
        return out
    flat = flatten_subgraphs_in_canvas(canvas)
    _strip_positions_from_canvas(flat)
    out["canvas"] = flat
    _clear_discovery_scope_lists_for_deploy(out)
    strip_legacy_rule_lists_from_scope_document(out)
    sv_keep = _collect_source_view_indices_for_trim(out, flat)
    _prune_source_views_and_remap(out, flat, sv_keep)
    return out
