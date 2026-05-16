"""Trim unified scope documents for ``workflow.input.configuration`` (deploy payload)."""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Set, Tuple

from functions.cdf_fn_common.scope_document_dm import restrict_aliasing_pathways_to_wanted_names
from functions.cdf_fn_common.workflow_associations import (
    KIND_SOURCE_VIEW_TO_EXTRACTION,
    coerce_association_source_view_index,
)
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


def _rule_id_from_extraction_rule(rule: Mapping[str, Any]) -> Optional[str]:
    rid = rule.get("rule_id") or rule.get("name")
    if rid is None:
        return None
    s = str(rid).strip()
    return s or None


def _name_from_aliasing_rule(rule: Mapping[str, Any]) -> Optional[str]:
    n = rule.get("name")
    if n is None:
        return None
    s = str(n).strip()
    return s or None


def _collect_executable_rule_names_from_flat_canvas(
    canvas: Mapping[str, Any],
) -> Tuple[Set[str], Set[str]]:
    """Return (extraction_rule_names, aliasing_rule_names) referenced by executable canvas nodes."""
    ext: Set[str] = set()
    als: Set[str] = set()
    nodes = canvas.get("nodes")
    if not isinstance(nodes, list):
        return ext, als
    for n in nodes:
        if not isinstance(n, dict):
            continue
        kind = str(n.get("kind") or "").strip()
        data = n.get("data") if isinstance(n.get("data"), dict) else {}
        ref = data.get("ref") if isinstance(data.get("ref"), dict) else {}
        if kind == "extraction":
            rn = ref.get("extraction_rule_name")
            if isinstance(rn, str) and rn.strip():
                ext.add(rn.strip())
        elif kind == "aliasing":
            an = ref.get("aliasing_rule_name")
            if isinstance(an, str) and an.strip():
                als.add(an.strip())
    return ext, als


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


def _prune_key_extraction_rules(doc: Dict[str, Any], keep: Set[str]) -> None:
    if not keep:
        return
    ke = doc.get("key_extraction")
    if not isinstance(ke, dict):
        return
    cfg = ke.get("config")
    if not isinstance(cfg, dict):
        return
    data = cfg.get("data")
    if not isinstance(data, dict):
        return
    rules = data.get("extraction_rules")
    if not isinstance(rules, list):
        return
    kept: List[Any] = []
    for r in rules:
        if not isinstance(r, dict):
            continue
        rid = _rule_id_from_extraction_rule(r)
        if rid and rid in keep:
            kept.append(copy.deepcopy(r))
    data["extraction_rules"] = kept


def _clear_discovery_scope_lists_for_deploy(doc: Dict[str, Any]) -> None:
    for lk in _DISCOVERY_SCOPE_LIST_KEYS:
        if lk in doc:
            doc[lk] = []


def _collect_source_view_indices_for_trim(
    doc: Mapping[str, Any], canvas: Mapping[str, Any]
) -> Set[int]:
    """Indices referenced by ``associations`` (source_view_to_extraction) or ``source_view`` canvas nodes."""
    out: Set[int] = set()
    raw = doc.get("associations")
    if isinstance(raw, list):
        for row in raw:
            if not isinstance(row, dict):
                continue
            if str(row.get("kind") or "").strip() != KIND_SOURCE_VIEW_TO_EXTRACTION:
                continue
            ii = coerce_association_source_view_index(row.get("source_view_index"))
            if ii is not None:
                out.add(ii)
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
    """Shrink ``source_views`` to *keep_indices*, remapping canvas and ``associations`` rows."""
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
    raw = doc.get("associations")
    if not isinstance(raw, list):
        return
    for row in raw:
        if not isinstance(row, dict):
            continue
        if str(row.get("kind") or "").strip() != KIND_SOURCE_VIEW_TO_EXTRACTION:
            continue
        oi = coerce_association_source_view_index(row.get("source_view_index"))
        if oi is None:
            continue
        ni = old_to_new.get(oi)
        if ni is not None:
            row["source_view_index"] = ni


def _prune_aliasing_rules(doc: Dict[str, Any], keep: Set[str]) -> None:
    if not keep:
        return
    al = doc.get("aliasing")
    if not isinstance(al, dict):
        return
    cfg = al.get("config")
    if not isinstance(cfg, dict):
        return
    data = cfg.get("data")
    if not isinstance(data, dict):
        return
    rules = data.get("aliasing_rules")
    if isinstance(rules, list):
        kept_ar: List[Any] = []
        for r in rules:
            if not isinstance(r, dict):
                continue
            nm = _name_from_aliasing_rule(r)
            if nm and nm in keep:
                kept_ar.append(copy.deepcopy(r))
        data["aliasing_rules"] = kept_ar
    restrict_aliasing_pathways_to_wanted_names(data, keep)
    defs = doc.get("aliasing_rule_definitions")
    if isinstance(defs, dict):
        doc["aliasing_rule_definitions"] = {
            k: copy.deepcopy(v) for k, v in defs.items() if k in keep
        }


def trim_scope_document_for_trigger_input(scope_document: Mapping[str, Any]) -> Dict[str, Any]:
    """Deep-copy *scope_document*, flatten canvas subgraphs, strip layout, prune unreferenced rules.

    Used for ``workflow.input.configuration`` only. Omits root ``compiled_workflow`` when present.

    Prunes ``key_extraction`` / ``aliasing`` rule rows to those referenced by executable canvas nodes.
    Drops unused ``source_views`` rows when ``associations`` and/or ``source_view`` canvas nodes
    reference a strict subset of indices, remapping those indices afterward.
    Clears discovery top-level lists (``view_queries``, ``transforms``, …) when present: configuration
    for those stages is only on canvas ``node.data.config``, not in deploy lists.
    """
    out = copy.deepcopy(dict(scope_document))
    out.pop("compiled_workflow", None)
    canvas = out.get("canvas")
    if not isinstance(canvas, dict):
        return out
    flat = flatten_subgraphs_in_canvas(canvas)
    _strip_positions_from_canvas(flat)
    ext_keep, als_keep = _collect_executable_rule_names_from_flat_canvas(flat)
    out["canvas"] = flat
    _clear_discovery_scope_lists_for_deploy(out)
    _prune_key_extraction_rules(out, ext_keep)
    _prune_aliasing_rules(out, als_keep)
    sv_keep = _collect_source_view_indices_for_trim(out, flat)
    _prune_source_views_and_remap(out, flat, sv_keep)
    return out
