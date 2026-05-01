"""Trim unified scope documents for ``workflow.input.configuration`` (deploy payload)."""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Set, Tuple

from functions.cdf_fn_common.scope_document_dm import restrict_aliasing_pathways_to_wanted_names
from functions.cdf_fn_common.workflow_compile.canvas_dag import flatten_subgraphs_in_canvas


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
    _prune_key_extraction_rules(out, ext_keep)
    _prune_aliasing_rules(out, als_keep)
    return out
