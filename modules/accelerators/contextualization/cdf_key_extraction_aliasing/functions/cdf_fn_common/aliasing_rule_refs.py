"""Resolve aliasing rule indirection in v1 scope documents.

Expands:

- ``key_extraction.config.data.extraction_rules[].aliasing_pipeline`` — string refs,
  ``ref:``, ``sequence:``, ``hierarchy: { … children: … }`` (same grammar as confidence steps).
- ``aliasing.config.data.pathways.steps[]`` — the same ref grammar on each sequential
  ``rules`` list and on each parallel branch (``branches[].rules`` or a branch that is a bare list).

Scope libraries (removed after resolution, like confidence-match definitions):

- ``aliasing_rule_definitions`` — id → transformation rule body.
- ``aliasing_rule_sequences`` — sequence id → ordered definition ids.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, MutableMapping, Optional

from cdf_fn_common.confidence_match_eval import normalize_confidence_match_step

_DEFINITIONS_KEY = "aliasing_rule_definitions"
_SEQUENCES_KEY = "aliasing_rule_sequences"


def definitions_lookup_from_scope(doc: MutableMapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Build name → rule dict from ``aliasing_rule_definitions``."""
    raw = doc.get(_DEFINITIONS_KEY)
    return _definitions_as_lookup(raw)


def _is_inline_transform_rule_body(row: Dict[str, Any]) -> bool:
    """True for concrete aliasing transform specs (not a bare ``ref`` / ``sequence`` shim)."""
    if row.get("ref") is not None and str(row.get("ref", "")).strip():
        return False
    if row.get("sequence") is not None and str(row.get("sequence", "")).strip():
        return False
    if row.get("hierarchy") is not None:
        return False
    nm = row.get("name")
    if nm is None or not str(nm).strip():
        return False
    return row.get("handler") is not None or row.get("type") is not None


def _inline_transform_rules_by_name_from_aliasing_data(
    doc: MutableMapping[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """
    Collect name → rule dict from ``aliasing.config.data.aliasing_rules`` and from
    ``pathways`` step rule lists (before ref expansion).

    Used so ``key_extraction`` ``aliasing_pipeline`` string ids can resolve to rules that
    only appear inline under aliasing (e.g. ``prefix_suffix_1``) while ``aliasing_rule_definitions``
    still override on name collision.
    """
    out: Dict[str, Dict[str, Any]] = {}

    def ingest_list(rules: Any) -> None:
        if not isinstance(rules, list):
            return
        for r in rules:
            if not isinstance(r, dict):
                continue
            if not _is_inline_transform_rule_body(r):
                continue
            key = str(r.get("name", "")).strip()
            if key:
                out[key] = copy.deepcopy(r)

    al = doc.get("aliasing")
    if not isinstance(al, dict):
        return out
    acfg = al.get("config")
    if not isinstance(acfg, dict):
        return out
    adata = acfg.get("data")
    if not isinstance(adata, dict):
        return out

    ingest_list(adata.get("aliasing_rules"))

    pw = adata.get("pathways")
    if not isinstance(pw, dict):
        return out
    steps = pw.get("steps")
    if not isinstance(steps, list):
        return out
    for step in steps:
        if not isinstance(step, dict):
            continue
        mode = str(step.get("mode") or "sequential").strip().lower()
        if mode == "sequential":
            ingest_list(step.get("rules"))
        elif mode == "parallel":
            for br in step.get("branches") or []:
                if isinstance(br, dict):
                    ingest_list(br.get("rules"))
                elif isinstance(br, list):
                    ingest_list(br)
    return out


def _combined_aliasing_transform_lookup(doc: MutableMapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Inline pathway / flat rules first, then ``aliasing_rule_definitions`` (definitions win)."""
    inline = _inline_transform_rules_by_name_from_aliasing_data(doc)
    defs = definitions_lookup_from_scope(doc)
    merged: Dict[str, Dict[str, Any]] = {**inline, **defs}
    return merged


def sequences_lookup_from_scope(doc: MutableMapping[str, Any]) -> Dict[str, List[str]]:
    """Build sequence id → ordered definition ids from ``aliasing_rule_sequences``."""
    raw = doc.get(_SEQUENCES_KEY)
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, List[str]] = {}
    for k, v in raw.items():
        sk = str(k).strip()
        if not sk or not isinstance(v, list):
            continue
        ids: List[str] = []
        for x in v:
            s = str(x).strip()
            if s:
                ids.append(s)
        out[sk] = ids
    return out


def _definitions_as_lookup(raw: Any) -> Dict[str, Dict[str, Any]]:
    if raw is None:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            if not isinstance(v, dict):
                continue
            d = copy.deepcopy(v)
            key = str(k).strip()
            nm = d.get("name")
            if nm is None or (isinstance(nm, str) and not str(nm).strip()):
                d["name"] = key
            name = str(d.get("name") or key).strip()
            if name:
                out[name] = d
        return out
    if isinstance(raw, list):
        for v in raw:
            if not isinstance(v, dict):
                continue
            nm = v.get("name")
            if nm is None or not str(nm).strip():
                continue
            name = str(nm).strip()
            out[name] = copy.deepcopy(v)
        return out
    return {}


def _merge_rule_base_and_overrides(
    base: Dict[str, Any], overrides: Dict[str, Any]
) -> Dict[str, Any]:
    merged = copy.deepcopy(base)
    for k, v in overrides.items():
        if k == "ref":
            continue
        merged[k] = copy.deepcopy(v)
    return merged


def _hierarchy_children_raw(hi: Any) -> Optional[List[Any]]:
    if not isinstance(hi, dict):
        return None
    ch = hi.get("children")
    if isinstance(ch, list):
        return list(ch)
    return None


def _rule_slot(rules_path: str, i: int) -> str:
    return f"{rules_path}[{i}]" if rules_path else f"rules[{i}]"


def _expand_aliasing_pipeline_entry(
    item: Any,
    i: int,
    lookup: Dict[str, Dict[str, Any]],
    seqmap: Dict[str, List[str]],
    rules_path: str,
) -> List[Any]:
    slot = _rule_slot(rules_path, i)
    if isinstance(item, str):
        rid = item.strip()
        if not rid:
            return []
        if rid not in lookup:
            raise ValueError(
                f"{slot}: unknown rule reference {rid!r}; define it under {_DEFINITIONS_KEY}"
            )
        return [copy.deepcopy(lookup[rid])]
    if not isinstance(item, dict):
        raise ValueError(
            f"{slot}: expected mapping or string ref, got {type(item).__name__}"
        )
    item = normalize_confidence_match_step(item)
    if isinstance(item.get("hierarchy"), dict):
        hi = item.get("hierarchy") or {}
        ch = _hierarchy_children_raw(hi)
        if not isinstance(ch, list):
            raise ValueError(f"{slot}.hierarchy: expected `children` list")
        child_path = f"{slot}.hierarchy.children"
        nested = expand_aliasing_pipeline_list(
            ch,
            lookup,
            sequences=seqmap,
            rules_path=child_path,
        )
        new_hi = copy.deepcopy(hi)
        new_hi["children"] = nested
        return [{"hierarchy": new_hi}]
    seq_id = item.get("sequence")
    if seq_id is not None and str(seq_id).strip():
        if item.get("ref") is not None:
            raise ValueError(f"{slot}: use either `ref` or `sequence`, not both")
        sid = str(seq_id).strip()
        if sid not in seqmap:
            raise ValueError(
                f"{slot}: unknown sequence {sid!r}; define it under {_SEQUENCES_KEY}"
            )
        extra_keys = set(item.keys()) - {"sequence"}
        if extra_keys:
            raise ValueError(
                f"{slot}: sequence object must contain only `sequence` "
                f"(got extra keys: {sorted(extra_keys)})"
            )
        out_seq: List[Any] = []
        for rid in seqmap[sid]:
            if rid not in lookup:
                raise ValueError(
                    f"{slot}: sequence {sid!r} references unknown rule {rid!r} "
                    f"(not in {_DEFINITIONS_KEY})"
                )
            out_seq.append(copy.deepcopy(lookup[rid]))
        return out_seq
    ref = item.get("ref")
    if ref is not None and str(ref).strip():
        rid = str(ref).strip()
        if rid not in lookup:
            raise ValueError(
                f"{slot}: unknown ref {rid!r}; define it under {_DEFINITIONS_KEY}"
            )
        base = lookup[rid]
        if len(item) <= 1 or (len(item) == 1 and "ref" in item):
            return [copy.deepcopy(base)]
        return [_merge_rule_base_and_overrides(base, item)]
    return [copy.deepcopy(item)]


def expand_aliasing_pipeline_list(
    rules: Any,
    lookup: Dict[str, Dict[str, Any]],
    *,
    sequences: Optional[Dict[str, List[str]]] = None,
    rules_path: str = "",
) -> List[Any]:
    """Expand a rule list (pipeline or pathway step) using *lookup* and optional *sequences*."""
    if rules is None:
        return []
    if not isinstance(rules, list):
        return []
    seqmap = sequences or {}
    out: List[Any] = []
    for i, item in enumerate(rules):
        out.extend(_expand_aliasing_pipeline_entry(item, i, lookup, seqmap, rules_path))
    return out


def _expand_pathways_in_aliasing_data(
    adata: MutableMapping[str, Any],
    lookup: Dict[str, Dict[str, Any]],
    sequences: Dict[str, List[str]],
) -> None:
    pw = adata.get("pathways")
    if not isinstance(pw, dict):
        return
    steps = pw.get("steps")
    if not isinstance(steps, list):
        return
    base = "aliasing.config.data.pathways"
    for si, step in enumerate(steps):
        if not isinstance(step, dict):
            continue
        mode = str(step.get("mode") or "sequential").strip().lower()
        step_base = f"{base}.steps[{si}]"
        if mode == "sequential":
            raw = step.get("rules")
            if raw is None:
                continue
            if not isinstance(raw, list):
                raise ValueError(f"{step_base}.rules must be a list")
            step["rules"] = expand_aliasing_pipeline_list(
                raw,
                lookup,
                sequences=sequences,
                rules_path=f"{step_base}.rules",
            )
        elif mode == "parallel":
            branches = step.get("branches")
            if not isinstance(branches, list):
                continue
            for bi, br in enumerate(branches):
                br_base = f"{step_base}.branches[{bi}]"
                if isinstance(br, dict):
                    raw = br.get("rules")
                    if raw is None:
                        continue
                    if not isinstance(raw, list):
                        raise ValueError(f"{br_base}.rules must be a list")
                    br["rules"] = expand_aliasing_pipeline_list(
                        raw,
                        lookup,
                        sequences=sequences,
                        rules_path=f"{br_base}.rules",
                    )
                elif isinstance(br, list):
                    branches[bi] = expand_aliasing_pipeline_list(
                        br,
                        lookup,
                        sequences=sequences,
                        rules_path=br_base,
                    )


def resolve_aliasing_pipeline_refs_in_scope_document(doc: MutableMapping[str, Any]) -> None:
    """Mutate *doc*: expand extraction pipelines and aliasing pathways; strip definition keys."""
    lookup = _combined_aliasing_transform_lookup(doc)
    sequences = sequences_lookup_from_scope(doc)

    ke = doc.get("key_extraction")
    if isinstance(ke, dict):
        kcfg = ke.get("config")
        if isinstance(kcfg, dict):
            data = kcfg.get("data")
            if isinstance(data, dict):
                rules = data.get("extraction_rules")
                if isinstance(rules, list):
                    for j, rule in enumerate(rules):
                        if not isinstance(rule, dict):
                            continue
                        if "aliasing_pipeline" not in rule:
                            continue
                        raw = rule.get("aliasing_pipeline")
                        if not isinstance(raw, list):
                            raise ValueError(
                                f"key_extraction.config.data.extraction_rules[{j}].aliasing_pipeline "
                                "must be a list"
                            )
                        rule["aliasing_pipeline"] = expand_aliasing_pipeline_list(
                            raw,
                            lookup,
                            sequences=sequences,
                            rules_path=f"key_extraction.config.data.extraction_rules[{j}].aliasing_pipeline",
                        )

    al = doc.get("aliasing")
    if isinstance(al, dict):
        acfg = al.get("config")
        if isinstance(acfg, dict):
            adata = acfg.get("data")
            if isinstance(adata, dict):
                _expand_pathways_in_aliasing_data(adata, lookup, sequences)

    doc.pop(_DEFINITIONS_KEY, None)
    doc.pop(_SEQUENCES_KEY, None)
