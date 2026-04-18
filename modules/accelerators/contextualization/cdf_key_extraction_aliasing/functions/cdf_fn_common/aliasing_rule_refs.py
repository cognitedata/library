"""Resolve ``extraction_rules[].aliasing_pipeline`` using shared definition/sequence libraries.

Same structural grammar as ``confidence_match_rules`` (string refs, ``ref:``, ``sequence:``,
``hierarchy: { mode: ordered | concurrent, children: [...] }``, shorthand ``{ id: [ tail... ] }``).

Scope keys:

- ``aliasing_rule_definitions`` — id → transformation rule body.
- ``aliasing_rule_sequences`` — sequence id → ordered definition ids.

Removed from the document after resolution (like confidence-match definitions).
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


def _expand_aliasing_pipeline_entry(
    item: Any,
    i: int,
    lookup: Dict[str, Dict[str, Any]],
    seqmap: Dict[str, List[str]],
    context: str,
) -> List[Any]:
    if isinstance(item, str):
        rid = item.strip()
        if not rid:
            return []
        if rid not in lookup:
            raise ValueError(
                f"{context}aliasing_pipeline[{i}]: unknown rule reference {rid!r}; "
                f"define it under {_DEFINITIONS_KEY}"
            )
        return [copy.deepcopy(lookup[rid])]
    if not isinstance(item, dict):
        raise ValueError(
            f"{context}aliasing_pipeline[{i}]: expected mapping or string ref, got {type(item).__name__}"
        )
    item = normalize_confidence_match_step(item)
    if isinstance(item.get("hierarchy"), dict):
        hi = item.get("hierarchy") or {}
        ch = _hierarchy_children_raw(hi)
        if not isinstance(ch, list):
            raise ValueError(
                f"{context}aliasing_pipeline[{i}].hierarchy: expected `children` list"
            )
        nested = expand_aliasing_pipeline_list(
            ch,
            lookup,
            sequences=seqmap,
            context=f"{context}aliasing_pipeline[{i}].hierarchy.",
        )
        new_hi = copy.deepcopy(hi)
        new_hi["children"] = nested
        return [{"hierarchy": new_hi}]
    seq_id = item.get("sequence")
    if seq_id is not None and str(seq_id).strip():
        if item.get("ref") is not None:
            raise ValueError(
                f"{context}aliasing_pipeline[{i}]: use either `ref` or `sequence`, not both"
            )
        sid = str(seq_id).strip()
        if sid not in seqmap:
            raise ValueError(
                f"{context}aliasing_pipeline[{i}]: unknown sequence {sid!r}; "
                f"define it under {_SEQUENCES_KEY}"
            )
        extra_keys = set(item.keys()) - {"sequence"}
        if extra_keys:
            raise ValueError(
                f"{context}aliasing_pipeline[{i}]: sequence object must contain only "
                f"`sequence` (got extra keys: {sorted(extra_keys)})"
            )
        out_seq: List[Any] = []
        for rid in seqmap[sid]:
            if rid not in lookup:
                raise ValueError(
                    f"{context}aliasing_pipeline[{i}]: sequence {sid!r} references "
                    f"unknown rule {rid!r} (not in {_DEFINITIONS_KEY})"
                )
            out_seq.append(copy.deepcopy(lookup[rid]))
        return out_seq
    ref = item.get("ref")
    if ref is not None and str(ref).strip():
        rid = str(ref).strip()
        if rid not in lookup:
            raise ValueError(
                f"{context}aliasing_pipeline[{i}]: unknown ref {rid!r}; "
                f"define it under {_DEFINITIONS_KEY}"
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
    context: str = "",
) -> List[Any]:
    """Expand an ``aliasing_pipeline`` value using *lookup* and optional named *sequences*."""
    if rules is None:
        return []
    if not isinstance(rules, list):
        return []
    seqmap = sequences or {}
    out: List[Any] = []
    for i, item in enumerate(rules):
        out.extend(_expand_aliasing_pipeline_entry(item, i, lookup, seqmap, context))
    return out


def resolve_aliasing_pipeline_refs_in_scope_document(doc: MutableMapping[str, Any]) -> None:
    """Mutate *doc*: expand ``extraction_rules[].aliasing_pipeline`` in place."""
    lookup = definitions_lookup_from_scope(doc)
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
                            context=f"key_extraction.config.data.extraction_rules[{j}].",
                        )

    doc.pop(_DEFINITIONS_KEY, None)
    doc.pop(_SEQUENCES_KEY, None)
