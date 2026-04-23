"""Resolve ``validation_rules`` list entries that reference a shared definition library.

Scope YAML (v1) may define reusable rules under top-level ``validation_rule_definitions``
(mapping rule id -> body, or a list of objects with ``name``). Any ``validation_rules``
list may then use:

- A string entry: the rule id / name to clone from definitions.
- A mapping with ``ref: <id>`` and optional overrides merged onto the definition.
- A mapping ``{sequence: <sequence_id>}`` to insert the ordered rule list from
  ``confidence_match_rule_sequences`` (named chains of definition ids for sequential scoring).

Optional ``confidence_match_rule_targets`` is merged with ``validation_rules`` (targets
after base list) so validation can declare one or more named targets in a separate key.

Hierarchical entries use ``hierarchy: { mode: ordered | concurrent, children: [ ... ] }``.

Shorthand for a linear chain: ``{ "first_rule_id": [ "second", ... ] }`` (one key, list value);
it is normalized to ``hierarchy`` before expansion.

Inline rule objects (with ``name`` and ``match`` / etc., no ``ref`` / ``sequence``) pass through.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from cdf_fn_common.confidence_match_eval import normalize_confidence_match_step

_DEFINITIONS_KEY = "validation_rule_definitions"
_SEQUENCES_KEY = "confidence_match_rule_sequences"
_TARGETS_KEY = "confidence_match_rule_targets"

_RULES_LIST_KEY = "validation_rules"
_LEGACY_RULES_LIST_KEY = "confidence_match_rules"


def validation_rules_list_get(block: Mapping[str, Any]) -> Any:
    """Read the validation rule pipeline list (prefers ``validation_rules`` over legacy alias)."""
    if _RULES_LIST_KEY in block:
        return block.get(_RULES_LIST_KEY)
    return block.get(_LEGACY_RULES_LIST_KEY)


def validation_rules_list_set(block: MutableMapping[str, Any], rules: List[Any]) -> None:
    block[_RULES_LIST_KEY] = rules
    block.pop(_LEGACY_RULES_LIST_KEY, None)


def definitions_lookup_from_scope(doc: MutableMapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Build name -> rule dict from ``validation_rule_definitions``."""
    raw = doc.get(_DEFINITIONS_KEY)
    return _definitions_as_lookup(raw)


def sequences_lookup_from_scope(doc: MutableMapping[str, Any]) -> Dict[str, List[str]]:
    """Build sequence id -> ordered definition ids from ``confidence_match_rule_sequences``."""
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


def dedupe_confidence_match_rules_by_name(rules: List[Any]) -> List[Any]:
    """Drop later leaf rules with the same ``name`` as an earlier leaf (keeps first). Unnamed kept."""
    seen: set = set()
    return _dedupe_rules_tree(rules, seen)


def merge_validation_dict_overlay(global_block: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """Merge *overlay* onto *global_block*: concatenate ``validation_rules`` (deduped); other keys from *overlay* win."""
    merged = dict(global_block)
    brules = list(validation_rules_list_get(merged) or [])
    orules = validation_rules_list_get(overlay)
    if isinstance(orules, list) and orules:
        validation_rules_list_set(
            merged,
            dedupe_confidence_match_rules_by_name(brules + list(orules)),
        )
    else:
        validation_rules_list_set(merged, dedupe_confidence_match_rules_by_name(brules))
    for k, v in overlay.items():
        if k in (_RULES_LIST_KEY, _LEGACY_RULES_LIST_KEY):
            continue
        if v is not None:
            merged[k] = v
    return merged


def _hierarchy_children_raw(hi: Any) -> Optional[List[Any]]:
    if not isinstance(hi, dict):
        return None
    ch = hi.get("children")
    if isinstance(ch, list):
        return list(ch)
    return None


def _dedupe_rules_tree(nodes: List[Any], seen: set) -> List[Any]:
    out: List[Any] = []
    for r in nodes:
        if isinstance(r, dict):
            r = normalize_confidence_match_step(r)
        if isinstance(r, dict) and isinstance(r.get("hierarchy"), dict):
            hi = r.get("hierarchy") or {}
            ch = _hierarchy_children_raw(hi)
            if ch is not None:
                new_hi = dict(hi)
                new_hi["children"] = _dedupe_rules_tree(ch, seen)
                out.append({"hierarchy": new_hi})
            continue
        name = None
        if isinstance(r, dict):
            name = r.get("name")
        else:
            name = getattr(r, "name", None)
        if isinstance(name, str) and name.strip():
            key = name.strip()
            if key in seen:
                continue
            seen.add(key)
        out.append(r)
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
    return out


def _merge_rule_base_and_overrides(
    base: Dict[str, Any], overrides: Dict[str, Any]
) -> Dict[str, Any]:
    merged = copy.deepcopy(base)
    for k, v in overrides.items():
        if k == "ref":
            continue
        merged[k] = copy.deepcopy(v)
    return merged


def _expand_confidence_match_entry(
    item: Any,
    i: int,
    lookup: Dict[str, Dict[str, Any]],
    seqmap: Dict[str, List[str]],
    context: str,
) -> List[Any]:
    """Expand one list entry into zero or more leaf rule dicts (sequence may expand to many)."""
    if isinstance(item, str):
        rid = item.strip()
        if not rid:
            return []
        if rid not in lookup:
            raise ValueError(
                f"{context}{_RULES_LIST_KEY}[{i}]: unknown rule reference {rid!r}; "
                f"define it under {_DEFINITIONS_KEY}"
            )
        return [copy.deepcopy(lookup[rid])]
    if not isinstance(item, dict):
        raise ValueError(
            f"{context}{_RULES_LIST_KEY}[{i}]: expected mapping or string ref, got {type(item).__name__}"
        )
    item = normalize_confidence_match_step(item)
    if isinstance(item.get("hierarchy"), dict):
        hi = item.get("hierarchy") or {}
        ch = _hierarchy_children_raw(hi)
        if not isinstance(ch, list):
            raise ValueError(
                f"{context}{_RULES_LIST_KEY}[{i}].hierarchy: expected `children` list"
            )
        nested = expand_confidence_match_rules_list(
            ch,
            lookup,
            sequences=seqmap,
            context=f"{context}{_RULES_LIST_KEY}[{i}].hierarchy.",
        )
        new_hi = copy.deepcopy(hi)
        new_hi["children"] = nested
        return [{"hierarchy": new_hi}]
    seq_id = item.get("sequence")
    if seq_id is not None and str(seq_id).strip():
        if item.get("ref") is not None:
            raise ValueError(
                f"{context}{_RULES_LIST_KEY}[{i}]: use either `ref` or `sequence`, not both"
            )
        if item.get("match") is not None:
            raise ValueError(
                f"{context}{_RULES_LIST_KEY}[{i}]: `sequence` entries must not include `match`"
            )
        sid = str(seq_id).strip()
        if sid not in seqmap:
            raise ValueError(
                f"{context}{_RULES_LIST_KEY}[{i}]: unknown sequence {sid!r}; "
                f"define it under {_SEQUENCES_KEY}"
            )
        extra_keys = set(item.keys()) - {"sequence"}
        if extra_keys:
            raise ValueError(
                f"{context}{_RULES_LIST_KEY}[{i}]: sequence object must contain only "
                f"`sequence` (got extra keys: {sorted(extra_keys)})"
            )
        out_seq: List[Any] = []
        for rid in seqmap[sid]:
            if rid not in lookup:
                raise ValueError(
                    f"{context}{_RULES_LIST_KEY}[{i}]: sequence {sid!r} references "
                    f"unknown rule {rid!r} (not in {_DEFINITIONS_KEY})"
                )
            out_seq.append(copy.deepcopy(lookup[rid]))
        return out_seq
    ref = item.get("ref")
    if ref is not None and str(ref).strip():
        rid = str(ref).strip()
        if rid not in lookup:
            raise ValueError(
                f"{context}{_RULES_LIST_KEY}[{i}]: unknown ref {rid!r}; "
                f"define it under {_DEFINITIONS_KEY}"
            )
        base = lookup[rid]
        if len(item) <= 1 or (len(item) == 1 and "ref" in item):
            return [copy.deepcopy(base)]
        return [_merge_rule_base_and_overrides(base, item)]
    return [copy.deepcopy(item)]


def expand_confidence_match_rules_list(
    rules: Any,
    lookup: Dict[str, Dict[str, Any]],
    *,
    sequences: Optional[Dict[str, List[str]]] = None,
    context: str = "",
) -> List[Any]:
    """Expand a ``validation_rules`` value using *lookup* and optional named *sequences*."""
    if rules is None:
        return []
    if not isinstance(rules, list):
        return []
    seqmap = sequences or {}
    out: List[Any] = []
    for i, item in enumerate(rules):
        out.extend(_expand_confidence_match_entry(item, i, lookup, seqmap, context))
    return out


def _patch_validation_block(
    validation: Any,
    lookup: Dict[str, Dict[str, Any]],
    sequences: Dict[str, List[str]],
    *,
    context: str,
) -> Optional[Dict[str, Any]]:
    if validation is None:
        return None
    if not isinstance(validation, dict):
        return None
    block = dict(validation)
    has_rules = _RULES_LIST_KEY in block or _LEGACY_RULES_LIST_KEY in block
    has_targets = _TARGETS_KEY in block
    if not has_rules and not has_targets:
        return block
    combined: List[Any] = []
    base = validation_rules_list_get(block)
    if isinstance(base, list):
        combined.extend(base)
    targets = block.get(_TARGETS_KEY)
    if isinstance(targets, list):
        combined.extend(targets)
    if combined:
        validation_rules_list_set(block, combined)
    block.pop(_TARGETS_KEY, None)
    expanded = expand_confidence_match_rules_list(
        validation_rules_list_get(block),
        lookup,
        sequences=sequences,
        context=context,
    )
    validation_rules_list_set(block, dedupe_confidence_match_rules_by_name(expanded))
    return block


def resolve_confidence_match_rule_refs_in_scope_document(doc: MutableMapping[str, Any]) -> None:
    """Mutate *doc* in place: expand rule refs wherever ``validation_rules`` appears.

    Top-level ``source_views[]`` rows do not carry ``validation`` (use extraction / global / aliasing
    validation only). When ``validation_rule_definitions`` is absent, *lookup* is empty: inline rule dicts
    pass through; string or ``ref:`` entries raise unless the id exists in definitions.
    Removes the definitions and sequences keys from *doc* after resolution so downstream
    config matches the legacy shape (definitions are not part of key_extraction Pydantic models).
    """
    lookup = definitions_lookup_from_scope(doc)
    sequences = sequences_lookup_from_scope(doc)

    ke = doc.get("key_extraction")
    if isinstance(ke, dict):
        kcfg = ke.get("config")
        if isinstance(kcfg, dict):
            data = kcfg.get("data")
            if isinstance(data, dict):
                pv = data.get("validation")
                patched = _patch_validation_block(
                    pv, lookup, sequences, context="key_extraction.config.data.validation."
                )
                if patched is not None:
                    data["validation"] = patched
                rules = data.get("extraction_rules")
                if isinstance(rules, list):
                    for j, rule in enumerate(rules):
                        if not isinstance(rule, dict):
                            continue
                        rv = rule.get("validation")
                        patched = _patch_validation_block(
                            rv,
                            lookup,
                            sequences,
                            context=f"key_extraction.config.data.extraction_rules[{j}].validation.",
                        )
                        if patched is not None:
                            rule["validation"] = patched

    al = doc.get("aliasing")
    if isinstance(al, dict):
        acfg = al.get("config")
        if isinstance(acfg, dict):
            adata = acfg.get("data")
            if isinstance(adata, dict):
                av = adata.get("validation")
                patched = _patch_validation_block(
                    av, lookup, sequences, context="aliasing.config.data.validation."
                )
                if patched is not None:
                    adata["validation"] = patched
                arules = adata.get("aliasing_rules")
                if isinstance(arules, list):
                    for k, arule in enumerate(arules):
                        if not isinstance(arule, dict):
                            continue
                        rv = arule.get("validation")
                        patched = _patch_validation_block(
                            rv,
                            lookup,
                            sequences,
                            context=f"aliasing.config.data.aliasing_rules[{k}].validation.",
                        )
                        if patched is not None:
                            arule["validation"] = patched

                pw = adata.get("pathways")
                if isinstance(pw, dict) and isinstance(pw.get("steps"), list):
                    for si, step in enumerate(pw["steps"]):
                        if not isinstance(step, dict):
                            continue
                        mode = str(step.get("mode") or "sequential").strip().lower()
                        if mode == "sequential":
                            srules = step.get("rules")
                            if isinstance(srules, list):
                                for rk, arule in enumerate(srules):
                                    if not isinstance(arule, dict):
                                        continue
                                    rv = arule.get("validation")
                                    patched = _patch_validation_block(
                                        rv,
                                        lookup,
                                        sequences,
                                        context=(
                                            f"aliasing.config.data.pathways.steps[{si}].rules[{rk}].validation."
                                        ),
                                    )
                                    if patched is not None:
                                        arule["validation"] = patched
                        elif mode == "parallel":
                            branches = step.get("branches")
                            if not isinstance(branches, list):
                                continue
                            for bi, br in enumerate(branches):
                                rules_list = None
                                if isinstance(br, dict):
                                    rules_list = br.get("rules")
                                elif isinstance(br, list):
                                    rules_list = br
                                if not isinstance(rules_list, list):
                                    continue
                                for rk, arule in enumerate(rules_list):
                                    if not isinstance(arule, dict):
                                        continue
                                    rv = arule.get("validation")
                                    patched = _patch_validation_block(
                                        rv,
                                        lookup,
                                        sequences,
                                        context=(
                                            f"aliasing.config.data.pathways.steps[{si}].branches[{bi}].rules[{rk}].validation."
                                        ),
                                    )
                                    if patched is not None:
                                        arule["validation"] = patched

    doc.pop(_DEFINITIONS_KEY, None)
    doc.pop(_SEQUENCES_KEY, None)
