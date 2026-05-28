"""
Shared scoring_rules evaluation for key discovery and aliasing.

Resolves expression_match per rule (rule -> validation default -> search).
offset modifiers chain; explicit modifier then stops further rules for that value.

Each rule may set ``score_modifier`` (or ``on_match.score_modifier``) when
``match`` succeeds, and ``on_no_match.score_modifier`` when it does not.

``scoring_rules`` is a **hierarchy**:

- A **top-level YAML list** is an **ordered** pipeline (strict list order).
- **Shorthand** for a linear chain: ``{ "first_rule_id": [ tail... ] }`` — one mapping per segment.
  Tail entries are strings or nested one-key mappings; a right-nested tail
  ``{ a: [ { b: [ c ] } ] }`` runs ``a`` then ``b`` then ``c`` (same order as a flat list
  ``[a, b, c]``) but nests each step under the previous key. Equivalent to nested
  ``hierarchy: { mode: ordered, children: [...] }``.
- Use ``hierarchy: { mode: ordered | concurrent, children: [ ... ] }`` for explicit grouping or
  ``concurrent`` (children by ascending ``priority``).
"""

from __future__ import annotations

import re
from types import SimpleNamespace
from typing import Any, Callable, List, Optional, Sequence, Tuple

ExpressionMatchMode = str  # "search" | "fullmatch"

ModifierPair = Tuple[str, float]  # (mode, value)

RuntimeRule = Tuple[
    int,
    int,
    Optional[str],
    List[re.Pattern],
    List[str],
    ExpressionMatchMode,
    Optional[ModifierPair],
    Optional[ModifierPair],
]


def normalize_expression_match(value: Any) -> Optional[ExpressionMatchMode]:
    if value is None:
        return None
    s = str(value).strip().lower()
    if s in ("search", "fullmatch"):
        return s
    return None


def resolve_expression_match_for_rule(
    rule_dict: dict,
    validation_default: Optional[Any],
) -> ExpressionMatchMode:
    em = normalize_expression_match(rule_dict.get("expression_match"))
    if em:
        return em
    if isinstance(validation_default, dict):
        vd = normalize_expression_match(validation_default.get("expression_match"))
    elif validation_default is not None:
        vd = normalize_expression_match(
            getattr(validation_default, "expression_match", None)
        )
    else:
        vd = None
    if vd:
        return vd
    return "search"


def expression_items_to_pattern_strings(expressions: Any) -> List[str]:
    """Normalize match.expressions entries to regex strings."""
    out: List[str] = []
    if not expressions:
        return out
    for e in expressions:
        if isinstance(e, str):
            s = str(e).strip()
            if s:
                out.append(s)
        elif isinstance(e, dict):
            p = str(e.get("pattern", "") or "").strip()
            if p:
                out.append(p)
        elif hasattr(e, "pattern"):
            p = str(getattr(e, "pattern", "") or "").strip()
            if p:
                out.append(p)
    return out


def score_rule_as_dict(raw: Any) -> Optional[dict]:
    if raw is None:
        return None
    if hasattr(raw, "model_dump"):
        return raw.model_dump(mode="python")
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, SimpleNamespace):
        return {
            k: getattr(raw, k)
            for k in vars(raw)
            if not k.startswith("_")
        }
    return None


def value_matches_score_rule(
    key_value: str,
    compiled: Sequence[re.Pattern],
    keywords: Sequence[str],
    expression_match: ExpressionMatchMode,
) -> bool:
    kv_lower = key_value.lower()
    if any(kw.lower() in kv_lower for kw in keywords):
        return True
    for cre in compiled:
        if expression_match == "fullmatch":
            if cre.fullmatch(key_value):
                return True
        else:
            if cre.search(key_value):
                return True
    return False


def apply_score_modifier_value(score: float, mode: str, value: float) -> float:
    if mode == "explicit":
        out = value
    else:
        out = score + value
    return max(0.0, min(1.0, out))


def _parse_score_modifier_block(raw: Any) -> Optional[ModifierPair]:
    if not isinstance(raw, dict):
        if hasattr(raw, "model_dump"):
            raw = raw.model_dump(mode="python")
        elif isinstance(raw, SimpleNamespace):
            raw = vars(raw)
        else:
            return None
    mode = raw.get("mode")
    if mode not in ("explicit", "offset"):
        return None
    try:
        value = float(raw.get("value", 0.0))
    except (TypeError, ValueError):
        return None
    return (str(mode), value)


def _resolve_rule_modifiers(rd: dict) -> Tuple[Optional[ModifierPair], Optional[ModifierPair]]:
    """
    Return ``(on_match, on_no_match)`` modifier pairs for a rule.

    ``score_modifier`` (legacy) and ``on_match.score_modifier`` apply when
    the rule's ``match`` succeeds; ``on_no_match.score_modifier`` applies when it
    does not.
    """
    on_match: Optional[ModifierPair] = None
    on_match_block = rd.get("on_match")
    if isinstance(on_match_block, dict):
        on_match = _parse_score_modifier_block(on_match_block.get("score_modifier"))
    if on_match is None:
        on_match = _parse_score_modifier_block(rd.get("score_modifier"))

    on_no_match: Optional[ModifierPair] = None
    on_no_match_block = rd.get("on_no_match")
    if isinstance(on_no_match_block, dict):
        on_no_match = _parse_score_modifier_block(
            on_no_match_block.get("score_modifier")
        )
    return on_match, on_no_match


def _hierarchy_children_list(hi: Any) -> Optional[List[Any]]:
    if not isinstance(hi, dict):
        return None
    ch = hi.get("children")
    if isinstance(ch, list):
        return list(ch)
    return None


_SHORTHAND_EXCLUDE_KEYS = frozenset(
    {
        "hierarchy",
        "ref",
        "sequence",
        "match",
        "name",
        "enabled",
        "priority",
        "expression_match",
        "score_modifier",
        "on_match",
        "on_no_match",
        "pipeline_input",
        "pipeline_output",
    }
)


def is_shorthand_chain_mapping(d: dict) -> bool:
    """``{ rule_id: [ tail... ] }`` — not a full inline rule body."""
    if len(d) != 1:
        return False
    (k, v), = d.items()
    if str(k).strip() in _SHORTHAND_EXCLUDE_KEYS:
        return False
    return isinstance(v, list)


def expand_shorthand_mapping_to_hierarchy(d: dict) -> dict:
    (k, v), = d.items()
    head = str(k).strip()
    children: List[Any] = [head]
    for x in v:
        if isinstance(x, str):
            children.append(x.strip())
        elif isinstance(x, dict):
            children.append(normalize_score_match_step(x))
        else:
            children.append(x)
    return {"hierarchy": {"mode": "ordered", "children": children}}


def normalize_score_match_step(node: Any) -> Any:
    """Expand shorthand chain mappings; recurse into ``hierarchy`` children."""
    if not isinstance(node, dict):
        return node
    if is_shorthand_chain_mapping(node):
        return normalize_score_match_step(expand_shorthand_mapping_to_hierarchy(node))
    hi = node.get("hierarchy")
    if isinstance(hi, dict):
        ch = _hierarchy_children_list(hi)
        if ch is not None:
            new_hi = dict(hi)
            new_hi["children"] = [normalize_score_match_step(x) for x in ch]
            return {"hierarchy": new_hi}
    return node


def _parse_group_node(node: Any) -> Optional[Tuple[str, List[Any]]]:
    """Return ``(mode, children)`` where mode is ``concurrent`` | ``ordered``, or None if not a group."""
    if not isinstance(node, dict):
        return None
    hi = node.get("hierarchy")
    if not isinstance(hi, dict):
        return None
    ch = _hierarchy_children_list(hi)
    if ch is None:
        raise ValueError("scoring_rules hierarchy requires children: [...]")
    raw_mode = str(hi.get("mode") or "ordered").strip().lower()
    if raw_mode not in ("ordered", "concurrent"):
        raise ValueError(
            "scoring_rules hierarchy.mode must be 'ordered' or 'concurrent', "
            f"not {raw_mode!r}"
        )
    if raw_mode == "concurrent":
        return ("concurrent", ch)
    return ("ordered", ch)


def min_priority_in_subtree(node: Any, child_index: int = 0) -> int:
    """Minimum ``priority`` among rule leaves in *node* (for sorting concurrent siblings)."""
    node = normalize_score_match_step(node)
    grouped = _parse_group_node(node)
    if grouped is not None:
        _mode, ch = grouped
        if not ch:
            return 10**9
        return min((min_priority_in_subtree(c, i) for i, c in enumerate(ch)), default=10**9)
    rd = score_rule_as_dict(node)
    if not rd:
        return 10**9
    pri = rd.get("priority")
    if pri is None:
        return child_index * 10
    try:
        return int(pri)
    except (TypeError, ValueError):
        return child_index * 10


def _sort_concurrent_children(children: List[Any]) -> List[Any]:
    indexed = list(enumerate(children or []))
    indexed.sort(key=lambda ic: (min_priority_in_subtree(ic[1], ic[0]), ic[0]))
    return [c for _, c in indexed]


def build_sorted_score_runtime(
    rules_raw: List[Any],
    *,
    default_expression_match: Optional[Any] = None,
    rules_order: str = "priority",
    log_warning: Optional[Callable[[str], None]] = None,
    log_verbose: Optional[Callable[[str, str], None]] = None,
) -> List[RuntimeRule]:
    """Build runtime tuples including per-rule expression_match mode, then sort for application."""
    runtime: List[RuntimeRule] = []
    for idx, raw in enumerate(rules_raw or []):
        rd = score_rule_as_dict(raw)
        if not rd:
            continue
        if not rd.get("enabled", True):
            continue
        match = rd.get("match") or {}
        if hasattr(match, "model_dump"):
            match = match.model_dump(mode="python")
        elif isinstance(match, SimpleNamespace):
            match = vars(match)
        exprs = expression_items_to_pattern_strings(match.get("expressions"))
        kws = [k for k in (match.get("keywords") or []) if str(k).strip()]
        on_match_mod, on_no_match_mod = _resolve_rule_modifiers(rd)
        if not exprs and not kws:
            if log_verbose:
                log_verbose(
                    "WARNING",
                    "Skipping score_match_rule with empty match (no expressions or keywords)",
                )
            continue
        if on_match_mod is None and on_no_match_mod is None:
            if log_warning:
                log_warning(
                    f"Skipping score_match_rule {rd.get('name', idx)!r}: "
                    "no score_modifier, on_match, or on_no_match"
                )
            continue
        pri = rd.get("priority")
        if pri is None:
            pri = idx * 10
        compiled: List[re.Pattern] = []
        for pat in exprs:
            try:
                compiled.append(re.compile(pat))
            except re.error as e:
                if log_warning:
                    log_warning(
                        f"Invalid regex in score_match_rule {rd.get('name', idx)!r} "
                        f"pattern {pat!r}: {e}"
                    )
        expr_mode = resolve_expression_match_for_rule(rd, default_expression_match)
        name = rd.get("name")
        runtime.append(
            (int(pri), idx, name, compiled, kws, expr_mode, on_match_mod, on_no_match_mod)
        )
    order = str(rules_order or "priority").strip().lower()
    if order == "list":
        runtime.sort(key=lambda t: (t[1],))
    else:
        runtime.sort(key=lambda t: (t[0], t[1]))
    return runtime


def _apply_runtime_to_item(
    item: Any,
    runtime: List[RuntimeRule],
    *,
    value_attr: str,
    score_attr: str,
    log_verbose: Optional[Callable[[str, str], None]],
) -> bool:
    """Apply sorted runtime rules to one item. Returns True if ``explicit`` stopped the chain."""
    for _pri, _idx, name, compiled, kws, expr_mode, on_match_mod, on_no_match_mod in runtime:
        val = getattr(item, value_attr, None)
        if val is None:
            continue
        key_value = str(val)
        matched = value_matches_score_rule(key_value, compiled, kws, expr_mode)
        mod = on_match_mod if matched else on_no_match_mod
        if mod is None:
            continue
        mod_mode, mod_val = mod
        cur = float(getattr(item, score_attr, 0.0) or 0.0)
        new_c = apply_score_modifier_value(cur, mod_mode, mod_val)
        setattr(item, score_attr, new_c)
        if log_verbose:
            branch = "match" if matched else "no_match"
            log_verbose(
                "DEBUG",
                f"score_match_rule {name or _idx!r} ({branch}) applied to key "
                f"{key_value!r} -> {new_c:.4f}",
            )
        if mod_mode == "explicit":
            return True
    return False


def _apply_leaf_rule_to_item(
    item: Any,
    rule_raw: Any,
    *,
    value_attr: str,
    score_attr: str,
    default_expression_match: Optional[Any],
    log_warning: Optional[Callable[[str], None]],
    log_verbose: Optional[Callable[[str, str], None]],
) -> bool:
    runtime = build_sorted_score_runtime(
        [rule_raw],
        default_expression_match=default_expression_match,
        rules_order="list",
        log_warning=log_warning,
        log_verbose=log_verbose,
    )
    if not runtime:
        return False
    return _apply_runtime_to_item(
        item,
        runtime,
        value_attr=value_attr,
        score_attr=score_attr,
        log_verbose=log_verbose,
    )


def _apply_node_for_item(
    item: Any,
    node: Any,
    *,
    value_attr: str,
    score_attr: str,
    default_expression_match: Optional[Any],
    log_warning: Optional[Callable[[str], None]],
    log_verbose: Optional[Callable[[str, str], None]],
) -> bool:
    """Returns True if ``explicit`` stopped further processing for this item."""
    node = normalize_score_match_step(node)
    grouped = _parse_group_node(node)
    if grouped is not None:
        mode, children = grouped
        if mode == "concurrent":
            for child in _sort_concurrent_children(children):
                if _apply_node_for_item(
                    item,
                    child,
                    value_attr=value_attr,
                    score_attr=score_attr,
                    default_expression_match=default_expression_match,
                    log_warning=log_warning,
                    log_verbose=log_verbose,
                ):
                    return True
            return False
        for child in children:
            if _apply_node_for_item(
                item,
                child,
                value_attr=value_attr,
                score_attr=score_attr,
                default_expression_match=default_expression_match,
                log_warning=log_warning,
                log_verbose=log_verbose,
            ):
                return True
        return False
    return _apply_leaf_rule_to_item(
        item,
        node,
        value_attr=value_attr,
        score_attr=score_attr,
        default_expression_match=default_expression_match,
        log_warning=log_warning,
        log_verbose=log_verbose,
    )


def apply_score_match_rules_mutating(
    items: Sequence[Any],
    *,
    value_attr: str = "value",
    score_attr: str = "score",
    rules_raw: List[Any],
    default_expression_match: Optional[Any] = None,
    log_warning: Optional[Callable[[str], None]] = None,
    log_verbose: Optional[Callable[[str, str], None]] = None,
) -> None:
    """
    Mutate each item's score using hierarchical ``scoring_rules``.
    offset: apply and continue; explicit: apply and break for that item.
    """
    steps = [normalize_score_match_step(s) for s in list(rules_raw or [])]
    for item in items:
        for step in steps:
            if _apply_node_for_item(
                item,
                step,
                value_attr=value_attr,
                score_attr=score_attr,
                default_expression_match=default_expression_match,
                log_warning=log_warning,
                log_verbose=log_verbose,
            ):
                break


def apply_score_match_rules_to_float_scores(
    scores: List[Tuple[str, float]],
    *,
    rules_raw: List[Any],
    default_expression_match: Optional[Any] = None,
    log_warning: Optional[Callable[..., None]] = None,
    log_verbose: Optional[Callable[..., None]] = None,
) -> List[Tuple[str, float]]:
    """
    Apply rules to (value, score) pairs; returns new list of (value, score).
    Used by aliasing when working with plain strings.
    """
    class _Bag:
        __slots__ = ("value", "score")

        def __init__(self, value: str, score: float) -> None:
            self.value = value
            self.score = score

    bags = [_Bag(v, c) for v, c in scores]
    apply_score_match_rules_mutating(
        bags,
        rules_raw=rules_raw,
        default_expression_match=default_expression_match,
        log_warning=log_warning,
        log_verbose=log_verbose,
    )
    return [(b.value, b.score) for b in bags]
