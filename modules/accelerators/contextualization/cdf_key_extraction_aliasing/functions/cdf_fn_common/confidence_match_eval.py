"""
Shared confidence_match_rules evaluation for key extraction and aliasing.

Resolves expression_match per rule (rule -> validation default -> search).
offset modifiers chain; explicit modifier then stops further rules for that value.
"""

from __future__ import annotations

import re
from types import SimpleNamespace
from typing import Any, Callable, List, Optional, Sequence, Tuple

ExpressionMatchMode = str  # "search" | "fullmatch"

RuntimeRule = Tuple[
    int,
    int,
    Optional[str],
    List[re.Pattern],
    List[str],
    str,
    float,
    ExpressionMatchMode,
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


def confidence_rule_as_dict(raw: Any) -> Optional[dict]:
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


def value_matches_confidence_rule(
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


def apply_confidence_modifier_value(confidence: float, mode: str, value: float) -> float:
    if mode == "explicit":
        out = value
    else:
        out = confidence + value
    return max(0.0, min(1.0, out))


def build_sorted_confidence_runtime(
    rules_raw: List[Any],
    *,
    default_expression_match: Optional[Any] = None,
    log_warning: Optional[Callable[[str], None]] = None,
    log_verbose: Optional[Callable[[str, str], None]] = None,
) -> List[RuntimeRule]:
    """Build sorted runtime tuples including per-rule expression_match mode."""
    runtime: List[RuntimeRule] = []
    for idx, raw in enumerate(rules_raw or []):
        rd = confidence_rule_as_dict(raw)
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
        if not exprs and not kws:
            if log_verbose:
                log_verbose(
                    "WARNING",
                    "Skipping confidence_match_rule with empty match (no expressions or keywords)",
                )
            continue
        mod = rd.get("confidence_modifier") or {}
        if hasattr(mod, "model_dump"):
            mod = mod.model_dump(mode="python")
        elif isinstance(mod, SimpleNamespace):
            mod = vars(mod)
        mode = mod.get("mode")
        if mode not in ("explicit", "offset"):
            if log_warning:
                log_warning(
                    f"Skipping confidence_match_rule {rd.get('name', idx)!r}: "
                    f"invalid confidence_modifier.mode {mode!r}"
                )
            continue
        try:
            mod_val = float(mod.get("value", 0.0))
        except (TypeError, ValueError):
            if log_warning:
                log_warning(
                    f"Skipping confidence_match_rule {rd.get('name', idx)!r}: "
                    "invalid confidence_modifier.value"
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
                        f"Invalid regex in confidence_match_rule {rd.get('name', idx)!r} "
                        f"pattern {pat!r}: {e}"
                    )
        expr_mode = resolve_expression_match_for_rule(rd, default_expression_match)
        name = rd.get("name")
        runtime.append((int(pri), idx, name, compiled, kws, mode, mod_val, expr_mode))
    runtime.sort(key=lambda t: (t[0], t[1]))
    return runtime


def apply_confidence_match_rules_mutating(
    items: Sequence[Any],
    *,
    value_attr: str = "value",
    confidence_attr: str = "confidence",
    rules_raw: List[Any],
    default_expression_match: Optional[Any] = None,
    log_warning: Optional[Callable[[str], None]] = None,
    log_verbose: Optional[Callable[[str, str], None]] = None,
) -> None:
    """
    Mutate each item's confidence by applying confidence_match_rules in order.
    offset: apply and continue; explicit: apply and break for that item.
    """
    runtime = build_sorted_confidence_runtime(
        rules_raw,
        default_expression_match=default_expression_match,
        log_warning=log_warning,
        log_verbose=log_verbose,
    )
    if not runtime:
        return
    for item in items:
        for _pri, _idx, name, compiled, kws, mod_mode, mod_val, expr_mode in runtime:
            val = getattr(item, value_attr, None)
            if val is None:
                continue
            key_value = str(val)
            if not value_matches_confidence_rule(
                key_value, compiled, kws, expr_mode
            ):
                continue
            cur = float(getattr(item, confidence_attr, 0.0) or 0.0)
            new_c = apply_confidence_modifier_value(cur, mod_mode, mod_val)
            setattr(item, confidence_attr, new_c)
            if log_verbose:
                log_verbose(
                    "DEBUG",
                    f"confidence_match_rule {name or _idx!r} applied to key {key_value!r} -> {new_c:.4f}",
                )
            if mod_mode == "explicit":
                break


def apply_confidence_match_rules_to_float_scores(
    scores: List[Tuple[str, float]],
    *,
    rules_raw: List[Any],
    default_expression_match: Optional[Any] = None,
    log_warning: Optional[Callable[..., None]] = None,
    log_verbose: Optional[Callable[..., None]] = None,
) -> List[Tuple[str, float]]:
    """
    Apply rules to (value, confidence) pairs; returns new list of (value, confidence).
    Used by aliasing when working with plain strings.
    """
    class _Bag:
        __slots__ = ("value", "confidence")

        def __init__(self, value: str, confidence: float) -> None:
            self.value = value
            self.confidence = confidence

    bags = [_Bag(v, c) for v, c in scores]
    apply_confidence_match_rules_mutating(
        bags,
        rules_raw=rules_raw,
        default_expression_match=default_expression_match,
        log_warning=log_warning,
        log_verbose=log_verbose,
    )
    return [(b.value, b.confidence) for b in bags]
