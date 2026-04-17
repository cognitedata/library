"""Heuristic extraction: delimiter_split, sliding_token, loose_patterns (handler: heuristic)."""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from ...utils.DataStructures import ExtractedKey, ExtractionMethod
from ...utils.rule_utils import get_extraction_type_from_rule, get_rule_id
from .ExtractionMethodHandler import ExtractionMethodHandler


def _rule_attr(rule: Any, key: str, default: Any = None) -> Any:
    if rule is None:
        return default
    if isinstance(rule, dict):
        return rule.get(key, default)
    return getattr(rule, key, default)


def _spec_attr(spec: Any, key: str, default: Any = None) -> Any:
    if spec is None:
        return default
    if isinstance(spec, dict):
        return spec.get(key, default)
    return getattr(spec, key, default)


def _rule_fields(rule: Any) -> List[Any]:
    raw = _rule_attr(rule, "fields")
    if not raw:
        return []
    return list(raw)


def _params(rule: Any) -> Any:
    return _rule_attr(rule, "parameters")


def _strategy_weight_map(rule: Any) -> Dict[str, float]:
    p = _params(rule)
    if p is None:
        return {}
    strategies = getattr(p, "strategies", None) if not isinstance(p, dict) else p.get("strategies")
    if not strategies:
        return {}
    out: Dict[str, float] = {}
    for s in strategies:
        if isinstance(s, dict):
            sid = str(s.get("id", "")).strip()
            w = float(s.get("weight", 1.0))
        else:
            sid = str(getattr(s, "id", "")).strip()
            w = float(getattr(s, "weight", 1.0))
        if sid:
            out[sid] = w
    return out


def _max_candidates(rule: Any) -> int:
    p = _params(rule)
    if p is None:
        return 20
    if isinstance(p, dict):
        return int(p.get("max_candidates_per_field", 20))
    return int(getattr(p, "max_candidates_per_field", 20))


_TOKEN_SPLIT_RE = re.compile(r"[\s,;|/\\]+")


def _delimiter_split_candidates(text: str) -> List[str]:
    parts = [p for p in _TOKEN_SPLIT_RE.split(text.strip()) if p]
    out: List[str] = []
    seen: Set[str] = set()
    for p in parts:
        if len(p) >= 2 and p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _sliding_token_candidates(text: str, min_len: int = 3, max_len: int = 24) -> List[str]:
    t = text.strip()
    if len(t) < min_len:
        return []
    out: List[str] = []
    seen: Set[str] = set()
    n = len(t)
    for size in range(min_len, min(max_len, n) + 1):
        for i in range(0, n - size + 1):
            sub = t[i : i + size]
            if sub in seen:
                continue
            seen.add(sub)
            out.append(sub)
    return out


_LOOSE_PATTERNS = (
    re.compile(r"\b[A-Z]{1,3}[-_]?\d{2,6}[A-Z]?\b"),
    re.compile(r"\b[A-Za-z]{2,12}[-_]\d{2,6}\b"),
    re.compile(r"\b\d{3,12}[A-Za-z]{0,3}\b"),
    # Unit/area numeric prefix + tag body (e.g. 10-P-1234 when tags repeat per unit).
    # Letter segment after prefix avoids matching bare ISO-like dates (2024-01-15).
    re.compile(r"\b\d{1,4}[-_][A-Za-z]{1,6}(?:[-_][A-Za-z0-9]{1,12})+\b"),
)


def _loose_patterns_candidates(text: str) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for pat in _LOOSE_PATTERNS:
        for m in pat.finditer(text):
            s = m.group(0).strip()
            if len(s) >= 2 and s not in seen:
                seen.add(s)
                out.append(s)
    return out


_STRATEGY_FUNCS = {
    "delimiter_split": _delimiter_split_candidates,
    "sliding_token": _sliding_token_candidates,
    "sliding_window": _sliding_token_candidates,
    "loose_patterns": _loose_patterns_candidates,
}


def _combine_scores(
    candidates_with_strategy: List[Tuple[str, str]],
    weights: Dict[str, float],
) -> List[Tuple[str, float]]:
    """Aggregate (value, strategy_id) into (value, score in [0,1]) using strategy weights."""
    acc: Dict[str, float] = {}
    max_w = max(weights.values()) if weights else 1.0
    if max_w <= 0:
        max_w = 1.0
    for val, sid in candidates_with_strategy:
        w = float(weights.get(sid, 1.0))
        acc[val] = max(acc.get(val, 0.0), w)
    out: List[Tuple[str, float]] = []
    for val, score in acc.items():
        out.append((val, min(1.0, max(0.0, score / max_w))))
    out.sort(key=lambda x: -x[1])
    return out


class HeuristicExtractionHandler(ExtractionMethodHandler):
    """Emit candidate substrings using weighted strategies; same validation as regex_handler."""

    HANDLER_METHOD = ExtractionMethod.HEURISTIC

    def extract_from_entity(
        self,
        entity: Dict[str, Any],
        rule: Any,
        context: Dict[str, Any],
        *,
        get_field_value: Callable[..., Optional[str]],
    ) -> List[ExtractedKey]:
        if not self._rule_applies_to_entity_types(rule, context):
            return []

        fields = _rule_fields(rule)
        if not fields:
            return []

        weights = _strategy_weight_map(rule)
        if not weights:
            weights = {"delimiter_split": 1.0}

        cap = _max_candidates(rule)
        rid = get_rule_id(rule)
        ext_type = get_extraction_type_from_rule(rule)

        keys: List[ExtractedKey] = []

        for spec in fields:
            raw = get_field_value(entity, spec, _rule_attr(rule, "rule_id") or rid)
            if raw is None or str(raw).strip() == "":
                continue
            text = str(raw).strip()
            fn = _spec_attr(spec, "field_name") or "field"
            combined = self._candidates_for_text(text, weights)
            for val, conf in combined[:cap]:
                if not val:
                    continue
                keys.append(
                    ExtractedKey(
                        value=val,
                        extraction_type=ext_type,
                        source_field=fn,
                        confidence=conf,
                        method=self.HANDLER_METHOD,
                        rule_id=rid,
                        metadata={
                            "context": context,
                            "handler": self.HANDLER_METHOD.value,
                            "heuristic": True,
                        },
                        source_inputs={fn: text},
                    )
                )

        return keys

    def _rule_applies_to_entity_types(self, rule: Any, context: Dict[str, Any]) -> bool:
        et = _rule_attr(rule, "entity_types") or []
        if not et:
            return True
        ctx_et = (context.get("entity_type") or "").strip().lower()
        allowed = {str(x).strip().lower() for x in et}
        return ctx_et in allowed

    def _candidates_for_text(self, text: str, weights: Dict[str, float]) -> List[Tuple[str, float]]:
        pairs: List[Tuple[str, str]] = []
        for sid in weights:
            fn = _STRATEGY_FUNCS.get(sid)
            if not fn:
                self.logger.verbose("WARNING", f"Unknown heuristic strategy id: {sid}")
                continue
            try:
                cands = fn(text)
            except Exception as e:
                self.logger.verbose("WARNING", f"Heuristic strategy {sid} failed: {e}")
                continue
            for c in cands:
                pairs.append((c, sid))
        return _combine_scores(pairs, weights)
