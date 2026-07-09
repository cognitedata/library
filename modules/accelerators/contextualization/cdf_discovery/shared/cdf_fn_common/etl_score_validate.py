"""Scoring stage: apply scoring_rules to fields on predecessor rows."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from .etl_common import _first_nonempty
from .etl_score_match_eval import apply_score_match_rules_to_float_scores
from .etl_score_property import score_property_key

DEFAULT_INITIAL_SCORE = 1.0
DEFAULT_MIN_SCORE = 0.0


def validate_scoring_config(cfg: Mapping[str, Any]) -> None:
    desc = _first_nonempty(cfg.get("description"))
    if not desc:
        raise ValueError("score config requires non-empty description")
    rules = materialize_scoring_rules(cfg)
    if not rules:
        raise ValueError("score config requires non-empty scoring_rules")
    if not _parse_score_fields(cfg):
        raise ValueError("score config requires non-empty score_fields")


def materialize_scoring_rules(cfg: Mapping[str, Any]) -> List[Any]:
    raw = cfg.get("scoring_rules")
    if isinstance(raw, list) and raw:
        return list(raw)
    return []


def _parse_score_fields(cfg: Mapping[str, Any]) -> List[str]:
    raw = cfg.get("score_fields")
    if isinstance(raw, list):
        out = [str(x).strip() for x in raw if str(x).strip()]
        if out:
            return out
    return []


def _initial_score(cfg: Mapping[str, Any]) -> float:
    v = cfg.get("initial_score")
    if v is None:
        return DEFAULT_INITIAL_SCORE
    try:
        return float(v)
    except (TypeError, ValueError):
        return DEFAULT_INITIAL_SCORE


def _min_score(cfg: Mapping[str, Any]) -> float:
    v = cfg.get("min_score")
    if v is None:
        return DEFAULT_MIN_SCORE
    try:
        return float(v)
    except (TypeError, ValueError):
        return DEFAULT_MIN_SCORE


def _min_threshold_filter_enabled(cfg: Mapping[str, Any]) -> bool:
    return bool(cfg.get("min_threshold_filter_enabled"))


def _min_threshold(cfg: Mapping[str, Any]) -> float:
    v = cfg.get("min_threshold")
    if v is None:
        return _min_score(cfg)
    try:
        return float(v)
    except (TypeError, ValueError):
        return _min_score(cfg)


def _apply_min_threshold_filter(
    pairs: List[Tuple[str, float]], cfg: Mapping[str, Any]
) -> List[Tuple[str, float]]:
    if not _min_threshold_filter_enabled(cfg):
        return pairs
    bound = _min_threshold(cfg)
    return [(v, c) for v, c in pairs if c >= bound]


def _normalize_field_values(
    raw: Any,
    *,
    initial: float,
    field: str = "",
    parallel_source: Optional[Mapping[str, Any]] = None,
) -> List[Tuple[str, float]]:
    if raw is None:
        return []
    if isinstance(raw, str):
        s = raw.strip()
        return [(s, initial)] if s else []
    if isinstance(raw, list):
        score_key = score_property_key(field) if field else ""
        if field and parallel_source is not None:
            par = parallel_source.get(score_key)
            if isinstance(par, list) and par and all(isinstance(x, str) for x in raw):
                strs = [str(x).strip() for x in raw if str(x).strip()]
                out: List[Tuple[str, float]] = []
                for i, s in enumerate(strs):
                    try:
                        c = float(par[i]) if i < len(par) else initial
                    except (TypeError, ValueError):
                        c = initial
                    out.append((s, c))
                if out:
                    return out
        out2: List[Tuple[str, float]] = []
        for item in raw:
            if isinstance(item, str):
                s = item.strip()
                if s:
                    out2.append((s, initial))
            elif isinstance(item, dict):
                v = _first_nonempty(item.get("value"), item.get("key"))
                if not v:
                    continue
                try:
                    c = float(item.get("score", initial))
                except (TypeError, ValueError):
                    c = initial
                out2.append((v, c))
        return out2
    s = str(raw).strip()
    return [(s, initial)] if s else []


def _write_parallel_scores(
    props: MutableMapping[str, Any],
    field: str,
    scored: Sequence[Tuple[str, float]],
) -> None:
    key = score_property_key(field)
    if scored:
        props[key] = [round(c, 6) for _, c in scored]
    else:
        props.pop(key, None)


def score_primary_value_field(cfg: Mapping[str, Any]) -> str:
    fields = _parse_score_fields(cfg)
    if not fields:
        raise ValueError("score config requires non-empty score_fields")
    return fields[0]


def score_row_properties(
    props: Mapping[str, Any],
    cfg: Mapping[str, Any],
    rules_raw: Sequence[Any],
) -> Dict[str, Any]:
    out = dict(props)
    initial = _initial_score(cfg)
    min_score = _min_score(cfg)
    rules = list(rules_raw)
    for field in _parse_score_fields(cfg):
        pairs = _normalize_field_values(
            out.get(field),
            initial=initial,
            field=field,
            parallel_source=out,
        )
        if rules:
            pairs = apply_score_match_rules_to_float_scores(pairs, rules_raw=rules)
        pairs = [(v, max(min_score, c)) for v, c in pairs]
        pairs = _apply_min_threshold_filter(pairs, cfg)
        values = [v for v, _ in pairs]
        out[field] = values
        _write_parallel_scores(out, field, pairs)
    return out
