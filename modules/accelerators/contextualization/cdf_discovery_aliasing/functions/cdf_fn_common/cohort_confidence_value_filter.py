"""Per-value confidence pruning for discovery ``confidence_filter`` canvas stage."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from .confidence_property import confidence_property_key
from .discovery_query_shared import _first_nonempty
from .discovery_validate import _normalize_field_values, _write_scored_field

DEFAULT_VALUE_FIELD = "aliases"
DEFAULT_MIN_CONFIDENCE = 0.0
COMPARISONS = frozenset({"gte", "gt", "lte", "lt"})


def _comparison_op(cfg: Mapping[str, Any]) -> str:
    op = str(cfg.get("comparison") or "gte").strip().lower()
    if op not in COMPARISONS:
        raise ValueError(f"comparison must be one of {sorted(COMPARISONS)}; got {op!r}")
    return op


def _min_confidence(cfg: Mapping[str, Any]) -> float:
    v = cfg.get("min_confidence")
    if v is None:
        return DEFAULT_MIN_CONFIDENCE
    try:
        return float(v)
    except (TypeError, ValueError):
        return DEFAULT_MIN_CONFIDENCE


def _value_field(cfg: Mapping[str, Any]) -> str:
    vf = _first_nonempty(cfg.get("value_field"))
    return vf or DEFAULT_VALUE_FIELD


def _pair_passes(score: float, threshold: float, op: str) -> bool:
    if op == "gte":
        return score >= threshold
    if op == "gt":
        return score > threshold
    if op == "lte":
        return score <= threshold
    if op == "lt":
        return score < threshold
    return False


def validate_confidence_filter_config(cfg: Mapping[str, Any]) -> None:
    if not _first_nonempty(cfg.get("description")):
        raise ValueError("confidence_filter config requires non-empty description")
    _comparison_op(cfg)
    _value_field(cfg)


def _strip_stale_score_keys(
    props: MutableMapping[str, Any],
    *,
    score_key: str,
) -> None:
    props.pop("confidence", None)
    for k in list(props.keys()):
        if k.endswith("_confidence") and k != score_key:
            props.pop(k, None)


def apply_confidence_value_filter(
    props: Mapping[str, Any],
    cfg: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Prune aligned (value, score) pairs; return updated props or ``None`` to drop the row.
    """
    if not bool(cfg.get("enabled", True)):
        return dict(props)

    value_field = _value_field(cfg)
    score_key = confidence_property_key(value_field)
    threshold = _min_confidence(cfg)
    op = _comparison_op(cfg)
    drop_if_empty = bool(cfg.get("drop_row_if_empty", True))
    output_mode = _first_nonempty(cfg.get("output_mode"), "strings")

    pairs = _normalize_field_values(
        props.get(value_field),
        initial=1.0,
        field=value_field,
        parallel_source=props,
    )
    if not pairs:
        return None if drop_if_empty else dict(props)

    kept: List[Tuple[str, float]] = [
        (v, c) for v, c in pairs if _pair_passes(c, threshold, op)
    ]
    if not kept and drop_if_empty:
        return None

    out: Dict[str, Any] = dict(props)
    _strip_stale_score_keys(out, score_key=score_key)
    _write_scored_field(out, value_field, kept, output_mode=output_mode)
    return out


def confidence_value_field_from_config(cfg: Mapping[str, Any]) -> str:
    """``value_field`` for RAW confidence column split/merge on this task."""
    return _value_field(cfg)
