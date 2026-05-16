"""Discovery validate stage: apply validation_rules to score fields on predecessor RAW cohort rows.

In ``strings`` output mode, parallel per-value scores for ``aliases`` and ``discoveredKey`` use the
top-level property ``confidence`` (parallel list to those string lists) in the in-memory properties
dict. Legacy rows may still carry ``aliases_confidence`` or ``discoveredKey_confidence``; readers
accept those as fallbacks when resolving input scores for ``aliases`` / ``discoveredKey``.

When rows are written to RAW via :func:`build_entity_cohort_row`, ``confidence`` is stored in
the dedicated ``CONFIDENCE`` column (JSON array string) and omitted from ``PROPERTIES_JSON``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from .confidence_match_eval import apply_confidence_match_rules_to_float_scores
from .confidence_match_rule_refs import (
    definitions_lookup_from_scope,
    expand_confidence_match_rules_list,
    sequences_lookup_from_scope,
    validation_rules_list_get,
)
from .discovery_query_shared import _first_nonempty

DEFAULT_VALIDATE_FIELDS = ("aliases", "discoveredKey")
DEFAULT_INITIAL_CONFIDENCE = 1.0
DEFAULT_MIN_CONFIDENCE = 0.0


def _confidence_property_key(field: str) -> str:
    """JSON property name for parallel per-value confidence scores (strings output mode)."""
    if field in ("discoveredKey", "aliases"):
        return "confidence"
    return f"{field}_confidence"


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def validate_validation_config(cfg: Mapping[str, Any]) -> None:
    desc = _first_nonempty(cfg.get("description"))
    if not desc:
        raise ValueError("validation config requires non-empty description")
    rules = materialize_validation_rules(cfg)
    if not rules:
        raise ValueError(
            "validation config requires validation_rule_definitions and/or validation_rules"
        )


def materialize_validation_rules(cfg: Mapping[str, Any]) -> List[Any]:
    """
    Build expanded ``validation_rules`` from task config.

    Canvas nodes typically store one rule under ``validation_rule_definitions``;
    scope YAML may also use ``validation_rules`` refs resolved via definitions/sequences
    on the same config object.
    """
    doc = _as_dict(cfg)
    lookup = definitions_lookup_from_scope(doc)
    sequences = sequences_lookup_from_scope(doc)
    inline = validation_rules_list_get(doc)
    if isinstance(inline, list) and inline:
        return expand_confidence_match_rules_list(
            inline,
            lookup,
            sequences=sequences,
            context="validate.config.",
        )
    if lookup:
        return [lookup[k] for k in sorted(lookup.keys())]
    return []


def _parse_validate_fields(cfg: Mapping[str, Any]) -> List[str]:
    raw = cfg.get("validate_fields")
    if isinstance(raw, list):
        out = [str(x).strip() for x in raw if str(x).strip()]
        if out:
            return out
    single = _first_nonempty(cfg.get("validate_field"))
    if single:
        return [single]
    return list(DEFAULT_VALIDATE_FIELDS)


def _initial_confidence(cfg: Mapping[str, Any]) -> float:
    v = cfg.get("initial_confidence")
    if v is None:
        return DEFAULT_INITIAL_CONFIDENCE
    try:
        return float(v)
    except (TypeError, ValueError):
        return DEFAULT_INITIAL_CONFIDENCE


def _min_confidence(cfg: Mapping[str, Any]) -> float:
    v = cfg.get("min_confidence")
    if v is None:
        return DEFAULT_MIN_CONFIDENCE
    try:
        return float(v)
    except (TypeError, ValueError):
        return DEFAULT_MIN_CONFIDENCE


def _float_conf(x: Any, *, default: float) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _parallel_confidence_list(field: str, src: Optional[Mapping[str, Any]]) -> Optional[List[float]]:
    """Parallel float list for *field* values (e.g. ``confidence`` for ``aliases`` / ``discoveredKey``)."""
    if not field or not isinstance(src, Mapping):
        return None
    keys: List[str] = []
    if field == "aliases":
        keys = ["aliases_confidence", _confidence_property_key(field)]
    elif field == "discoveredKey":
        keys = ["discoveredKey_confidence", _confidence_property_key(field)]
    else:
        keys = [_confidence_property_key(field)]
    for key in keys:
        v = src.get(key)
        if isinstance(v, list) and v:
            return [_float_conf(x, default=0.0) for x in v]
    return None


def _normalize_field_values(
    raw: Any,
    *,
    initial: float,
    field: str = "",
    parallel_source: Optional[Mapping[str, Any]] = None,
) -> List[Tuple[str, float]]:
    """Return (value, confidence) pairs to score."""
    if raw is None:
        return []
    if isinstance(raw, str):
        s = raw.strip()
        return [(s, initial)] if s else []
    if isinstance(raw, list):
        if field and parallel_source is not None:
            par = _parallel_confidence_list(field, parallel_source)
            all_str = all(isinstance(x, str) for x in raw)
            if all_str and par:
                strs = [str(x).strip() for x in raw if isinstance(x, str) and str(x).strip()]
                if strs:
                    out_zip: List[Tuple[str, float]] = []
                    for i, s in enumerate(strs):
                        c = par[i] if i < len(par) else initial
                        out_zip.append((s, c))
                    return out_zip
        out: List[Tuple[str, float]] = []
        for item in raw:
            if isinstance(item, str):
                s = item.strip()
                if s:
                    out.append((s, initial))
            elif isinstance(item, dict):
                v = _first_nonempty(item.get("value"), item.get("alias"), item.get("key"))
                if not v:
                    continue
                try:
                    c = float(item.get("confidence", initial))
                except (TypeError, ValueError):
                    c = initial
                out.append((v, c))
        return out
    s = str(raw).strip()
    return [(s, initial)] if s else []


def _write_scored_field(
    props: MutableMapping[str, Any],
    field: str,
    scored: Sequence[Tuple[str, float]],
    *,
    output_mode: str,
) -> None:
    mode = (output_mode or "strings").strip().lower()
    conf_key = _confidence_property_key(field)
    if mode == "scored_objects":
        props[field] = [{"value": v, "confidence": round(c, 6)} for v, c in scored]
        props.pop(conf_key, None)
        return
    props[field] = [v for v, _ in scored]
    if scored:
        props[conf_key] = [round(c, 6) for _, c in scored]
    else:
        props.pop(conf_key, None)


def validate_row_properties(
    props: Mapping[str, Any],
    cfg: Mapping[str, Any],
    rules_raw: List[Any],
) -> Dict[str, Any]:
    """Return new properties dict with validated/scored fields."""
    out = dict(props)
    if not bool(cfg.get("enabled", True)):
        return out

    initial = _initial_confidence(cfg)
    min_c = _min_confidence(cfg)
    default_em = cfg.get("expression_match")
    output_mode = _first_nonempty(cfg.get("output_mode"), "strings")
    fields = _parse_validate_fields(cfg)

    for field in fields:
        if field not in out and field not in props:
            continue
        pairs = _normalize_field_values(
            out.get(field, props.get(field)),
            initial=initial,
            field=field,
            parallel_source=props,
        )
        if not pairs:
            continue
        scored = apply_confidence_match_rules_to_float_scores(
            pairs,
            rules_raw=rules_raw,
            default_expression_match=default_em if isinstance(default_em, dict) else cfg,
        )
        filtered = [(v, c) for v, c in scored if c >= min_c]
        _write_scored_field(out, field, filtered, output_mode=output_mode)

    if "aliases" in fields:
        out.pop("aliases_confidence", None)
    return out


def discovery_handle_validate(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    from fn_dm_validate.engine.validate_runtime import discovery_handle_validate as _impl

    return _impl(fn_external_id, data, client, log)
