"""Discovery validate stage: apply validation_rules to score fields on predecessor RAW cohort rows.

In ``strings`` output mode, parallel per-value scores use ``{field}_confidence`` on the
properties dict. When rows are written to RAW via :func:`build_entity_cohort_row`, scores are
stored in the dedicated ``CONFIDENCE`` column and omitted from ``PROPERTIES_JSON``.
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
from .confidence_property import confidence_property_key
from .discovery_query_shared import _first_nonempty

DEFAULT_VALIDATE_FIELDS = ("aliases", "discoveredKey")
DEFAULT_INITIAL_CONFIDENCE = 1.0
DEFAULT_MIN_CONFIDENCE = 0.0


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
    """Parallel float list for *field* at ``{field}_confidence`` only."""
    if not field or not isinstance(src, Mapping):
        return None
    key = confidence_property_key(field)
    v = src.get(key)
    if isinstance(v, list) and v:
        return [_float_conf(x, default=0.0) for x in v]
    return None


def _primary_confidence_field(fields: Sequence[str]) -> Optional[str]:
    """Field whose per-value scores are written for each validated field."""
    for cand in ("aliases", "discoveredKey"):
        if cand in fields:
            return cand
    return fields[0] if fields else None


def _write_parallel_confidence(
    props: MutableMapping[str, Any],
    field: str,
    scored: Sequence[Tuple[str, float]],
) -> None:
    key = confidence_property_key(field)
    if scored:
        props[key] = [round(c, 6) for _, c in scored]
    else:
        props.pop(key, None)


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
        score_prop = confidence_property_key(field) if field else ""
        if raw and field == score_prop:
            out_num: List[Tuple[str, float]] = []
            for x in raw:
                try:
                    out_num.append((str(x), float(x)))
                except (TypeError, ValueError):
                    continue
            if out_num:
                return out_num
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
    conf_key = confidence_property_key(field)
    if mode == "scored_objects":
        props[field] = [{"value": v, "confidence": round(c, 6)} for v, c in scored]
        props.pop(conf_key, None)
        return
    props[field] = [v for v, _ in scored]
    if scored:
        props[conf_key] = [round(c, 6) for _, c in scored]
    else:
        props.pop(conf_key, None)
    props.pop("confidence", None)


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
        _write_scored_field(out, field, scored, output_mode=output_mode)

    out.pop("confidence", None)
    return out


def validate_primary_value_field(cfg: Mapping[str, Any]) -> str:
    """Primary field for RAW ``CONFIDENCE`` column on validate task output."""
    return _primary_confidence_field(_parse_validate_fields(cfg)) or "aliases"


def discovery_handle_validate(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    from fn_dm_validate.engine.validate_runtime import discovery_handle_validate as _impl

    return _impl(fn_external_id, data, client, log)
