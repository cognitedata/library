"""
Deterministic hash of extraction source inputs (scope + rules fingerprint + field values).

Used for incremental cohort gating: skip emitting detected rows when inputs match
the last persisted EXTRACTION_INPUTS_HASH in RAW.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional, Tuple

from .incremental_scope import scope_key_from_view_dict
from .property_path import get_value_by_property_path


def _norm_entity_type_value(et: Any) -> str:
    if et is None:
        return ""
    if hasattr(et, "value"):
        return str(et.value).strip().lower()
    return str(et).strip().lower()


def _view_entity_type_normalized(entity_view_config: Any) -> Optional[str]:
    """Lowercase entity_type string from a source view config (dict or model)."""
    et = getattr(entity_view_config, "entity_type", None)
    if et is None and isinstance(entity_view_config, dict):
        et = entity_view_config.get("entity_type")
    s = _norm_entity_type_value(et)
    return s or None


def _entity_types_from_rule(rule: Any) -> Optional[List[Any]]:
    """``scope_filters.entity_type`` or top-level ``entity_types`` (list of strings)."""
    et_list: Optional[List[Any]] = None
    sf = getattr(rule, "scope_filters", None)
    if sf is None and isinstance(rule, dict):
        sf = rule.get("scope_filters")
    if isinstance(sf, dict):
        raw = sf.get("entity_type")
        if raw is not None:
            et_list = raw if isinstance(raw, list) else [raw]
    if et_list is None:
        top = getattr(rule, "entity_types", None)
        if top is None and isinstance(rule, dict):
            top = rule.get("entity_types")
        if top:
            et_list = top if isinstance(top, list) else [top]
    return et_list


def _rule_matches_view_entity_type(rule: Any, view_et: Optional[str]) -> bool:
    """
    When a rule declares ``scope_filters.entity_type`` or ``entity_types``, only include
    matching views. Rules with no such declaration apply to every view.
    """
    if view_et is None:
        return True
    et_list = _entity_types_from_rule(rule)
    if not et_list:
        return True
    allowed = {
        _norm_entity_type_value(x) for x in et_list if x is not None and str(x).strip()
    }
    if not allowed:
        return True
    return view_et in allowed


def _rule_field_rows(rule: Any) -> Any:
    """``fields`` or ``source_fields`` from a rule (engine accepts both)."""
    rows = getattr(rule, "fields", None) or getattr(rule, "source_fields", None)
    if rows is None and isinstance(rule, dict):
        rows = rule.get("fields") or rule.get("source_fields")
    return rows


def _dedupe_wanted_fields(
    wanted: List[Tuple[str, bool, List[str]]],
) -> List[Tuple[str, bool, List[str]]]:
    """One entry per field_name; ``required`` is True if any contributing row required it."""
    acc: Dict[str, Tuple[bool, List[str]]] = {}
    for fn, req, pre in wanted:
        if not fn:
            continue
        if fn not in acc:
            acc[fn] = (req, pre)
        else:
            r0, p0 = acc[fn]
            acc[fn] = (r0 or req, pre if req else p0)
    return [(k, acc[k][0], acc[k][1]) for k in sorted(acc.keys())]


def apply_preprocessing(field_value: str, preprocessing: List[str]) -> str:
    """Apply preprocessing steps to a string field value (same contract as key extraction)."""
    for task in preprocessing:
        t = str(task).lower()
        if t == "trim":
            field_value = field_value.strip()
        elif t == "lowercase":
            field_value = field_value.lower()
        elif t in ("uppercase", "upper"):
            field_value = field_value.upper()
    return field_value


def resolve_key_discovery_hash_field_paths(
    extraction_rules: Any,
    entity_view_config: Any,
) -> List[Tuple[str, bool, List[str]]]:
    """
    Fields participating in incremental content hash.

    When ``key_discovery_hash_property_paths`` is non-empty on the source view config,
    hash only those paths (no preprocessing unless matching extraction rules provide it).
    Otherwise fall back to ``iter_wanted_fields`` (rules + include_properties).
    """
    raw_paths: Optional[List[str]] = None
    if hasattr(entity_view_config, "key_discovery_hash_property_paths"):
        raw_paths = getattr(entity_view_config, "key_discovery_hash_property_paths", None)
    elif isinstance(entity_view_config, dict):
        raw_paths = entity_view_config.get("key_discovery_hash_property_paths")

    view_et = _view_entity_type_normalized(entity_view_config)

    if raw_paths:
        wanted: List[Tuple[str, bool, List[str]]] = []
        rule_fields: Dict[str, List[str]] = {}
        if isinstance(extraction_rules, list) and extraction_rules:
            for rule in extraction_rules:
                if not _rule_matches_view_entity_type(rule, view_et):
                    continue
                source_fields = _rule_field_rows(rule)
                if not source_fields:
                    continue
                if not isinstance(source_fields, list):
                    source_fields = [source_fields]
                for sf in source_fields:
                    fn = str(getattr(sf, "field_name", None) or (sf.get("field_name") if isinstance(sf, dict) else "") or "")
                    pre = list(getattr(sf, "preprocessing", None) or (sf.get("preprocessing") if isinstance(sf, dict) else []) or [])
                    if fn:
                        rule_fields[fn] = pre

        for path in raw_paths:
            p = str(path or "").strip()
            if not p:
                continue
            pre = rule_fields.get(p, [])
            wanted.append((p, False, pre))
        if wanted:
            return wanted

    return iter_wanted_fields(extraction_rules, entity_view_config)


def iter_wanted_fields(
    extraction_rules: Any,
    entity_view_config: Any,
) -> List[Tuple[str, bool, List[str]]]:
    """
    Field names, required flags, and preprocessing lists used to populate extraction entities.

    Only fields from extraction rules whose ``scope_filters.entity_type`` matches the
    source view's ``entity_type`` are included (when the rule declares entity types).

    Matches cohort loading in fn_dm_key_extraction ``_load_incremental_cohort_entities``.
    """
    view_et = _view_entity_type_normalized(entity_view_config)
    wanted_fields: List[Tuple[str, bool, List[str]]] = []
    if isinstance(extraction_rules, list) and extraction_rules:
        first = extraction_rules[0]
        if hasattr(first, "fields") or hasattr(first, "source_fields"):
            for rule in extraction_rules:
                if not _rule_matches_view_entity_type(rule, view_et):
                    continue
                rows = _rule_field_rows(rule)
                if rows is None:
                    continue
                if not isinstance(rows, list):
                    rows = [rows]
                for sf in rows:
                    wanted_fields.append(
                        (
                            str(getattr(sf, "field_name", "") or ""),
                            bool(getattr(sf, "required", False)),
                            list(getattr(sf, "preprocessing", []) or []),
                        )
                    )
        elif isinstance(first, dict):
            for rule in extraction_rules:
                if not _rule_matches_view_entity_type(rule, view_et):
                    continue
                for sf in (rule.get("fields") or rule.get("source_fields") or []):
                    if not isinstance(sf, dict):
                        continue
                    wanted_fields.append(
                        (
                            str(sf.get("field_name") or ""),
                            bool(sf.get("required") or False),
                            list(sf.get("preprocessing") or []),
                        )
                    )

    if not wanted_fields:
        if isinstance(entity_view_config, dict):
            props = list(entity_view_config.get("include_properties") or [])
        else:
            props = list(getattr(entity_view_config, "include_properties", []) or [])
        for p in props:
            wanted_fields.append((str(p or ""), False, []))

    return _dedupe_wanted_fields([(fn, req, pre) for fn, req, pre in wanted_fields if fn])


def _normalize_json_leaf(value: Any) -> Any:
    """JSON-serializable stable form for hashing (lists/dicts sorted where unambiguous)."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _normalize_json_leaf(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_normalize_json_leaf(v) for v in value]
    return str(value)


def build_field_map_for_hash(
    entity_props: Dict[str, Any],
    wanted_fields: List[Tuple[str, bool, List[str]]],
    logger: Any = None,
) -> Dict[str, Any]:
    """
    Map field_name -> normalized value for hashing, mirroring cohort entity population.
    Later entries win if the same field appears multiple times.
    """
    field_map: Dict[str, Any] = {}
    for field_name, required, preprocessing in wanted_fields:
        field_value = get_value_by_property_path(entity_props, field_name)
        if field_value is None:
            if required and logger is not None and hasattr(logger, "verbose"):
                logger.verbose(
                    "WARNING",
                    f"Missing required field '{field_name}' for extraction input hash",
                )
            continue
        if preprocessing:
            field_value = apply_preprocessing(str(field_value), preprocessing)
        else:
            field_value = _normalize_json_leaf(field_value)
        field_map[field_name] = field_value
    return field_map


def rules_fingerprint(extraction_rules: Any) -> str:
    """SHA-256 hex digest of canonical extraction_rules JSON (invalidates hash on rule edits)."""
    if not extraction_rules:
        return hashlib.sha256(b"[]").hexdigest()
    blobs: List[Dict[str, Any]] = []
    for r in extraction_rules:
        if hasattr(r, "model_dump"):
            blobs.append(r.model_dump(mode="json", by_alias=True))
        elif isinstance(r, dict):
            blobs.append(dict(r))
        else:
            blobs.append({"repr": repr(r)})
    blobs.sort(key=lambda d: json.dumps(d, sort_keys=True, separators=(",", ":")))
    payload = json.dumps(blobs, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def extraction_inputs_hash(
    scope_key: str,
    rules_fp: str,
    field_name_to_value: Dict[str, Any],
    *,
    workflow_scope: Optional[str] = None,
    source_view_fingerprint: Optional[str] = None,
) -> str:
    """SHA-256 hex digest of scope, rules fingerprint, and sorted field map."""
    fields_sorted = {k: field_name_to_value[k] for k in sorted(field_name_to_value)}
    if workflow_scope is not None:
        payload = {
            "hashVersion": 2,
            "workflow_scope": workflow_scope,
            "source_view_fingerprint": source_view_fingerprint or "",
            "rules_fingerprint": rules_fp,
            "fields": fields_sorted,
        }
    else:
        payload = {
            "scope_key": scope_key,
            "rules_fingerprint": rules_fp,
            "fields": fields_sorted,
        }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def compute_extraction_inputs_hash_from_entity_row(
    entity_metadata: Dict[str, Any],
    extraction_rules: Any,
    entity_view_config: Any,
    *,
    workflow_scope: Optional[str] = None,
    source_view_fingerprint: Optional[str] = None,
    logger: Any = None,
) -> str:
    """
    Hash from a key-extraction entity metadata dict (includes source field keys copied
    onto the entity before ``extract_keys``).
    """
    wanted = resolve_key_discovery_hash_field_paths(extraction_rules, entity_view_config)
    field_map = build_field_map_for_hash(entity_metadata, wanted, logger=logger)
    if hasattr(entity_view_config, "model_dump"):
        view_dict = entity_view_config.model_dump()
    elif hasattr(entity_view_config, "dict"):
        view_dict = entity_view_config.dict()
    elif isinstance(entity_view_config, dict):
        view_dict = entity_view_config
    else:
        view_dict = {}
    scope_key = scope_key_from_view_dict(view_dict)
    rf = rules_fingerprint(extraction_rules)
    if workflow_scope is not None:
        return extraction_inputs_hash(
            scope_key,
            rf,
            field_map,
            workflow_scope=workflow_scope,
            source_view_fingerprint=source_view_fingerprint,
        )
    return extraction_inputs_hash(scope_key, rf, field_map)


def resolve_source_view_config_for_entity(
    source_views: List[Any],
    entity_metadata: Dict[str, Any],
) -> Optional[Any]:
    """Match ``source_views`` entry using view_space / view_external_id / view_version on metadata."""
    vs = entity_metadata.get("view_space")
    ve = entity_metadata.get("view_external_id")
    vv = entity_metadata.get("view_version")
    if not vs or not ve or not vv:
        return None
    for svc in source_views:
        if (
            getattr(svc, "view_space", None) == vs
            and getattr(svc, "view_external_id", None) == ve
            and getattr(svc, "view_version", None) == vv
        ):
            return svc
    return None
