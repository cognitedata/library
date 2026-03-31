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


def iter_wanted_fields(
    extraction_rules: Any,
    entity_view_config: Any,
) -> List[Tuple[str, bool, List[str]]]:
    """
    Field names, required flags, and preprocessing lists used to populate extraction entities.

    Matches cohort loading in fn_dm_key_extraction ``_load_incremental_cohort_entities``.
    """
    wanted_fields: List[Tuple[str, bool, List[str]]] = []
    if isinstance(extraction_rules, list) and extraction_rules:
        if hasattr(extraction_rules[0], "source_fields"):
            for rule in extraction_rules:
                source_fields = getattr(rule, "source_fields", None)
                if source_fields is None:
                    continue
                if not isinstance(source_fields, list):
                    source_fields = [source_fields]
                for sf in source_fields:
                    wanted_fields.append(
                        (
                            str(getattr(sf, "field_name", "") or ""),
                            bool(getattr(sf, "required", False)),
                            list(getattr(sf, "preprocessing", []) or []),
                        )
                    )
        elif isinstance(extraction_rules[0], dict):
            for rule in extraction_rules:
                for sf in (rule.get("source_fields", []) or []):
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
        for p in list(getattr(entity_view_config, "include_properties", []) or []):
            wanted_fields.append((str(p or ""), False, []))

    return [(fn, req, pre) for fn, req, pre in wanted_fields if fn]


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
        field_value = entity_props.get(field_name)
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
) -> str:
    """SHA-256 hex digest of scope, rules fingerprint, and sorted field map."""
    fields_sorted = {k: field_name_to_value[k] for k in sorted(field_name_to_value)}
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
    logger: Any = None,
) -> str:
    """
    Hash from a key-extraction entity metadata dict (includes source field keys copied
    onto the entity before ``extract_keys``).
    """
    wanted = iter_wanted_fields(extraction_rules, entity_view_config)
    field_map = build_field_map_for_hash(entity_metadata, wanted, logger=logger)
    view_dict = (
        entity_view_config.model_dump()
        if hasattr(entity_view_config, "model_dump")
        else entity_view_config.dict()
    )
    scope_key = scope_key_from_view_dict(view_dict)
    rf = rules_fingerprint(extraction_rules)
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
