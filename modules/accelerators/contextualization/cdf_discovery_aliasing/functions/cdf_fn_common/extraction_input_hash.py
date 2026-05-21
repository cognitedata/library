"""
Deterministic hash of discovery source inputs (scope + view hash config + field values).

Used for incremental cohort gating: skip emitting rows when inputs match the last persisted hash in RAW.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional, Tuple

from .incremental_scope import scope_key_from_view_dict
from .property_path import get_value_by_property_path


def resolve_key_discovery_hash_field_paths(
    entity_view_config: Any,
) -> List[Tuple[str, bool, List[str]]]:
    """
    Fields participating in incremental content hash.

    Uses ``key_discovery_hash_property_paths`` when set; otherwise ``include_properties`` on the view.
    """
    raw_paths: Optional[List[str]] = None
    if hasattr(entity_view_config, "key_discovery_hash_property_paths"):
        raw_paths = getattr(entity_view_config, "key_discovery_hash_property_paths", None)
    elif isinstance(entity_view_config, dict):
        raw_paths = entity_view_config.get("key_discovery_hash_property_paths")

    if raw_paths:
        wanted: List[Tuple[str, bool, List[str]]] = []
        for path in raw_paths:
            p = str(path or "").strip()
            if p:
                wanted.append((p, False, []))
        if wanted:
            return wanted

    if isinstance(entity_view_config, dict):
        props = list(entity_view_config.get("include_properties") or [])
    else:
        props = list(getattr(entity_view_config, "include_properties", []) or [])
    return [(str(p or ""), False, []) for p in props if str(p or "").strip()]


def apply_preprocessing(field_value: str, preprocessing: List[str]) -> str:
    """Apply preprocessing steps to a string field value."""
    for task in preprocessing:
        t = str(task).lower()
        if t == "trim":
            field_value = field_value.strip()
        elif t == "lowercase":
            field_value = field_value.lower()
        elif t in ("uppercase", "upper"):
            field_value = field_value.upper()
    return field_value


def _normalize_json_leaf(value: Any) -> Any:
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


def source_view_hash_fingerprint(entity_view_config: Any) -> str:
    """SHA-256 hex digest of hash-relevant source view config (paths + include_properties)."""
    if isinstance(entity_view_config, dict):
        payload = {
            "key_discovery_hash_property_paths": entity_view_config.get(
                "key_discovery_hash_property_paths"
            ),
            "include_properties": entity_view_config.get("include_properties"),
        }
    else:
        payload = {
            "key_discovery_hash_property_paths": getattr(
                entity_view_config, "key_discovery_hash_property_paths", None
            ),
            "include_properties": getattr(entity_view_config, "include_properties", None),
        }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def extraction_inputs_hash(
    scope_key: str,
    view_fp: str,
    field_name_to_value: Dict[str, Any],
    *,
    workflow_scope: Optional[str] = None,
    source_view_fingerprint: Optional[str] = None,
) -> str:
    """SHA-256 hex digest of scope, view hash config fingerprint, and sorted field map."""
    fields_sorted = {k: field_name_to_value[k] for k in sorted(field_name_to_value)}
    if workflow_scope is not None:
        payload = {
            "hashVersion": 2,
            "workflow_scope": workflow_scope,
            "source_view_fingerprint": source_view_fingerprint or "",
            "view_hash_fingerprint": view_fp,
            "fields": fields_sorted,
        }
    else:
        payload = {
            "scope_key": scope_key,
            "view_hash_fingerprint": view_fp,
            "fields": fields_sorted,
        }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def compute_extraction_inputs_hash_from_entity_row(
    entity_metadata: Dict[str, Any],
    entity_view_config: Any,
    *,
    workflow_scope: Optional[str] = None,
    source_view_fingerprint: Optional[str] = None,
    logger: Any = None,
    extraction_rules: Any = None,
    association_pairs: Any = None,
    source_view_index: Optional[int] = None,
) -> str:
    """Hash from entity metadata and source view hash configuration.

    Legacy ``extraction_rules`` / ``association_pairs`` arguments are ignored (removed).
    """
    del extraction_rules, association_pairs, source_view_index
    wanted = resolve_key_discovery_hash_field_paths(entity_view_config)
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
    vf = source_view_hash_fingerprint(entity_view_config)
    if workflow_scope is not None:
        return extraction_inputs_hash(
            scope_key,
            vf,
            field_map,
            workflow_scope=workflow_scope,
            source_view_fingerprint=source_view_fingerprint,
        )
    return extraction_inputs_hash(scope_key, vf, field_map)


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
