"""CDM direct relation helpers and config-driven node resolution."""

from __future__ import annotations

from typing import Any, Optional

from inverted_index.extract import read_property_path


def merge_direct_relation_list(
    existing: list,
    target_space: str,
    target_external_id: str,
    max_list_size: int = 1000,
) -> tuple[list, bool]:
    """Idempotent append for list-valued CDM direct relations."""
    ref = {"space": target_space, "externalId": target_external_id}
    normalized_existing = [
        {"space": r.get("space"), "externalId": r.get("externalId")}
        if isinstance(r, dict)
        else r
        for r in (existing or [])
    ]
    if ref in normalized_existing:
        return existing or [], False
    if len(normalized_existing) >= max_list_size:
        return existing or [], False
    return (existing or []) + [ref], True


def merge_direct_relation_single(
    current: Optional[dict],
    target_space: str,
    target_external_id: str,
    overwrite: bool = False,
) -> tuple[Optional[dict], str]:
    """Set or skip single direct relation (CogniteEquipment.asset)."""
    ref = {"space": target_space, "externalId": target_external_id}
    if not current:
        return ref, "set"
    if overwrite:
        return ref, "overwrite"
    return current, "already_linked"


def _read_hit_path(hit: dict, path: str | None, default: Any = None) -> Any:
    if not path:
        return default
    if path == "reference_space":
        return hit.get("reference_space", default)
    if path == "reference_external_id":
        return hit.get("reference_external_id", default)
    value = read_property_path(hit, path)
    return default if value is None or value == "" else value


def _resolve_from_rules(
    hit: dict,
    rules: list[dict],
    *,
    default_space: str,
) -> tuple[str, str] | None:
    ref_type = hit.get("reference_type", "")
    for rule in rules:
        allowed = rule.get("when_reference_types")
        if allowed and ref_type not in allowed:
            continue
        space = _read_hit_path(hit, rule.get("space"), default_space)
        ext_id = _read_hit_path(hit, rule.get("external_id"))
        if ext_id:
            return str(space or default_space), str(ext_id)
        fallback = rule.get("fallback") or {}
        space = _read_hit_path(hit, fallback.get("space"), default_space)
        ext_id = _read_hit_path(hit, fallback.get("external_id"))
        if ext_id:
            return str(space or default_space), str(ext_id)
    return None


def resolve_node_from_config(
    hit: dict,
    resolve_cfg: dict | None,
    *,
    incoming_space: str,
    incoming_external_id: str,
    default_space: str = "cdf_cdm",
) -> tuple[str, str] | None:
    """Resolve (space, external_id) from hit config or incoming instance."""
    if not resolve_cfg:
        return None
    if resolve_cfg.get("source") == "incoming_instance":
        return incoming_space, incoming_external_id
    rules = resolve_cfg.get("rules")
    if rules:
        return _resolve_from_rules(hit, rules, default_space=default_space)
    return None


def hit_link_gate_reason(hit: dict, link_cfg: dict, dr_cfg: dict) -> str | None:
    """Return a gate failure reason, or None when the hit may be linked."""
    allowed_source_types = link_cfg.get("source_types") or dr_cfg.get("source_types") or []
    if allowed_source_types and hit.get("source_type") not in allowed_source_types:
        return "source_type"
    min_conf = link_cfg.get("min_confidence", dr_cfg.get("min_confidence", 0.6))
    meta = hit.get("additional_metadata") or {}
    conf = meta.get("confidence")
    if conf is not None and float(conf) < float(min_conf):
        return "confidence"
    require_status = link_cfg.get(
        "require_annotation_status", dr_cfg.get("require_annotation_status")
    )
    status = meta.get("status")
    if require_status and status != require_status:
        return "require_annotation_status"
    allowed_statuses = set(
        link_cfg.get("allowed_annotation_statuses")
        or dr_cfg.get("allowed_annotation_statuses")
        or ["Suggested", "Approved"]
    )
    if status and allowed_statuses and status not in allowed_statuses:
        return "annotation_status"
    return None


def hit_passes_link_gates(hit: dict, link_cfg: dict, dr_cfg: dict) -> bool:
    """Per-link source_type, confidence, and annotation status gates."""
    return hit_link_gate_reason(hit, link_cfg, dr_cfg) is None
