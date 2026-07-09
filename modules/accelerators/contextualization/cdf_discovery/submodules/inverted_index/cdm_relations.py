"""CDM direct relation helpers and config-driven node resolution."""

from __future__ import annotations

from typing import Any, Optional

from inverted_index.extract import read_property_path

LEGACY_DIRECT_RELATION_LINK_KEYS = frozenset(
    {"instance_types", "resolve_by_instance_type"}
)
LEGACY_DIRECT_RELATION_TOP_LEVEL_KEYS = frozenset({"write_on_suggested_annotations"})
LEGACY_SUBSCRIPTION_KEYS = frozenset(
    {
        "asset_views",
        "file_views",
        "equipment_views",
        "timeseries_views",
        "default_instance_type",
    }
)


def resolve_view_key(
    views: dict,
    *,
    space: str | None = None,
    external_id: str,
) -> str | None:
    """Map DM view (space, external_id) to a configured view registry key."""
    if not views or not external_id:
        return None
    norm_space = (space or "cdf_cdm").strip()
    for key, ref in views.items():
        if not isinstance(ref, dict):
            continue
        ref_ext = str(ref.get("external_id", ""))
        ref_space = str(ref.get("space", "cdf_cdm"))
        if ref_ext == external_id and ref_space == norm_space:
            return str(key)
    for key, ref in views.items():
        if isinstance(ref, dict) and str(ref.get("external_id", "")) == external_id:
            return str(key)
    return None


def view_external_id(views: dict, view_key: str) -> str:
    ref = views.get(view_key) if isinstance(views, dict) else None
    if isinstance(ref, dict) and ref.get("external_id"):
        return str(ref["external_id"])
    return view_key


def view_space(views: dict, view_key: str, default: str = "cdf_cdm") -> str:
    ref = views.get(view_key) if isinstance(views, dict) else None
    if isinstance(ref, dict) and ref.get("space"):
        return str(ref["space"])
    return default


def watch_view_external_ids(watch_view_keys: list[str], views: dict) -> list[str]:
    """Resolve subscription watch_view_keys to DM view external IDs."""
    ids: list[str] = []
    for key in watch_view_keys:
        ext = view_external_id(views, key)
        if ext and ext not in ids:
            ids.append(ext)
    return ids


def validate_direct_relation_config(dr_cfg: dict | None) -> list[str]:
    """Return human-readable validation errors for direct_relation_config."""
    errors: list[str] = []
    if not isinstance(dr_cfg, dict):
        return ["direct_relation_config must be a mapping"]
    for legacy in LEGACY_DIRECT_RELATION_TOP_LEVEL_KEYS:
        if legacy in dr_cfg:
            errors.append(
                f"direct_relation_config: legacy field {legacy!r} is not supported; "
                "use allowed_annotation_statuses"
            )
    views = dr_cfg.get("views") or {}
    if not isinstance(views, dict):
        errors.append("direct_relation_config.views must be a mapping")
        views = {}
    links = dr_cfg.get("links") or {}
    if not isinstance(links, dict):
        errors.append("direct_relation_config.links must be a mapping")
        return errors
    for link_key, link_cfg in links.items():
        if not isinstance(link_cfg, dict):
            errors.append(f"links.{link_key} must be a mapping")
            continue
        for legacy in LEGACY_DIRECT_RELATION_LINK_KEYS:
            if legacy in link_cfg:
                errors.append(f"links.{link_key}: legacy field {legacy!r} is not supported")
        for field in ("forward_view", "target_view", "property"):
            if not link_cfg.get(field):
                errors.append(f"links.{link_key}: missing {field}")
        cardinality = link_cfg.get("cardinality", "list")
        if cardinality not in ("list", "single"):
            errors.append(f"links.{link_key}: cardinality must be list or single")
        incoming = link_cfg.get("incoming_views") or []
        if not incoming:
            errors.append(f"links.{link_key}: incoming_views must be non-empty")
        resolve_map = link_cfg.get("resolve_by_incoming_view") or {}
        if not isinstance(resolve_map, dict):
            errors.append(f"links.{link_key}: resolve_by_incoming_view must be a mapping")
            continue
        for view_key in incoming:
            if view_key not in views:
                errors.append(
                    f"links.{link_key}: incoming_views entry {view_key!r} "
                    "not found in views"
                )
            entry = resolve_map.get(view_key)
            if not isinstance(entry, dict):
                errors.append(
                    f"links.{link_key}: missing resolve_by_incoming_view.{view_key}"
                )
                continue
            if "forward" not in entry or "target" not in entry:
                errors.append(
                    f"links.{link_key}: resolve_by_incoming_view.{view_key} "
                    "requires forward and target"
                )
        fwd_key = link_cfg.get("forward_view")
        tgt_key = link_cfg.get("target_view")
        if fwd_key and fwd_key not in views:
            errors.append(f"links.{link_key}: forward_view {fwd_key!r} not in views")
        if tgt_key and tgt_key not in views:
            errors.append(f"links.{link_key}: target_view {tgt_key!r} not in views")
    return errors


def validate_subscription_config(
    sub_cfg: dict | None,
    *,
    views: dict | None = None,
) -> list[str]:
    errors: list[str] = []
    if not isinstance(sub_cfg, dict):
        return errors
    for legacy in LEGACY_SUBSCRIPTION_KEYS:
        if legacy in sub_cfg:
            errors.append(f"subscription: legacy field {legacy!r} is not supported")
    watch_keys = sub_cfg.get("watch_view_keys")
    if watch_keys is not None:
        if not isinstance(watch_keys, list) or not watch_keys:
            errors.append("subscription.watch_view_keys must be a non-empty list")
        elif views:
            for key in watch_keys:
                if key not in views:
                    errors.append(
                        f"subscription.watch_view_keys: {key!r} not found in "
                        "direct_relation_config.views"
                    )
    return errors


def file_reference_types(dr_cfg: dict, link_cfg: dict | None = None) -> list[str]:
    if link_cfg:
        raw = link_cfg.get("file_reference_types")
        if isinstance(raw, list) and raw:
            return [str(v) for v in raw]
    raw = dr_cfg.get("file_reference_types")
    if isinstance(raw, list) and raw:
        return [str(v) for v in raw]
    return ["CogniteFile"]


def collect_direct_relation_purge_targets(
    dr_cfg: dict,
) -> list[tuple[str, str, str, tuple[str, ...]]]:
    """Return (view_key, view_external_id, view_space, property_names) for forward direct-relation clears."""
    views = dr_cfg.get("views") or {}
    grouped: dict[str, set[str]] = {}
    for link_cfg in (dr_cfg.get("links") or {}).values():
        if not isinstance(link_cfg, dict):
            continue
        if not link_cfg.get("enabled", True):
            continue
        modes = link_cfg.get("write_modes") or []
        if "direct_relation" not in modes:
            continue
        forward_key = str(link_cfg.get("forward_view") or "")
        prop = link_cfg.get("property")
        if not forward_key or not prop:
            continue
        grouped.setdefault(forward_key, set()).add(str(prop))
    targets: list[tuple[str, str, str, tuple[str, ...]]] = []
    for view_key, props in sorted(grouped.items()):
        ext_id = view_external_id(views, view_key)
        space = view_space(views, view_key)
        targets.append((view_key, ext_id, space, tuple(sorted(props))))
    return targets


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
