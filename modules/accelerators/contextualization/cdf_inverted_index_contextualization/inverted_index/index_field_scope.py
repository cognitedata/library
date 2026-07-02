"""Scoped property override resolution for index_field_config views."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from inverted_index.scope import build_scope_key, parse_scope_key

ScopeOverrideResolution = Literal["default", "exact", "wildcard", "ambiguous"]
ScopeOverrideMode = Literal["merge", "replace"]


@dataclass
class ScopePropertyOverrideResult:
    resolution: ScopeOverrideResolution
    properties: list[dict]
    matched_key: str | None = None
    mode: ScopeOverrideMode | None = None
    matching_keys: list[str] = field(default_factory=list)


def property_rule_key(prop_cfg: dict) -> tuple[str, str]:
    return (
        str(prop_cfg.get("path", "")),
        str(prop_cfg.get("source_type", "asset_metadata")),
    )


def normalize_scope_properties_entry(raw: Any) -> dict:
    """Coerce list shorthand or {mode, properties} object."""
    if isinstance(raw, list):
        return {"mode": "merge", "properties": [dict(p) for p in raw if isinstance(p, dict)]}
    if isinstance(raw, dict):
        mode = raw.get("mode", "merge")
        if mode not in ("merge", "replace"):
            mode = "merge"
        props = raw.get("properties") or []
        return {
            "mode": mode,
            "properties": [dict(p) for p in props if isinstance(p, dict)],
        }
    return {"mode": "merge", "properties": []}


def _parse_scope_segments(key: str) -> dict[str, str]:
    segments: dict[str, str] = {}
    for part in str(key or "").split("|"):
        part = part.strip()
        if ":" not in part:
            continue
        level, _, value = part.partition(":")
        level = level.strip()
        value = value.strip()
        if level:
            segments[level] = value
    return segments


def _configured_levels(scope_config: dict, instance_levels: dict[str, str] | None) -> list[str]:
    levels = scope_config.get("levels") or []
    if levels:
        return [str(level) for level in levels]
    if instance_levels:
        return list(instance_levels.keys())
    return []


def pattern_wildcard_tier(pattern_key: str, scope_config: dict) -> int:
    """Higher tier = less specific (more wildcards / missing levels)."""
    key = str(pattern_key or "").strip()
    if not key or key == "*":
        return 10_000
    segments = _parse_scope_segments(key)
    levels = scope_config.get("levels") or list(segments.keys())
    if not levels:
        return sum(1 for value in segments.values() if value == "*")
    return sum(1 for level in levels if segments.get(level, "*") == "*")


def scope_pattern_matches_instance(
    pattern_key: str,
    match_scope_key: str,
    scope_config: dict,
) -> bool:
    """Return True when pattern_key matches the instance scope (exact or per-level *)."""
    pattern = str(pattern_key or "").strip()
    instance_key = str(match_scope_key or "").strip()
    if not pattern or not instance_key:
        return False
    if pattern == instance_key:
        return True
    if pattern == "*":
        return True
    fallback = str(scope_config.get("fallback_scope_key") or "").strip()
    if fallback and pattern == fallback:
        return True

    instance_levels = parse_scope_key(instance_key, scope_config)
    if instance_levels is None:
        instance_levels = _parse_scope_segments(instance_key)
    pattern_levels = _parse_scope_segments(pattern)
    levels = _configured_levels(scope_config, instance_levels)
    if not levels:
        return pattern == instance_key or pattern == "*"

    for level in levels:
        pat_val = pattern_levels.get(level, "*")
        inst_val = instance_levels.get(level, "")
        if pat_val != "*" and pat_val != inst_val:
            return False
    return True


def iter_scope_override_lookup_tiers(match_scope_key: str, scope_config: dict) -> list[int]:
    """Ordered wildcard tiers to try after exact match (least to most specific wildcards)."""
    levels = scope_config.get("levels") or []
    if not levels:
        return [1]
    return list(range(1, len(levels) + 1))


def _normalize_property_cfg(prop: dict) -> dict:
    cfg = dict(prop)
    pattern = str(cfg.get("extract_pattern") or "").strip()
    if pattern and cfg.get("extract_mode") != "regex":
        cfg["extract_mode"] = "regex"
    return cfg


def _merge_properties(defaults: list[dict], overrides: list[dict]) -> list[dict]:
    merged: dict[tuple[str, str], dict] = {
        property_rule_key(p): _normalize_property_cfg(p) for p in defaults
    }
    for prop in overrides:
        merged[property_rule_key(prop)] = _normalize_property_cfg(prop)
    default_keys = [property_rule_key(p) for p in defaults]
    result: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for key in default_keys:
        if key in merged:
            result.append(merged[key])
            seen.add(key)
    for key, prop in merged.items():
        if key not in seen:
            result.append(prop)
    return result


def _apply_override_mode(defaults: list[dict], entry: dict) -> tuple[list[dict], ScopeOverrideMode]:
    mode: ScopeOverrideMode = entry.get("mode", "merge")
    scoped = entry.get("properties") or []
    if mode == "replace":
        return [_normalize_property_cfg(p) for p in scoped], mode
    return _merge_properties(defaults, scoped), mode


def resolve_scope_property_override(
    view_config: dict,
    match_scope_key: str | None,
    scope_config: dict,
) -> ScopePropertyOverrideResult:
    defaults = [dict(p) for p in view_config.get("properties") or []]
    overrides_map = view_config.get("properties_by_scope") or {}
    scope_key = str(match_scope_key or "").strip()

    if not overrides_map or not scope_key:
        return ScopePropertyOverrideResult(
            resolution="default",
            properties=[_normalize_property_cfg(p) for p in defaults],
            mode=None,
        )

    if scope_key in overrides_map:
        entry = normalize_scope_properties_entry(overrides_map[scope_key])
        props, mode = _apply_override_mode(defaults, entry)
        return ScopePropertyOverrideResult(
            resolution="exact",
            properties=props,
            matched_key=scope_key,
            mode=mode,
        )

    for tier in iter_scope_override_lookup_tiers(scope_key, scope_config):
        matching_keys = sorted(
            key
            for key in overrides_map
            if key != scope_key
            and pattern_wildcard_tier(key, scope_config) == tier
            and scope_pattern_matches_instance(key, scope_key, scope_config)
        )
        if len(matching_keys) == 1:
            matched_key = matching_keys[0]
            entry = normalize_scope_properties_entry(overrides_map[matched_key])
            props, mode = _apply_override_mode(defaults, entry)
            return ScopePropertyOverrideResult(
                resolution="wildcard",
                properties=props,
                matched_key=matched_key,
                mode=mode,
                matching_keys=matching_keys,
            )
        if len(matching_keys) > 1:
            return ScopePropertyOverrideResult(
                resolution="ambiguous",
                properties=[],
                matching_keys=matching_keys,
            )

    catch_all_key = "*"
    fallback_key = str(scope_config.get("fallback_scope_key") or "").strip()
    for global_key in (catch_all_key, fallback_key):
        if global_key and global_key in overrides_map:
            entry = normalize_scope_properties_entry(overrides_map[global_key])
            props, mode = _apply_override_mode(defaults, entry)
            return ScopePropertyOverrideResult(
                resolution="wildcard",
                properties=props,
                matched_key=global_key,
                mode=mode,
            )

    return ScopePropertyOverrideResult(
        resolution="default",
        properties=[_normalize_property_cfg(p) for p in defaults],
        mode=None,
    )


def resolve_effective_properties(
    view_config: dict,
    match_scope_key: str | None,
    scope_config: dict,
) -> list[dict]:
    return resolve_scope_property_override(
        view_config, match_scope_key, scope_config
    ).properties


def collect_view_config_property_paths(view_cfg: dict) -> list[str]:
    """Union default and scoped property paths for DM watch / select lists."""
    paths: list[str] = []
    seen: set[str] = set()
    for prop in view_cfg.get("properties") or []:
        if isinstance(prop, dict):
            path = str(prop.get("path", "")).strip()
            if path and path not in seen:
                seen.add(path)
                paths.append(path)
    overrides_map = view_cfg.get("properties_by_scope") or {}
    for raw_entry in overrides_map.values():
        entry = normalize_scope_properties_entry(raw_entry)
        for prop in entry.get("properties") or []:
            if isinstance(prop, dict):
                path = str(prop.get("path", "")).strip()
                if path and path not in seen:
                    seen.add(path)
                    paths.append(path)
    return paths


def build_example_scope_key(scope_config: dict, wildcard_suffix: bool = True) -> str:
    """Build a placeholder scope key from scope_config levels (for UI hints)."""
    levels = scope_config.get("levels") or []
    if not levels:
        return str(scope_config.get("fallback_scope_key") or "global")
    values = {
        level: ("*" if wildcard_suffix and level == levels[-1] else f"example_{level}")
        for level in levels
    }
    return build_scope_key(values, scope_config)
