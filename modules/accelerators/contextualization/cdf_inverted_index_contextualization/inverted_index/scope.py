"""Match scope resolution for index build and query."""

from __future__ import annotations

from typing import Any

from inverted_index.aliases import normalized_instance_aliases
from inverted_index.extract import extract_terms_from_property, read_property_path
from inverted_index.normalize import normalize_term


def normalize_resolve_candidate(raw: Any) -> dict | None:
    """Coerce one resolve_from candidate to {path, extract_mode, extract_pattern?}."""
    if isinstance(raw, str):
        path = raw.strip()
        if not path:
            return None
        return {"path": path, "extract_mode": "whole_value"}
    if isinstance(raw, dict):
        path = str(raw.get("path", "")).strip()
        if not path:
            return None
        mode = raw.get("extract_mode", "whole_value")
        if mode == "regex":
            pattern = raw.get("extract_pattern")
            if pattern:
                return {
                    "path": path,
                    "extract_mode": "regex",
                    "extract_pattern": str(pattern),
                }
        return {"path": path, "extract_mode": "whole_value"}
    text = str(raw).strip()
    if not text:
        return None
    return {"path": text, "extract_mode": "whole_value"}


def normalize_resolve_candidates(raw: Any) -> list[dict]:
    """Coerce a level value to a list of normalized resolve candidates."""
    if raw is None:
        return []
    if isinstance(raw, list):
        out: list[dict] = []
        for item in raw:
            candidate = normalize_resolve_candidate(item)
            if candidate:
                out.append(candidate)
        return out
    candidate = normalize_resolve_candidate(raw)
    return [candidate] if candidate else []


def normalize_resolve_from(
    view_config: dict | list | str | None, levels: list[str]
) -> dict[str, list[dict]]:
    """Coerce resolve_from config to level -> [candidate dicts]."""
    if view_config is None:
        return {level: [] for level in levels}
    if isinstance(view_config, list):
        return {
            level: normalize_resolve_candidates(view_config[i] if i < len(view_config) else None)
            for i, level in enumerate(levels)
        }
    if isinstance(view_config, str):
        if len(levels) == 1:
            return {levels[0]: normalize_resolve_candidates(view_config)}
        return {level: [] for level in levels}
    if not isinstance(view_config, dict):
        return {level: [] for level in levels}

    normalized: dict[str, list[dict]] = {}
    for level in levels:
        normalized[level] = normalize_resolve_candidates(view_config.get(level))
    return normalized


def resolve_scope_level(
    instance: dict[str, Any], candidates: list[Any]
) -> tuple[str | None, str | None]:
    """Return first non-empty value from fallback candidate list."""
    excluded_aliases = normalized_instance_aliases(instance)
    for raw in candidates:
        cfg = normalize_resolve_candidate(raw)
        if not cfg:
            continue
        path = cfg["path"]
        value = read_property_path(instance, path)
        if value is None:
            continue
        if cfg.get("extract_mode") == "regex":
            matches = extract_terms_from_property(
                value,
                cfg,
                exclude_normalized_aliases=excluded_aliases,
            )
            if matches:
                return matches[0][0], path
            continue
        text = str(value).strip()
        if text and normalize_term(text) not in excluded_aliases:
            return text, path
    return None, None


def build_scope_key(scope_dict: dict[str, str], scope_config: dict) -> str:
    """Format match_scope_key from resolved dimension values."""
    template = scope_config.get("scope_key_template", "")
    try:
        return template.format(**scope_dict)
    except KeyError:
        parts = [f"{k}:{v}" for k, v in scope_dict.items()]
        return "|".join(parts)


def resolve_match_scope(
    instance: dict[str, Any],
    view_external_id: str,
    scope_config: dict,
    *,
    client: Any = None,
    linked_file: dict | None = None,
) -> tuple[str | None, dict[str, str]]:
    """Resolve match_scope_key and structured scope dict for an instance."""
    del client
    if not scope_config.get("enabled", True):
        fallback = scope_config.get("fallback_scope_key", "global")
        return fallback, {}

    levels = scope_config.get("levels") or []
    if not levels:
        if scope_config.get("strict_scope", False):
            return None, {}
        fallback = scope_config.get("fallback_scope_key", "global")
        return fallback, {}
    strict = scope_config.get("strict_scope", True)
    via_file = scope_config.get("annotation_scope_via_linked_file", False)

    view_resolve = scope_config.get("resolve_from", {}).get(view_external_id)
    default_resolve = scope_config.get("resolve_from_default", {})
    merged = {**default_resolve, **(view_resolve or {})}
    resolve_from = normalize_resolve_from(merged, levels)

    scope_values: dict[str, str] = {}
    for level in levels:
        paths = resolve_from.get(level, [])
        value, _path = resolve_scope_level(instance, paths)
        if (not value) and linked_file is not None and via_file:
            value, _path = resolve_scope_level(linked_file, paths)
        if not value:
            if strict:
                return None, {}
            continue
        scope_values[level] = value

    if strict and len(scope_values) != len(levels):
        return None, {}

    return build_scope_key(scope_values, scope_config), scope_values
