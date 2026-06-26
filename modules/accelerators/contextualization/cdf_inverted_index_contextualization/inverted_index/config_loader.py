"""Load module configuration from default.config.yaml and environment."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from inverted_index.config import (
    ANNOTATION_INDEX_CONFIG,
    DIRECT_RELATION_CONFIG,
    INDEX_FIELD_CONFIG,
    INDEX_STORAGE_CONFIG,
    SCOPE_CONFIG,
    SUBSCRIPTION_CONFIG,
)

_MODULE_ROOT = Path(__file__).resolve().parent.parent


def load_yaml_config(path: Path | None = None) -> dict[str, Any]:
    cfg_path = path or (_MODULE_ROOT / "default.config.yaml")
    if not cfg_path.exists():
        return {}
    with cfg_path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data if isinstance(data, dict) else {}


def _merge_scope(yaml_scope: dict | None) -> dict:
    scope = dict(SCOPE_CONFIG)
    if not isinstance(yaml_scope, dict):
        return scope
    for key in (
        "enabled",
        "levels",
        "scope_key_template",
        "strict_scope",
        "fallback_scope_key",
        "resolve_from",
        "resolve_from_default",
        "annotation_scope_via_linked_file",
    ):
        if key in yaml_scope:
            scope[key] = yaml_scope[key]
    return scope


def _deep_merge_dict(base: dict, override: dict) -> dict:
    merged = dict(base)
    for key, val in override.items():
        if isinstance(val, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dict(merged[key], val)
        else:
            merged[key] = val
    return merged


def _merge_direct_relation_config(yaml_dr: dict | None) -> dict:
    direct_rel = dict(DIRECT_RELATION_CONFIG)
    if not isinstance(yaml_dr, dict):
        return direct_rel
    for key, val in yaml_dr.items():
        if key == "links" and isinstance(val, dict):
            base_links = dict(direct_rel.get("links") or {})
            for link_key, link_val in val.items():
                if isinstance(link_val, dict) and isinstance(base_links.get(link_key), dict):
                    base_links[link_key] = _deep_merge_dict(base_links[link_key], link_val)
                else:
                    base_links[link_key] = link_val
            direct_rel["links"] = base_links
        elif key == "edge_views" and isinstance(val, dict):
            direct_rel["edge_views"] = _deep_merge_dict(
                dict(direct_rel.get("edge_views") or {}), val
            )
        else:
            direct_rel[key] = val
    return direct_rel


def build_runtime_config(yaml_cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    y = yaml_cfg or load_yaml_config()
    storage = dict(INDEX_STORAGE_CONFIG)
    backend = os.getenv("INDEX_STORAGE_BACKEND") or y.get(
        "index_storage_backend", storage.get("backend", "raw")
    )
    storage["backend"] = backend
    raw_db = os.getenv("INDEX_RAW_DATABASE") or y.get(
        "index_raw_database", storage["raw"]["database"]
    )
    storage["raw"] = {**storage["raw"], "database": raw_db}

    scope = _merge_scope(y.get("scope"))
    if isinstance(y.get("scope_levels"), list) and y["scope_levels"]:
        scope["levels"] = y["scope_levels"]
        scope["enabled"] = True

    index_fields = y.get("index_field_config") or INDEX_FIELD_CONFIG
    annotation_cfg = {**ANNOTATION_INDEX_CONFIG, **(y.get("annotation_index_config") or {})}
    subscription_cfg = {**SUBSCRIPTION_CONFIG, **(y.get("subscription") or {})}

    instance_spaces_env = os.getenv("INDEX_INSTANCE_SPACES", "").strip()
    if instance_spaces_env:
        instance_spaces = [s.strip() for s in instance_spaces_env.split(",") if s.strip()]
    else:
        raw_spaces = y.get("instance_spaces")
        if isinstance(raw_spaces, list) and raw_spaces:
            instance_spaces = [str(s) for s in raw_spaces]
        else:
            sub_spaces = subscription_cfg.get("instance_spaces")
            instance_spaces = (
                [str(s) for s in sub_spaces]
                if isinstance(sub_spaces, list) and sub_spaces
                else None
            )

    direct_rel = _merge_direct_relation_config(y.get("direct_relation_config"))

    return {
        "storage_config": storage,
        "scope_config": scope,
        "index_field_config": index_fields,
        "annotation_index_config": annotation_cfg,
        "subscription_config": subscription_cfg,
        "direct_relation_config": direct_rel,
        "instance_spaces": instance_spaces,
        "yaml": y,
    }
