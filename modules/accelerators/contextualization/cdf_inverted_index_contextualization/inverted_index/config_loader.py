"""Load module configuration from default.config.yaml and environment."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from inverted_index.cdm_relations import (
    validate_direct_relation_config,
    validate_subscription_config,
)
from inverted_index.config import (
    ANNOTATION_INDEX_CONFIG,
    INDEX_FIELD_CONFIG,
    INDEX_STORAGE_CONFIG,
    RAW_TERM_PARTITION_POLICY,
    SCOPE_CONFIG,
    SUBSCRIPTION_CONFIG,
    TARGET_DRIVEN_CONFIG,
    VIRTUAL_TAG_CREATION_CONFIG,
)

_MODULE_ROOT = Path(__file__).resolve().parent.parent
_DIRECT_RELATION_PRESET_PATH = _MODULE_ROOT / "config" / "direct_relation.cdm_preset.yaml"


def load_yaml_config(path: Path | None = None) -> dict[str, Any]:
    cfg_path = path or (_MODULE_ROOT / "default.config.yaml")
    if not cfg_path.exists():
        return {}
    with cfg_path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data if isinstance(data, dict) else {}


def load_direct_relation_preset() -> dict[str, Any]:
    if not _DIRECT_RELATION_PRESET_PATH.exists():
        return {"enabled": True, "views": {}, "links": {}}
    with _DIRECT_RELATION_PRESET_PATH.open(encoding="utf-8") as fh:
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
    direct_rel = load_direct_relation_preset()
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
        elif key == "views" and isinstance(val, dict):
            direct_rel["views"] = _deep_merge_dict(dict(direct_rel.get("views") or {}), val)
        elif key == "edge_views" and isinstance(val, dict):
            direct_rel["edge_views"] = _deep_merge_dict(
                dict(direct_rel.get("edge_views") or {}), val
            )
        else:
            direct_rel[key] = val
    return direct_rel


def _merge_subscription(yaml_sub: dict | None) -> dict:
    sub = dict(SUBSCRIPTION_CONFIG)
    if isinstance(yaml_sub, dict):
        sub.update(yaml_sub)
    return sub


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
    target_driven_cfg = {**TARGET_DRIVEN_CONFIG, **(y.get("target_driven") or {})}
    subscription_cfg = _merge_subscription(y.get("subscription"))
    if "watch_property" not in (y.get("subscription") or {}):
        subscription_cfg["watch_property"] = target_driven_cfg.get(
            "query_property", "aliases"
        )

    instance_spaces_env = os.getenv("INDEX_INSTANCE_SPACES", "").strip()
    if instance_spaces_env:
        instance_spaces = [s.strip() for s in instance_spaces_env.split(",") if s.strip()]
    else:
        raw_spaces = y.get("instance_spaces")
        instance_spaces = (
            [str(s) for s in raw_spaces]
            if isinstance(raw_spaces, list) and raw_spaces
            else None
        )

    direct_rel = _merge_direct_relation_config(y.get("direct_relation_config"))

    dr_errors = validate_direct_relation_config(direct_rel)
    sub_errors = validate_subscription_config(
        subscription_cfg, views=direct_rel.get("views") or {}
    )
    config_errors = dr_errors + sub_errors
    if config_errors:
        raise ValueError(
            "Invalid configuration:\n" + "\n".join(f"  - {e}" for e in config_errors)
        )

    virtual_tag_cfg = _deep_merge_dict(
        dict(VIRTUAL_TAG_CREATION_CONFIG),
        y.get("virtual_tag_creation") or {},
    )

    term_partition = {**RAW_TERM_PARTITION_POLICY, **(y.get("index_raw_term_partition") or {})}
    storage["term_partition"] = term_partition

    return {
        "storage_config": storage,
        "term_partition_config": term_partition,
        "scope_config": scope,
        "index_field_config": index_fields,
        "annotation_index_config": annotation_cfg,
        "target_driven_config": target_driven_cfg,
        "subscription_config": subscription_cfg,
        "direct_relation_config": direct_rel,
        "virtual_tag_creation_config": virtual_tag_cfg,
        "instance_spaces": instance_spaces,
        "yaml": y,
    }
