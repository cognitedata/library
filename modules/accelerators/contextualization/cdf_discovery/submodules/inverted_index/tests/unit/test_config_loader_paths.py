"""Regression tests for inverted_index config path resolution after cdf_discovery merge."""

from __future__ import annotations

from pathlib import Path

from inverted_index.cdm_relations import view_external_id
from inverted_index.config_loader import (
    build_runtime_config,
    load_direct_relation_preset,
    load_yaml_config,
    module_root,
)


def test_module_root_points_at_inverted_index_package() -> None:
    root = module_root()
    assert (root / "default.config.yaml").is_file()
    assert (root / "config" / "direct_relation.cdm_preset.yaml").is_file()
    assert root.name == "inverted_index"


def test_load_direct_relation_preset_resolves_asset_view() -> None:
    preset = load_direct_relation_preset()
    views = preset.get("views") or {}
    assert view_external_id(views, "asset") == "CogniteAsset"


def test_build_runtime_config_loads_inverted_index_defaults() -> None:
    runtime = build_runtime_config()
    dr = runtime["direct_relation_config"]
    views = dr.get("views") or {}
    assert view_external_id(views, "asset") == "CogniteAsset"
    assert runtime["subscription_config"].get("watch_view_keys") == ["asset", "file"]


def test_load_yaml_config_default_uses_inverted_index_config() -> None:
    yaml_cfg = load_yaml_config()
    assert yaml_cfg.get("index_storage_backend") == "raw"
    assert yaml_cfg.get("name") == "inverted_index_contextualization"


def test_explicit_config_path_still_works() -> None:
    cfg_path = Path(__file__).resolve().parents[2] / "default.config.yaml"
    yaml_cfg = load_yaml_config(cfg_path)
    assert yaml_cfg.get("name") == "inverted_index_contextualization"
