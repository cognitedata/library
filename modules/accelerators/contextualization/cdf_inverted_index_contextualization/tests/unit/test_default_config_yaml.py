"""Ensure default.config.yaml index_field_config matches code defaults."""

from __future__ import annotations

from pathlib import Path

from inverted_index.config import INDEX_FIELD_CONFIG
from inverted_index.config_loader import load_yaml_config


def _property_keys(properties: list[dict]) -> set[tuple[str, str]]:
    return {
        (str(p.get("path", "")), str(p.get("source_type", "")))
        for p in properties
    }


def test_default_yaml_index_field_config_matches_code_defaults() -> None:
    cfg_path = Path(__file__).resolve().parents[2] / "default.config.yaml"
    yaml_cfg = load_yaml_config(cfg_path)
    yaml_views = yaml_cfg.get("index_field_config") or []
    code_file = next(v for v in INDEX_FIELD_CONFIG if v["view"] == "CogniteFile")
    yaml_file = next(v for v in yaml_views if v.get("view") == "CogniteFile")

    code_keys = _property_keys(code_file["properties"])
    yaml_keys = _property_keys(yaml_file.get("properties") or [])

    assert yaml_keys == code_keys
    assert yaml_file.get("instance_spaces") == code_file.get("instance_spaces")
    assert yaml_file.get("filters") == code_file.get("filters")
