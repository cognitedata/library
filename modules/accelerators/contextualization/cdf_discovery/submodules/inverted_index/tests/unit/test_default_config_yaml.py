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


def _property_signature(properties: list[dict]) -> list[tuple[str, str, str]]:
    return sorted(
        (
            str(p.get("path", "")),
            str(p.get("source_type", "")),
            str(p.get("extract_pattern", "")),
        )
        for p in properties
    )


def test_default_yaml_index_field_config_matches_code_defaults() -> None:
    cfg_path = Path(__file__).resolve().parents[2] / "default.config.yaml"
    yaml_cfg = load_yaml_config(cfg_path)
    yaml_views = yaml_cfg.get("index_field_config") or []
    code_by_view = {v["view"]: v for v in INDEX_FIELD_CONFIG}

    assert len(yaml_views) == len(INDEX_FIELD_CONFIG)
    for yaml_view in yaml_views:
        view = str(yaml_view.get("view", ""))
        code_view = code_by_view[view]
        assert _property_keys(yaml_view.get("properties") or []) == _property_keys(
            code_view["properties"]
        )
        assert _property_signature(yaml_view.get("properties") or []) == _property_signature(
            code_view["properties"]
        )
        assert yaml_view.get("instance_spaces") == code_view.get("instance_spaces")
        assert (yaml_view.get("filters") or []) == code_view.get("filters")
