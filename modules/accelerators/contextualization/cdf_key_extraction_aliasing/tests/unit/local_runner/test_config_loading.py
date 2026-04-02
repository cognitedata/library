"""Tests for local_runner.config_loading (v1 scope YAML)."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
import yaml

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.local_runner import (
    config_loading as cl,
)


def _logger() -> logging.Logger:
    log = logging.getLogger("test_config_loading")
    log.setLevel(logging.DEBUG)
    return log


def _minimal_key_extraction_data(
    *,
    source_views: list | None = None,
    extraction_rules: list | None = None,
) -> dict:
    views = source_views if source_views is not None else []
    data: dict = {
        "validation": {"min_confidence": 0.5},
    }
    if extraction_rules is not None:
        data["extraction_rules"] = extraction_rules
    return {
        "schemaVersion": 1,
        "source_views": views,
        "key_extraction": {
            "externalId": "ctx_ke_test",
            "config": {"parameters": {"debug": True}, "data": data},
        },
    }


def test_scope_doc_omitted_aliasing_identity_passthrough():
    doc = _minimal_key_extraction_data(
        source_views=[
            {
                "view_external_id": "CogniteAsset",
                "view_space": "cdf_cdm",
                "view_version": "v1",
                "entity_type": "asset",
            }
        ],
        extraction_rules=[
            {
                "name": "r1",
                "method": "passthrough",
                "extraction_type": "candidate_key",
                "enabled": True,
                "priority": 1,
                "scope_filters": {"entity_type": ["asset"]},
                "parameters": {"min_confidence": 1.0},
                "source_fields": [
                    {
                        "field_name": "name",
                        "required": True,
                        "field_type": "string",
                        "priority": 1,
                        "role": "target",
                        "preprocessing": ["trim"],
                    }
                ],
                "field_selection_strategy": "first_match",
            }
        ],
    )
    ext, alias, views, *_ = cl._load_from_scope_document(_logger(), doc)
    assert len(views) == 1
    assert ext.get("source_views") == views
    assert len(ext["extraction_rules"]) == 1
    assert ext["parameters"].get("debug") is True
    assert alias["rules"] == []


def test_scope_doc_empty_aliasing_rules():
    doc = _minimal_key_extraction_data(
        source_views=[
            {
                "view_external_id": "CogniteAsset",
                "view_space": "cdf_cdm",
                "view_version": "v1",
                "entity_type": "asset",
            }
        ],
        extraction_rules=[],
    )
    doc["aliasing"] = {
        "externalId": "ctx_al_test",
        "config": {
            "parameters": {"raw_db": "db"},
            "data": {"aliasing_rules": [], "validation": {}},
        },
    }
    ext, alias, *_ = cl._load_from_scope_document(_logger(), doc)
    assert len(ext["extraction_rules"]) >= 1
    assert alias["rules"] == []


def test_scope_doc_empty_extraction_injects_per_entity_type():
    doc = _minimal_key_extraction_data(
        source_views=[
            {
                "view_external_id": "CogniteAsset",
                "view_space": "cdf_cdm",
                "view_version": "v1",
                "entity_type": "asset",
            },
            {
                "view_external_id": "CogniteTimeSeries",
                "view_space": "cdf_cdm",
                "view_version": "v1",
                "entity_type": "timeseries",
            },
        ],
        extraction_rules=[],
    )
    ext, _, views, *_ = cl._load_from_scope_document(_logger(), doc)
    assert {v["entity_type"] for v in views} == {"asset", "timeseries"}
    assert len(ext["extraction_rules"]) == 2
    for r in ext["extraction_rules"]:
        assert r.get("method") == "passthrough"


def test_load_configs_from_explicit_path(tmp_path: Path):
    doc = _minimal_key_extraction_data(
        source_views=[
            {
                "view_external_id": "CogniteAsset",
                "view_space": "cdf_cdm",
                "view_version": "v1",
                "entity_type": "asset",
            }
        ],
        extraction_rules=[],
    )
    p = tmp_path / cl.WORKFLOW_LOCAL_CONFIG_FILENAME
    p.write_text(yaml.safe_dump(doc), encoding="utf-8")
    ext, alias, *_ = cl.load_configs(_logger(), config_path=str(p))
    assert len(ext["extraction_rules"]) >= 1
    assert alias["rules"] == []


def test_load_configs_missing_default_file_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    missing = tmp_path / cl.WORKFLOW_LOCAL_CONFIG_FILENAME
    monkeypatch.setattr(cl, "DEFAULT_SCOPE_DOCUMENT_PATH", missing)
    with pytest.raises(FileNotFoundError, match="Missing default scope document"):
        cl.load_configs(_logger(), scope="default")


def test_load_configs_default_uses_module_root_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    p = tmp_path / cl.WORKFLOW_LOCAL_CONFIG_FILENAME
    doc = _minimal_key_extraction_data(
        source_views=[
            {
                "view_external_id": "CogniteAsset",
                "view_space": "cdf_cdm",
                "view_version": "v1",
                "entity_type": "asset",
                "batch_size": 42,
            }
        ],
        extraction_rules=[],
    )
    p.write_text(yaml.safe_dump(doc), encoding="utf-8")
    monkeypatch.setattr(cl, "DEFAULT_SCOPE_DOCUMENT_PATH", p)
    _, _, views, *_ = cl.load_configs(_logger(), scope="default")
    assert views[0].get("batch_size") == 42


def test_resolve_scope_document_path_non_default_requires_config_path() -> None:
    with pytest.raises(FileNotFoundError, match="Per-scope directory layout was removed"):
        cl.resolve_scope_document_path("leaf_scope")


def test_resolve_scope_document_path_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    p = tmp_path / cl.WORKFLOW_LOCAL_CONFIG_FILENAME
    p.write_text(
        "schemaVersion: 1\nsource_views:\n  - {view_external_id: CogniteAsset, view_space: cdf_cdm, view_version: v1, entity_type: asset}\nkey_extraction:\n  externalId: x\n  config:\n    parameters: {raw_table_key: t}\n    data: {extraction_rules: []}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cl, "DEFAULT_SCOPE_DOCUMENT_PATH", p)
    assert cl.resolve_scope_document_path("default") == p
    assert cl.resolve_scope_document_path() == p

def test_scope_doc_missing_top_level_source_views_raises() -> None:
    doc = {
        "schemaVersion": 1,
        "key_extraction": {
            "externalId": "ctx_ke_test",
            "config": {
                "parameters": {"debug": True},
                "data": {"validation": {"min_confidence": 0.5}, "extraction_rules": []},
            },
        },
    }
    with pytest.raises(ValueError, match="top-level source_views"):
        cl._load_from_scope_document(_logger(), doc)

