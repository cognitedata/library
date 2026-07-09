"""Unit tests for inverted-index WorkflowTrigger generation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_SCRIPTS = _MODULE_ROOT / "scripts"
for _path in (_MODULE_ROOT, _SCRIPTS):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from inverted_index.config import INDEX_FIELD_CONFIG  # noqa: E402
from inverted_index.config_loader import build_runtime_config  # noqa: E402
from inverted_index_build.workflow_triggers import (  # noqa: E402
    build_all_trigger_docs,
    build_watermark_trigger_doc,
    canonical_trigger_text,
    check_inverted_index_triggers,
    generate_inverted_index_triggers,
    resolve_watch_specs_for_source_index,
    resolve_watch_specs_for_subscription,
)

VIEWS = {
    "file": {"space": "cdf_cdm", "external_id": "CogniteFile", "version": "v1"},
    "asset": {"space": "cdf_cdm", "external_id": "CogniteAsset", "version": "v1"},
    "equipment": {"space": "cdf_cdm", "external_id": "CogniteEquipment", "version": "v1"},
    "timeseries": {"space": "cdf_cdm", "external_id": "CogniteTimeSeries", "version": "v1"},
    "custom_tag": {
        "space": "custom_space",
        "external_id": "CustomTagView",
        "version": "v1",
    },
}


def _default_runtime() -> dict:
    return build_runtime_config()


def test_source_metadata_default_views() -> None:
    runtime = _default_runtime()
    specs = resolve_watch_specs_for_source_index(runtime)
    keys = {spec.view_key for spec in specs}
    assert keys == {"file", "asset", "equipment", "timeseries"}
    ts = next(spec for spec in specs if spec.view_key == "timeseries")
    assert "aliases" in ts.properties


def test_source_metadata_custom_view() -> None:
    runtime = _default_runtime()
    runtime["index_field_config"] = [
        *INDEX_FIELD_CONFIG,
        {
            "view": "CustomTagView",
            "view_space": "custom_space",
            "version": "v1",
            "instance_spaces": [],
            "properties": [{"path": "name", "source_type": "asset_metadata"}],
        },
    ]
    runtime["direct_relation_config"] = {
        **runtime["direct_relation_config"],
        "views": {**VIEWS},
    }
    runtime["source_index_config"] = {
        **runtime["source_index_config"],
        "watch_view_keys": ["file", "custom_tag"],
    }
    specs = resolve_watch_specs_for_source_index(runtime)
    assert {spec.view_key for spec in specs} == {"file", "custom_tag"}
    custom = next(spec for spec in specs if spec.view_key == "custom_tag")
    assert custom.external_id == "CustomTagView"
    assert custom.space == "custom_space"


def test_subscription_watch_keys_subset() -> None:
    runtime = _default_runtime()
    runtime["subscription_config"] = {
        **runtime["subscription_config"],
        "watch_view_keys": ["file"],
    }
    specs = resolve_watch_specs_for_subscription(runtime)
    assert len(specs) == 1
    assert specs[0].view_key == "file"


def test_subscription_properties_from_target_driven() -> None:
    runtime = _default_runtime()
    runtime["target_driven_config"] = {
        **runtime["target_driven_config"],
        "query_property": "aliases",
        "query_property_fallbacks": ["name"],
        "exclude_empty_aliases": False,
    }
    specs = resolve_watch_specs_for_subscription(runtime)
    assert specs[0].properties == ("aliases", "name")


def test_watermark_trigger_static_shape() -> None:
    doc = build_watermark_trigger_doc()
    assert doc["triggerRule"]["triggerType"] == "schedule"
    assert doc["triggerRule"]["cronExpression"] == "{{ source_index_watermark_cron }}"
    assert "dataModelingQuery" not in doc["triggerRule"]


def test_check_detects_manual_edit(tmp_path: Path) -> None:
    config_path = _MODULE_ROOT / "submodules/inverted_index" / "default.config.yaml"
    workflows_dir = tmp_path / "workflows"
    generate_inverted_index_triggers(
        config_path=config_path,
        workflows_dir=workflows_dir,
        overwrite=True,
    )
    trigger_path = workflows_dir / "trigger_source_metadata_file.WorkflowTrigger.yaml"
    text = trigger_path.read_text(encoding="utf-8")
    trigger_path.write_text(text.replace("CogniteFile", "MutatedFile"), encoding="utf-8")
    errors = check_inverted_index_triggers(
        config_path=config_path,
        workflows_dir=workflows_dir,
    )
    assert any("trigger_source_metadata_file" in err for err in errors)


def test_build_all_trigger_docs_keys() -> None:
    docs = build_all_trigger_docs(_default_runtime())
    assert "trigger_source_index_watermark.WorkflowTrigger.yaml" in docs
    assert "trigger_source_metadata_file.WorkflowTrigger.yaml" in docs
    assert "trigger_source_metadata_asset.WorkflowTrigger.yaml" in docs
    assert "trigger_target_driven_file.WorkflowTrigger.yaml" in docs
    assert "trigger_target_driven_asset.WorkflowTrigger.yaml" in docs
    metadata = docs["trigger_source_metadata_timeseries.WorkflowTrigger.yaml"]
    select_keys = metadata["triggerRule"]["dataModelingQuery"]["select"].keys()
    assert list(select_keys) == ["timeseries"]
    assert list(metadata["triggerRule"]["dataModelingQuery"]["with"].keys()) == ["timeseries"]


def test_each_trigger_has_single_select_key() -> None:
    docs = build_all_trigger_docs(_default_runtime())
    for rel_name, doc in docs.items():
        if "dataModelingQuery" not in doc.get("triggerRule", {}):
            continue
        query = doc["triggerRule"]["dataModelingQuery"]
        assert len(query["select"]) == 1, rel_name
        assert len(query["with"]) == 1, rel_name
        assert set(query["select"]) == set(query["with"]), rel_name


def test_canonical_trigger_text_is_stable() -> None:
    runtime = _default_runtime()
    docs = build_all_trigger_docs(runtime)
    first = canonical_trigger_text(docs["trigger_target_driven_file.WorkflowTrigger.yaml"])
    second = canonical_trigger_text(docs["trigger_target_driven_file.WorkflowTrigger.yaml"])
    assert first == second
    assert "{{ subscription_batch_size }}" in first
