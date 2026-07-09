"""Unit tests for config-driven CDM relation resolution and apply."""

import json
from pathlib import Path

import pytest

from inverted_index.cdm_relations import (
    hit_link_gate_reason,
    hit_passes_link_gates,
    resolve_node_from_config,
    validate_direct_relation_config,
)
from inverted_index.config_loader import _merge_direct_relation_config, load_direct_relation_preset
from inverted_index.target_driven import apply_cdm_direct_relations, apply_configured_links

_DEMO_REPORT = (
    Path(__file__).resolve().parents[2] / "local_run_results" / "demo_report.json"
)


@pytest.fixture
def dr_cfg() -> dict:
    return load_direct_relation_preset()


def test_validate_rejects_write_on_suggested_annotations(dr_cfg: dict) -> None:
    bad = {**dr_cfg, "write_on_suggested_annotations": True}
    errors = validate_direct_relation_config(bad)
    assert any("write_on_suggested_annotations" in e for e in errors)


def test_collect_direct_relation_purge_targets(dr_cfg: dict) -> None:
    from inverted_index.cdm_relations import collect_direct_relation_purge_targets

    targets = collect_direct_relation_purge_targets(dr_cfg)
    by_view = {ext_id: props for _key, ext_id, _space, props in targets}
    assert "CogniteFile" in by_view
    assert "assets" in by_view["CogniteFile"]
    assert "CogniteTimeSeries" in by_view
    assert "assets" in by_view["CogniteTimeSeries"]
    assert "equipment" in by_view["CogniteTimeSeries"]


def test_validate_rejects_legacy_link_keys(dr_cfg: dict) -> None:
    bad = {
        **dr_cfg,
        "links": {
            "bad_link": {
                **dr_cfg["links"]["file_to_asset"],
                "instance_types": ["asset"],
            }
        },
    }
    errors = validate_direct_relation_config(bad)
    assert any("instance_types" in e for e in errors)


def test_validate_subscription_rejects_asset_views(dr_cfg: dict) -> None:
    from inverted_index.cdm_relations import validate_subscription_config

    errors = validate_subscription_config(
        {"asset_views": ["CogniteAsset"]},
        views=dr_cfg.get("views"),
    )
    assert any("asset_views" in e for e in errors)


def test_all_links_define_resolve_by_incoming_view(dr_cfg: dict) -> None:
    assert not validate_direct_relation_config(dr_cfg)
    for link_key, link_cfg in dr_cfg["links"].items():
        resolve = link_cfg.get("resolve_by_incoming_view") or {}
        for view_key in link_cfg.get("incoming_views", []):
            assert view_key in resolve, f"{link_key} missing resolve for {view_key}"
            assert "forward" in resolve[view_key]
            assert "target" in resolve[view_key]


def test_resolve_node_from_incoming_instance() -> None:
    resolved = resolve_node_from_config(
        {},
        {"source": "incoming_instance"},
        incoming_space="cdf_cdm",
        incoming_external_id="ASSET_1",
    )
    assert resolved == ("cdf_cdm", "ASSET_1")


def test_resolve_node_from_cognite_file_hit(dr_cfg: dict) -> None:
    hit = {
        "reference_type": "CogniteFile",
        "reference_space": "cdf_cdm",
        "reference_external_id": "FILE_PID_12",
    }
    rules = dr_cfg["links"]["file_to_asset"]["resolve_by_incoming_view"]["asset"]["forward"]
    resolved = resolve_node_from_config(
        hit,
        rules,
        incoming_space="cdf_cdm",
        incoming_external_id="ASSET_1",
    )
    assert resolved == ("cdf_cdm", "FILE_PID_12")


def test_resolve_node_from_annotation_metadata_fallback(dr_cfg: dict) -> None:
    hit = {
        "reference_type": "CogniteDiagramAnnotation",
        "additional_metadata": {
            "file_space": "cdf_cdm",
            "linked_file_extid": "FILE_LEGACY",
        },
    }
    rules = dr_cfg["links"]["file_to_asset"]["resolve_by_incoming_view"]["asset"]["forward"]
    resolved = resolve_node_from_config(
        hit,
        rules,
        incoming_space="cdf_cdm",
        incoming_external_id="ASSET_1",
    )
    assert resolved == ("cdf_cdm", "FILE_LEGACY")


def test_resolve_node_from_equipment_metadata_hit(dr_cfg: dict) -> None:
    hit = {
        "reference_type": "CogniteEquipment",
        "reference_space": "cdf_cdm",
        "reference_external_id": "EQ-1001",
        "source_type": "asset_metadata",
    }
    rules = dr_cfg["links"]["equipment_to_asset"]["resolve_by_incoming_view"]["asset"]["forward"]
    resolved = resolve_node_from_config(
        hit,
        rules,
        incoming_space="cdf_cdm",
        incoming_external_id="ASSET_P101",
    )
    assert resolved == ("cdf_cdm", "EQ-1001")


def test_hit_passes_link_gates_respects_per_link_source_types(dr_cfg: dict) -> None:
    hit = {"source_type": "asset_metadata", "additional_metadata": {"confidence": 0.9}}
    link_cfg = dr_cfg["links"]["file_to_asset"]
    assert hit_passes_link_gates(hit, link_cfg, dr_cfg)
    equipment_link = dr_cfg["links"]["equipment_to_asset"]
    assert hit_passes_link_gates(hit, equipment_link, dr_cfg)


def test_hit_link_gate_reason_low_confidence(dr_cfg: dict) -> None:
    hit = {
        "source_type": "diagram_annotation_pattern",
        "additional_metadata": {"confidence": 0.4, "status": "Suggested"},
    }
    link_cfg = dr_cfg["links"]["file_to_asset"]
    assert hit_link_gate_reason(hit, link_cfg, dr_cfg) == "confidence"


def test_hit_passes_link_gates_skips_low_confidence(dr_cfg: dict) -> None:
    hit = {
        "source_type": "diagram_annotation_pattern",
        "additional_metadata": {"confidence": 0.4, "status": "Suggested"},
    }
    link_cfg = dr_cfg["links"]["file_to_asset"]
    assert not hit_passes_link_gates(hit, link_cfg, dr_cfg)


def test_hit_passes_link_gates_skips_disallowed_status(dr_cfg: dict) -> None:
    hit = {
        "source_type": "diagram_annotation_pattern",
        "additional_metadata": {"confidence": 0.9, "status": "Rejected"},
    }
    link_cfg = dr_cfg["links"]["file_to_asset"]
    assert not hit_passes_link_gates(hit, link_cfg, dr_cfg)


def test_config_loader_deep_merge_preserves_resolve_rules() -> None:
    merged = _merge_direct_relation_config(
        {
            "links": {
                "file_to_asset": {
                    "min_confidence": 0.8,
                }
            }
        }
    )
    resolve = merged["links"]["file_to_asset"]["resolve_by_incoming_view"]
    assert resolve["asset"]["forward"]["rules"]
    assert merged["links"]["file_to_asset"]["min_confidence"] == 0.8


def test_apply_configured_links_counts_filtered_by_confidence(dr_cfg: dict) -> None:
    hit = {
        "source_type": "diagram_annotation_pattern",
        "reference_type": "CogniteFile",
        "reference_space": "cdf_cdm",
        "reference_external_id": "FILE_PID_12",
        "additional_metadata": {
            "confidence": 0.4,
            "status": "Suggested",
            "page": 3,
        },
    }
    result = apply_configured_links(
        None,
        "ASSET_P101",
        "cdf_cdm",
        "asset",
        [hit],
        direct_relation_config=dr_cfg,
        dry_run=True,
    )
    assert result["filtered_by_confidence"] >= 1
    assert result["direct_relations_by_link"]["file_to_asset"] == 0


def test_apply_configured_links_file_to_asset_from_metadata_dry_run(dr_cfg: dict) -> None:
    hit = {
        "source_type": "asset_metadata",
        "reference_type": "CogniteFile",
        "reference_space": "springfield_instances",
        "reference_external_id": "VAL_drawing.pdf",
        "additional_metadata": {
            "instance_space": "springfield_instances",
            "source_view": "CogniteFile",
        },
        "term": "P-101A",
        "normalized_term": "p101a",
    }
    result = apply_configured_links(
        None,
        "ASSET_P101",
        "springfield_instances",
        "asset",
        [hit],
        direct_relation_config=dr_cfg,
        dry_run=True,
    )
    assert result["direct_relations_by_link"]["file_to_asset"] == 1
    pending = result["pending_applies"]
    assert pending
    assert pending[0]["property"] == "assets"
    assert pending[0]["forward_external_id"] == "VAL_drawing.pdf"
    assert pending[0]["forward_space"] == "springfield_instances"
    assert pending[0]["target_external_id"] == "ASSET_P101"


def test_apply_configured_links_file_to_asset_dry_run(dr_cfg: dict) -> None:
    hit = {
        "source_type": "diagram_annotation_pattern",
        "reference_type": "CogniteFile",
        "reference_space": "cdf_cdm",
        "reference_external_id": "FILE_PID_12",
        "additional_metadata": {
            "confidence": 0.92,
            "status": "Suggested",
            "page": 3,
            "detection_key": "page3:bbox_abc:p101a",
        },
        "term": "P-101A",
        "normalized_term": "p101a",
    }
    result = apply_configured_links(
        None,
        "ASSET_P101",
        "cdf_cdm",
        "asset",
        [hit],
        direct_relation_config=dr_cfg,
        dry_run=True,
    )
    assert result["direct_relations_by_link"]["file_to_asset"] == 1
    pending = result["pending_applies"]
    assert pending
    assert pending[0]["forward_external_id"] == "FILE_PID_12"
    assert pending[0]["target_external_id"] == "ASSET_P101"
    assert pending[0]["forward_view_external_id"] == "CogniteFile"


def test_apply_cdm_direct_relations_equipment_to_asset_from_demo_hit(dr_cfg: dict) -> None:
    if not _DEMO_REPORT.exists():
        hit = {
            "source_type": "asset_metadata",
            "reference_type": "CogniteEquipment",
            "reference_space": "cdf_cdm",
            "reference_external_id": "EQ-1001",
        }
    else:
        report = json.loads(_DEMO_REPORT.read_text(encoding="utf-8"))
        hit = report["target_driven"]["hits"][0]

    result = apply_cdm_direct_relations(
        None,
        "ASSET_P101",
        "cdf_cdm",
        "asset",
        [hit],
        direct_relation_config=dr_cfg,
        dry_run=True,
    )
    assert result["direct_relations_by_link"]["equipment_to_asset"] == 1
    pending = result["pending_applies"]
    assert pending
    assert pending[0]["link"] == "equipment_to_asset"
    assert pending[0]["forward_external_id"] == "EQ-1001"
    assert pending[0]["forward_view_external_id"] == "CogniteEquipment"
    assert pending[0]["target_external_id"] == "ASSET_P101"


def test_apply_cdm_direct_relations_multiple_links_dry_run(dr_cfg: dict) -> None:
    equipment_hit = {
        "source_type": "asset_metadata",
        "reference_type": "CogniteEquipment",
        "reference_space": "cdf_cdm",
        "reference_external_id": "EQ-1001",
    }
    file_hit = {
        "source_type": "diagram_annotation_pattern",
        "reference_type": "CogniteFile",
        "reference_space": "cdf_cdm",
        "reference_external_id": "FILE_PID_12",
        "additional_metadata": {
            "confidence": 0.92,
            "status": "Suggested",
            "page": 3,
        },
    }
    result = apply_cdm_direct_relations(
        None,
        "ASSET_P101",
        "cdf_cdm",
        "asset",
        [equipment_hit, file_hit],
        direct_relation_config=dr_cfg,
        dry_run=True,
    )
    assert result["direct_relations_by_link"]["equipment_to_asset"] == 1
    assert result["direct_relations_by_link"]["file_to_asset"] == 1
    assert len(result["pending_applies"]) == 2


def test_apply_direct_relations_batched_groups_same_forward_node() -> None:
    from unittest.mock import MagicMock

    from inverted_index.dm_apply import apply_direct_relations_batched

    client = MagicMock()
    node = MagicMock()
    node.properties = {"assets": []}
    client.data_modeling.instances.retrieve_nodes.return_value = [node]

    pending = [
        {
            "forward_space": "cdf_cdm",
            "forward_external_id": "FILE_1",
            "forward_view_space": "cdf_cdm",
            "forward_view_external_id": "CogniteFile",
            "forward_view_version": "v1",
            "property": "assets",
            "target_space": "cdf_cdm",
            "target_external_id": "ASSET_A",
            "cardinality": "list",
            "overwrite_existing": False,
            "max_list_size": 1000,
        },
        {
            "forward_space": "cdf_cdm",
            "forward_external_id": "FILE_1",
            "forward_view_space": "cdf_cdm",
            "forward_view_external_id": "CogniteFile",
            "forward_view_version": "v1",
            "property": "assets",
            "target_space": "cdf_cdm",
            "target_external_id": "ASSET_B",
            "cardinality": "list",
            "overwrite_existing": False,
            "max_list_size": 1000,
        },
    ]
    result = apply_direct_relations_batched(client, pending)
    client.data_modeling.instances.retrieve_nodes.assert_called_once()
    client.data_modeling.instances.apply.assert_called_once()
    apply_arg = client.data_modeling.instances.apply.call_args[0][0][0]
    source = apply_arg.sources[0].source
    assert getattr(source, "external_id", None) == "CogniteFile"
    assert result["direct_relations_updated"] == 1
