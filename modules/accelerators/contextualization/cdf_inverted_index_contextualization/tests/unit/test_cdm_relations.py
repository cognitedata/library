"""Unit tests for config-driven CDM relation resolution and apply."""

import json
from pathlib import Path

from inverted_index.cdm_relations import (
    hit_link_gate_reason,
    hit_passes_link_gates,
    resolve_node_from_config,
)
from inverted_index.config import DIRECT_RELATION_CONFIG
from inverted_index.config_loader import _merge_direct_relation_config
from inverted_index.target_driven import apply_cdm_direct_relations, apply_configured_links

_DEMO_REPORT = (
    Path(__file__).resolve().parents[2] / "local_run_results" / "demo_report.json"
)


def test_all_links_define_resolve_by_instance_type() -> None:
    for link_key, link_cfg in DIRECT_RELATION_CONFIG["links"].items():
        resolve = link_cfg.get("resolve_by_instance_type") or {}
        for instance_type in link_cfg.get("instance_types", []):
            assert instance_type in resolve, f"{link_key} missing resolve for {instance_type}"
            assert "forward" in resolve[instance_type]
            assert "target" in resolve[instance_type]


def test_resolve_node_from_incoming_instance() -> None:
    resolved = resolve_node_from_config(
        {},
        {"source": "incoming_instance"},
        incoming_space="cdf_cdm",
        incoming_external_id="ASSET_1",
    )
    assert resolved == ("cdf_cdm", "ASSET_1")


def test_resolve_node_from_cognite_file_hit() -> None:
    hit = {
        "reference_type": "CogniteFile",
        "reference_space": "cdf_cdm",
        "reference_external_id": "FILE_PID_12",
    }
    rules = DIRECT_RELATION_CONFIG["links"]["file_to_asset"]["resolve_by_instance_type"][
        "asset"
    ]["forward"]
    resolved = resolve_node_from_config(
        hit,
        rules,
        incoming_space="cdf_cdm",
        incoming_external_id="ASSET_1",
    )
    assert resolved == ("cdf_cdm", "FILE_PID_12")


def test_resolve_node_from_annotation_metadata_fallback() -> None:
    hit = {
        "reference_type": "CogniteDiagramAnnotation",
        "additional_metadata": {
            "file_space": "cdf_cdm",
            "linked_file_extid": "FILE_LEGACY",
        },
    }
    rules = DIRECT_RELATION_CONFIG["links"]["file_to_asset"]["resolve_by_instance_type"][
        "asset"
    ]["forward"]
    resolved = resolve_node_from_config(
        hit,
        rules,
        incoming_space="cdf_cdm",
        incoming_external_id="ASSET_1",
    )
    assert resolved == ("cdf_cdm", "FILE_LEGACY")


def test_resolve_node_from_equipment_metadata_hit() -> None:
    hit = {
        "reference_type": "CogniteEquipment",
        "reference_space": "cdf_cdm",
        "reference_external_id": "EQ-1001",
        "source_type": "asset_metadata",
    }
    rules = DIRECT_RELATION_CONFIG["links"]["equipment_to_asset"][
        "resolve_by_instance_type"
    ]["asset"]["forward"]
    resolved = resolve_node_from_config(
        hit,
        rules,
        incoming_space="cdf_cdm",
        incoming_external_id="ASSET_P101",
    )
    assert resolved == ("cdf_cdm", "EQ-1001")


def test_hit_passes_link_gates_respects_per_link_source_types() -> None:
    hit = {"source_type": "asset_metadata", "additional_metadata": {"confidence": 0.9}}
    link_cfg = DIRECT_RELATION_CONFIG["links"]["file_to_asset"]
    assert not hit_passes_link_gates(hit, link_cfg, DIRECT_RELATION_CONFIG)
    equipment_link = DIRECT_RELATION_CONFIG["links"]["equipment_to_asset"]
    assert hit_passes_link_gates(hit, equipment_link, DIRECT_RELATION_CONFIG)


def test_hit_link_gate_reason_low_confidence() -> None:
    hit = {
        "source_type": "diagram_annotation_pattern",
        "additional_metadata": {"confidence": 0.4, "status": "Suggested"},
    }
    link_cfg = DIRECT_RELATION_CONFIG["links"]["file_to_asset"]
    assert hit_link_gate_reason(hit, link_cfg, DIRECT_RELATION_CONFIG) == "confidence"


def test_hit_passes_link_gates_skips_low_confidence() -> None:
    hit = {
        "source_type": "diagram_annotation_pattern",
        "additional_metadata": {"confidence": 0.4, "status": "Suggested"},
    }
    link_cfg = DIRECT_RELATION_CONFIG["links"]["file_to_asset"]
    assert not hit_passes_link_gates(hit, link_cfg, DIRECT_RELATION_CONFIG)


def test_hit_passes_link_gates_skips_disallowed_status() -> None:
    hit = {
        "source_type": "diagram_annotation_pattern",
        "additional_metadata": {"confidence": 0.9, "status": "Rejected"},
    }
    link_cfg = DIRECT_RELATION_CONFIG["links"]["file_to_asset"]
    assert not hit_passes_link_gates(hit, link_cfg, DIRECT_RELATION_CONFIG)


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
    resolve = merged["links"]["file_to_asset"]["resolve_by_instance_type"]
    assert resolve["asset"]["forward"]["rules"]
    assert merged["links"]["file_to_asset"]["min_confidence"] == 0.8


def test_apply_configured_links_counts_filtered_by_confidence() -> None:
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
        dry_run=True,
    )
    assert result["filtered_by_confidence"] >= 1
    assert result["direct_relations_by_link"]["file_to_asset"] == 0


def test_apply_configured_links_file_to_asset_dry_run() -> None:
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
        dry_run=True,
    )
    assert result["direct_relations_by_link"]["file_to_asset"] == 1
    pending = result["pending_applies"]
    assert pending
    assert pending[0]["forward_external_id"] == "FILE_PID_12"
    assert pending[0]["target_external_id"] == "ASSET_P101"
    assert pending[0]["forward_view_external_id"] == "CogniteFile"


def test_apply_cdm_direct_relations_equipment_to_asset_from_demo_hit() -> None:
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
        dry_run=True,
    )
    assert result["direct_relations_by_link"]["equipment_to_asset"] == 1
    pending = result["pending_applies"]
    assert pending
    assert pending[0]["link"] == "equipment_to_asset"
    assert pending[0]["forward_external_id"] == "EQ-1001"
    assert pending[0]["forward_view_external_id"] == "CogniteEquipment"
    assert pending[0]["target_external_id"] == "ASSET_P101"


def test_apply_cdm_direct_relations_multiple_links_dry_run() -> None:
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
        dry_run=True,
    )
    assert result["direct_relations_by_link"]["equipment_to_asset"] == 1
    assert result["direct_relations_by_link"]["file_to_asset"] == 1
    assert len(result["pending_applies"]) == 2

