"""Unit tests for custom edge and diagram annotation upsert helpers."""

from unittest.mock import MagicMock

from inverted_index.config import DIRECT_RELATION_CONFIG
from inverted_index.edge_links import build_custom_edge_apply, upsert_diagram_annotation


def test_build_custom_edge_apply_shape() -> None:
    edge_view = DIRECT_RELATION_CONFIG["edge_views"]["file_asset_link"]
    edge_apply = build_custom_edge_apply(
        edge_view_cfg=edge_view,
        start_space="cdf_cdm",
        start_external_id="FILE_1",
        end_space="cdf_cdm",
        end_external_id="ASSET_1",
    )
    assert edge_apply.external_id
    assert edge_apply.start_node.external_id == "FILE_1"
    assert edge_apply.end_node.external_id == "ASSET_1"


def test_upsert_diagram_annotation_dry_run_create() -> None:
    hit = {
        "source_type": "diagram_annotation_pattern",
        "reference_type": "CogniteFile",
        "reference_space": "cdf_cdm",
        "reference_external_id": "FILE_PID_12",
        "term": "P-101A",
        "normalized_term": "p101a",
        "additional_metadata": {
            "page": 3,
            "bbox": [0.1, 0.2, 0.3, 0.4],
            "confidence": 0.9,
            "status": "Suggested",
            "detection_key": "page3:bbox_abc:p101a",
        },
    }
    ann_cfg = DIRECT_RELATION_CONFIG["links"]["file_to_asset"]["diagram_annotation"]
    outcome = upsert_diagram_annotation(
        None,
        hit,
        start_space="cdf_cdm",
        start_external_id="FILE_PID_12",
        end_space="cdf_cdm",
        end_external_id="ASSET_P101",
        diagram_annotation_cfg=ann_cfg,
        dr_cfg=DIRECT_RELATION_CONFIG,
        dry_run=True,
    )
    assert outcome == "created"


def test_upsert_diagram_annotation_updates_existing_end_node() -> None:
    client = MagicMock()
    existing = MagicMock()
    existing.end_node = MagicMock(space="cdf_cdm", external_id="OLD_ASSET")
    client.data_modeling.instances.retrieve_edges.return_value = [existing]

    hit = {
        "source_type": "diagram_annotation_pattern",
        "reference_type": "CogniteFile",
        "reference_space": "cdf_cdm",
        "reference_external_id": "FILE_PID_12",
        "term": "P-101A",
        "normalized_term": "p101a",
        "additional_metadata": {
            "annotation_external_id": "ann_existing",
            "page": 3,
            "confidence": 0.9,
            "status": "Suggested",
        },
    }
    ann_cfg = DIRECT_RELATION_CONFIG["links"]["file_to_asset"]["diagram_annotation"]
    outcome = upsert_diagram_annotation(
        client,
        hit,
        start_space="cdf_cdm",
        start_external_id="FILE_PID_12",
        end_space="cdf_cdm",
        end_external_id="ASSET_P101",
        diagram_annotation_cfg=ann_cfg,
        dr_cfg=DIRECT_RELATION_CONFIG,
        dry_run=False,
    )
    assert outcome == "updated"
    client.data_modeling.instances.apply.assert_called_once()
