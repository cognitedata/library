"""Behavior tests for how detect results become annotation edges and RAW rows."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

from cognite.client.data_classes.data_modeling import EdgeApply, NodeId

sys.path.append(str(Path(__file__).parent))

from services.ApplyService import GeneralApplyService  # isort: skip
from services.ConfigService import Config  # isort: skip

FILE_ID = NodeId("inst_location", "file_PH-ME-P-0153-001.pdf")
LINKED_FILE = {
    "annotation_type": "diagrams.FileLink",
    "external_id": "file_PH-ME-P-0152-001.pdf",
    "resource_type": "CogniteFile",
    "space": "inst_location",
}
ASSET = {
    "annotation_type": "diagrams.AssetLink",
    "external_id": "23-DB-9101",
    "resource_type": "CogniteAsset",
    "space": "inst_location",
}


def _config(**thresholds: float) -> Config:
    return Config.model_validate(
        {
            "parameters": {"rawDb": "db_file_annotation", **thresholds},
            "data": {
                "fileView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "inst_location",
                    "externalId": "CogniteFile",
                    "version": "v1",
                },
                "targetEntitiesView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "inst_location",
                    "externalId": "CogniteAsset",
                    "version": "v1",
                },
                "annotationStateView": {
                    "schemaSpace": "sp_hdm",
                    "instanceSpace": "inst_location",
                    "externalId": "FileAnnotationState",
                    "version": "v1",
                },
                "sinkNode": {"space": "sp_dat_pattern_mode_results", "externalId": "pattern_detection_sink_node"},
            },
        }
    )


def _box(x_min: float, y_min: float, x_max: float, y_max: float) -> dict[str, object]:
    return {
        "page": 1,
        "shape": "rectangle",
        "vertices": [
            {"x": x_min, "y": y_min},
            {"x": x_max, "y": y_min},
            {"x": x_max, "y": y_max},
            {"x": x_min, "y": y_max},
        ],
    }


# The title-block detection from PH-ME-P-0153-001.pdf, and a pattern box that fully encloses it.
TITLE_BLOCK = _box(0.849889179931493, 0.9467236467236465, 0.9510376788232924, 0.9558404558404556)
ENCLOSING = _box(0.84, 0.94, 0.96, 0.96)


def _detection(entity: dict[str, str], confidence: float, region: dict[str, object] = TITLE_BLOCK) -> dict:
    return {"confidence": confidence, "entities": [entity], "region": region, "text": "PH-ME-P-0152-001"}


def _pattern(region: dict[str, object]) -> dict:
    return {"annotations": [{"entities": [LINKED_FILE], "region": region, "text": "PH-ME-P-0152-001 REV 2"}]}


def _apply(config: Config, regular: list[dict], pattern: dict | None = None) -> tuple[list[EdgeApply], MagicMock]:
    """Run the apply step for one file and return the edges written plus the mocked client."""
    client = MagicMock()
    file_node = MagicMock()
    file_node.as_id.return_value = FILE_ID
    file_node.properties = {}

    GeneralApplyService(client, config, MagicMock()).process_and_apply_annotations_for_file(
        file_node, {"annotations": regular}, pattern, clean_old=False
    )
    return client.data_modeling.instances.apply.call_args.kwargs["edges"], client


def _status_by_end_node(edges: list[EdgeApply]) -> dict[str, object]:
    return {edge.end_node.external_id: edge.sources[0].properties["status"] for edge in edges}


def _doc_doc_rows(client: MagicMock) -> list:
    return [
        row
        for call in client.raw.rows.insert.call_args_list
        if call.kwargs["table_name"] == "annotation_documents_docs"
        for row in call.kwargs["row"]
    ]


def test_an_approved_annotation_is_replaced_by_an_enclosing_pattern() -> None:
    """Keeping both would link the same text twice: once to the file, once to the sink."""
    edges, client = _apply(_config(), [_detection(LINKED_FILE, 1.0)], _pattern(ENCLOSING))

    assert "file_PH-ME-P-0152-001.pdf" not in _status_by_end_node(edges)
    assert _doc_doc_rows(client) == []


def test_a_suggested_annotation_is_replaced_by_an_enclosing_pattern() -> None:
    config = _config(assetAutoApprovalThreshold=1.0, assetAutoSuggestThreshold=0.5)

    edges, client = _apply(config, [_detection(LINKED_FILE, 0.8)], _pattern(ENCLOSING))

    assert "file_PH-ME-P-0152-001.pdf" not in _status_by_end_node(edges)
    assert _doc_doc_rows(client) == []


def test_file_links_and_asset_links_use_their_own_thresholds() -> None:
    config = _config(
        assetAutoApprovalThreshold=1.0,
        assetAutoSuggestThreshold=0.5,
        fileAutoApprovalThreshold=0.8,
        fileAutoSuggestThreshold=0.7,
    )
    asset_region = _box(0.1, 0.1, 0.2, 0.2)

    edges, _ = _apply(config, [_detection(LINKED_FILE, 0.9), _detection(ASSET, 0.9, asset_region)])

    assert _status_by_end_node(edges) == {"file_PH-ME-P-0152-001.pdf": "Approved", "23-DB-9101": "Suggested"}


def test_a_file_link_below_its_own_suggest_threshold_is_dropped() -> None:
    config = _config(assetAutoSuggestThreshold=0.5, fileAutoApprovalThreshold=1.0, fileAutoSuggestThreshold=0.7)

    edges, _ = _apply(config, [_detection(LINKED_FILE, 0.6)])

    assert edges == []


def test_file_link_thresholds_default_to_the_general_thresholds() -> None:
    apply = _config(assetAutoApprovalThreshold=0.9, assetAutoSuggestThreshold=0.6).finalize_function.apply_service

    assert (apply.file_auto_approval_threshold, apply.file_auto_suggest_threshold) == (0.9, 0.6)
