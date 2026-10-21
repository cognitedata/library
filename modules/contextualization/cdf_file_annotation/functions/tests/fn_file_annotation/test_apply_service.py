"""Behavior tests for how detect results become annotation edges and RAW rows."""

import json
import sys
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.data_modeling import EdgeApply, NodeId, ViewId

sys.path.append(str(Path(__file__).parent))

from fa_constants import MIN_FUZZY_SCORE  # isort: skip
from services.apply_service import GeneralApplyService  # isort: skip
from services.config_service import Config  # isort: skip

FILE_ID = NodeId("inst_location", "file_PH-ME-P-0153-001.pdf")
FILE_VIEW_ID = ViewId("cdf_cdm", "CogniteFile", "v1")
ANNOTATION_VIEW_ID = ViewId("cdf_cdm", "CogniteDiagramAnnotation", "v1")
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


def _config(**parameters: object) -> Config:
    return Config.model_validate(
        {
            "parameters": parameters,
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
                    "schemaSpace": "dm_sol_file_annotation",
                    "instanceSpace": "inst_location",
                    "externalId": "FileAnnotationState",
                    "version": "v1",
                },
                "sinkNode": {
                    "space": "inst_file_annotation_pattern_results",
                    "externalId": "pattern_detection_sink_node",
                },
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


def _existing_edge(
    region: dict[str, object] | None,
    source_user: str = "user@example.com",
    external_id: str = "manual-mapping",
) -> MagicMock:
    """An annotation edge already on the file, shaped like an instances.list result."""
    properties: dict[str, object] = {"sourceCreatedUser": source_user, "status": "Approved"}
    if region is not None:
        vertices = cast(list[dict[str, float]], region["vertices"])
        properties.update(
            {
                "startNodePageNumber": region["page"],
                "startNodeXMin": min(v["x"] for v in vertices),
                "startNodeYMin": min(v["y"] for v in vertices),
                "startNodeXMax": max(v["x"] for v in vertices),
                "startNodeYMax": max(v["y"] for v in vertices),
            }
        )
    edge = MagicMock()
    edge.space = "inst_other_app"
    edge.external_id = external_id
    edge.properties = {ANNOTATION_VIEW_ID: properties}
    return edge


def _run_apply(
    config: Config,
    regular: list[dict],
    pattern: dict | None = None,
    file_properties: dict[str, object] | None = None,
    existing: list[MagicMock] | None = None,
) -> tuple[list[EdgeApply], MagicMock, MagicMock, tuple[str, str]]:
    """Run the apply step for one file; return the edges written, the client, the logger, and the summaries."""
    client = MagicMock()
    client.data_modeling.instances.list.return_value = existing or []
    logger = MagicMock()
    file_node = MagicMock()
    file_node.as_id.return_value = FILE_ID
    file_node.properties = {FILE_VIEW_ID: file_properties or {}}

    summaries = GeneralApplyService(client, config, logger).process_and_apply_annotations_for_file(
        file_node, {"annotations": regular}, pattern, clean_old=False
    )
    return client.data_modeling.instances.apply.call_args.kwargs["edges"], client, logger, summaries


def _apply(
    config: Config,
    regular: list[dict],
    pattern: dict | None = None,
    file_properties: dict[str, object] | None = None,
    existing: list[MagicMock] | None = None,
) -> tuple[list[EdgeApply], MagicMock]:
    """Run the apply step for one file and return the edges written plus the mocked client."""
    edges, client, _, _ = _run_apply(config, regular, pattern, file_properties, existing)
    return edges, client


def _status_by_end_node(edges: list[EdgeApply]) -> dict[str, object]:
    return {edge.end_node.external_id: edge.sources[0].properties["status"] for edge in edges}


def _raw_rows(client: MagicMock, table_name: str) -> list:
    return [
        row
        for call in client.raw.rows.insert.call_args_list
        if call.kwargs["table_name"] == table_name
        for row in call.kwargs["row"]
    ]


def _doc_doc_rows(client: MagicMock) -> list:
    return _raw_rows(client, "annotation_documents_docs")


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


def test_an_enclosing_pattern_without_entities_keeps_the_annotation() -> None:
    """The pattern writes nothing for an empty result, so removing the annotation would lose the link."""
    pattern = {"annotations": [{"entities": [], "region": ENCLOSING, "text": "PH-ME-P-0152-001 REV 2"}]}

    edges, _ = _apply(_config(), [_detection(LINKED_FILE, 1.0)], pattern)

    assert _status_by_end_node(edges)["file_PH-ME-P-0152-001.pdf"] == "Approved"


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


def test_detections_without_a_region_do_not_crash_the_apply_step() -> None:
    regular = {"confidence": 1.0, "entities": [LINKED_FILE], "text": "PH-ME-P-0152-001"}
    pattern = {"annotations": [{"entities": [LINKED_FILE], "text": "PH-ME-P-0152-001"}]}

    edges, _ = _apply(_config(), [regular], pattern)

    assert _status_by_end_node(edges)["file_PH-ME-P-0152-001.pdf"] == "Approved"


def test_cleaning_old_annotations_only_targets_edges_created_by_this_function() -> None:
    """Manual and third-party annotations on the same file must survive a re-annotation.

    Cleanup must also match the legacy finalize function ID so older deployments do not leave
    orphaned duplicates.
    """
    client = MagicMock()
    file_node = MagicMock()
    file_node.as_id.return_value = FILE_ID
    file_node.properties = {}

    GeneralApplyService(client, _config(), MagicMock()).process_and_apply_annotations_for_file(
        file_node, None, None, clean_old=True
    )

    query_calls = client.data_modeling.instances.query.call_args_list
    assert len(query_calls) == 2
    client.data_modeling.instances.list.assert_not_called()
    for call in query_calls:
        dumped = json.dumps(call.args[0].with_["edges"].filter.dump())
        assert '"sourceCreatedUser"' in dumped
        assert '"fn_file_annotation"' in dumped
        assert '"fn_file_annotation_finalize"' in dumped
        # Only the ids and types of the edges are used to delete them.
        assert call.args[0].select["edges"].sources == []


def test_diagram_detect_config_sends_only_the_configured_parameters() -> None:
    detect = _config().launch_function.annotation_service.diagram_detect_config

    assert detect is not None
    assert detect.as_config().dump(camel_case=True) == {
        "connectionFlags": ["natural_reading_order", "no_text_inbetween"],
        "customizeFuzziness": {"minChars": 4},
        "directionWeights": {"left": 1.0, "right": 1.0, "up": 1.0, "down": 1.0},
        "minFuzzyScore": MIN_FUZZY_SCORE,
        "readEmbeddedText": True,
    }


def test_file_link_thresholds_default_to_the_general_thresholds() -> None:
    apply = _config(assetAutoApprovalThreshold=0.9, assetAutoSuggestThreshold=0.6).finalize_function.apply_service

    assert (apply.file_auto_approval_threshold, apply.file_auto_suggest_threshold) == (0.9, 0.6)


def test_extract_bounding_box_ignores_vertices_with_null_coordinates() -> None:
    service = GeneralApplyService(MagicMock(), _config(), MagicMock())

    box = service._extract_bounding_box_from_region(
        {"vertices": [{"x": None, "y": 0.1}, {"x": 0.2, "y": None}, {"x": 0.5, "y": 0.8}]}
    )

    # Null coordinates are dropped per axis. The remaining values are x=0.2, x=0.5 and y=0.1, y=0.8.
    assert (box.x_min, box.x_max, box.y_min, box.y_max) == (0.2, 0.5, 0.1, 0.8)


def test_document_and_asset_raw_rows_include_the_file_scope_property_values() -> None:
    """Doc, tag, and pattern RAW rows store the annotated file's configured scope values."""
    config = _config(primaryScopeProperty="site", secondaryScopeProperty="unit")
    asset_region = _box(0.1, 0.1, 0.2, 0.2)
    pattern_region = _box(0.3, 0.3, 0.4, 0.4)

    _, client = _apply(
        config,
        [_detection(LINKED_FILE, 1.0), _detection(ASSET, 1.0, asset_region)],
        _pattern(pattern_region),
        file_properties={"site": "PlantA", "unit": "U100", "sourceId": "src-1"},
    )

    rows = (
        _raw_rows(client, "annotation_documents_docs")
        + _raw_rows(client, "annotation_documents_tags")
        + _raw_rows(client, "annotation_documents_patterns")
    )
    assert rows
    for row in rows:
        assert row.columns["primaryScopeProperty"] == "PlantA"
        assert row.columns["secondaryScopeProperty"] == "U100"


INSIDE_TITLE_BLOCK = _box(0.86, 0.948, 0.94, 0.955)
PATTERN_REGION = _box(0.3, 0.3, 0.4, 0.4)
SINK = "pattern_detection_sink_node"


@pytest.mark.parametrize(
    "manual_region",
    [TITLE_BLOCK, INSIDE_TITLE_BLOCK, ENCLOSING],
    ids=["equal", "inside", "enclosing"],
)
def test_a_detection_on_top_of_a_manual_mapping_is_not_created(manual_region: dict[str, object]) -> None:
    edges, client, logger, summaries = _run_apply(
        _config(), [_detection(LINKED_FILE, 1.0)], existing=[_existing_edge(manual_region)]
    )

    assert edges == []
    assert _doc_doc_rows(client) == []
    logged = " ".join(str(call.args[0]) for call in logger.info.call_args_list)
    assert "manual-mapping" in logged
    assert "user@example.com" in logged
    assert "Skipped 1" in summaries[0]


@pytest.mark.parametrize(
    "manual_region",
    [PATTERN_REGION, _box(0.32, 0.32, 0.38, 0.38), _box(0.25, 0.25, 0.45, 0.45)],
    ids=["equal", "inside", "enclosing"],
)
def test_a_pattern_on_top_of_a_manual_mapping_is_not_created(manual_region: dict[str, object]) -> None:
    edges, client = _apply(_config(), [], _pattern(PATTERN_REGION), existing=[_existing_edge(manual_region)])

    assert SINK not in _status_by_end_node(edges)
    assert _raw_rows(client, "annotation_documents_patterns") == []


def test_a_pattern_inside_a_detection_skipped_for_a_manual_mapping_is_not_created() -> None:
    """The detection region stays taken even though the manual mapping kept it from being written."""
    # Inside the detection, but only crossing the manual box, so the manual box alone would not skip it.
    pattern = _pattern(_box(0.87, 0.947, 0.95, 0.950))

    edges, _ = _apply(_config(), [_detection(LINKED_FILE, 1.0)], pattern, existing=[_existing_edge(INSIDE_TITLE_BLOCK)])

    assert edges == []


def test_a_pattern_enclosing_a_detection_still_replaces_it_without_a_manual_mapping() -> None:
    edges, client = _apply(
        _config(), [_detection(LINKED_FILE, 1.0)], _pattern(ENCLOSING), existing=[_existing_edge(PATTERN_REGION)]
    )

    assert _status_by_end_node(edges) == {SINK: "Suggested"}
    assert _doc_doc_rows(client) == []


def test_edges_created_by_this_function_do_not_block_a_new_detection() -> None:
    existing = [
        _existing_edge(TITLE_BLOCK, source_user="fn_file_annotation"),
        _existing_edge(TITLE_BLOCK, source_user="fn_file_annotation_finalize"),
    ]

    edges, _ = _apply(_config(), [_detection(LINKED_FILE, 1.0)], existing=existing)

    assert _status_by_end_node(edges)["file_PH-ME-P-0152-001.pdf"] == "Approved"


def test_a_manual_mapping_without_a_box_does_not_block_a_detection() -> None:
    edges, _ = _apply(_config(), [_detection(LINKED_FILE, 1.0)], existing=[_existing_edge(None)])

    assert _status_by_end_node(edges)["file_PH-ME-P-0152-001.pdf"] == "Approved"


def test_a_manual_mapping_that_only_partly_overlaps_does_not_block_a_detection() -> None:
    crossing = _box(0.80, 0.90, 0.90, 0.95)

    edges, _ = _apply(_config(), [_detection(LINKED_FILE, 1.0)], existing=[_existing_edge(crossing)])

    assert _status_by_end_node(edges)["file_PH-ME-P-0152-001.pdf"] == "Approved"


def test_a_manual_mapping_on_another_page_does_not_block_a_detection() -> None:
    other_page = {**TITLE_BLOCK, "page": 2}

    edges, _ = _apply(_config(), [_detection(LINKED_FILE, 1.0)], existing=[_existing_edge(other_page)])

    assert _status_by_end_node(edges)["file_PH-ME-P-0152-001.pdf"] == "Approved"


def test_existing_annotations_are_read_from_every_space_with_their_properties() -> None:
    _, client = _apply(_config(), [_detection(LINKED_FILE, 1.0)])

    kwargs = client.data_modeling.instances.list.call_args.kwargs
    assert kwargs["instance_type"] == "edge"
    assert kwargs["sources"] == [ANNOTATION_VIEW_ID]
    assert kwargs.get("space") is None
    dumped = json.dumps(kwargs["filter"].dump())
    assert FILE_ID.external_id in dumped
