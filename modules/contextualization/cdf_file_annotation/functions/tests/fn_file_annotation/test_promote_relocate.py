"""Promote moves successfully promoted pattern edges into the file instance space."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes.data_modeling import DirectRelationReference, EdgeApply, EdgeId, NodeOrEdgeData
from services.config_service import Config
from services.promote_service import GeneralPromoteService, MatchedEntity

ASSET_LINK = "diagrams.AssetLink"


def _config(*, asset_suggest: float = 0.6, file_suggest: float | None = None) -> Config:
    parameters: dict[str, object] = {
        "assetAutoSuggestThreshold": asset_suggest,
        "assetAutoApprovalThreshold": max(asset_suggest, 0.8),
    }
    if file_suggest is not None:
        parameters["fileAutoSuggestThreshold"] = file_suggest
        parameters["fileAutoApprovalThreshold"] = max(file_suggest, 0.8)
    return Config.model_validate(
        {
            "parameters": parameters,
            "data": {
                "fileView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "plant_a",
                    "externalId": "CogniteFile",
                    "version": "v1",
                },
                "targetEntitiesView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "plant_a",
                    "externalId": "CogniteAsset",
                    "version": "v1",
                },
                "annotationStateView": {
                    "schemaSpace": "dm_sol_file_annotation",
                    "instanceSpace": "sp_state",
                    "externalId": "FileAnnotationState",
                    "version": "v1",
                },
                "sinkNode": {"space": "patterns", "externalId": "pattern_sink"},
            },
        }
    )


def test_prepare_edge_update_relocates_promoted_edge_to_file_instance_space() -> None:
    config = _config()
    view_id = config.data_model_views.core_annotation_view.as_view_id()
    edge_apply = EdgeApply(
        space="patterns",
        external_id="pattern:file:tag",
        type=DirectRelationReference(space="cdf_cdm", external_id=ASSET_LINK),
        start_node=DirectRelationReference(space="plant_a", external_id="PID-1"),
        end_node=DirectRelationReference(space="patterns", external_id="pattern_sink"),
        existing_version=3,
        sources=[
            NodeOrEdgeData(
                source=view_id,
                properties={
                    "startNodeText": "12-TW-96195",
                    "confidence": 1,
                    "sourceCreatedUser": "fn_file_annotation",
                    "tags": [],
                },
            )
        ],
    )
    edge = MagicMock()
    edge.space = "patterns"
    edge.external_id = "pattern:file:tag"
    edge.start_node.space = "plant_a"
    edge.start_node.external_id = "PID-1"
    edge.type.external_id = ASSET_LINK
    edge.properties = {
        view_id: {
            "startNodeText": "12-TW-96195",
            "confidence": 1,
            "sourceCreatedUser": "fn_file_annotation",
            "tags": [],
        }
    }
    edge.as_write.return_value = edge_apply

    client = MagicMock()
    client.raw.rows.retrieve.return_value = None
    service = GeneralPromoteService(client, config, MagicMock(), MagicMock(), MagicMock(), MagicMock())

    updated, raw_row, relocate = service._prepare_edge_update(
        edge, [MatchedEntity(space="plant_a", external_id="asset-1")]
    )

    assert relocate == EdgeId("patterns", "pattern:file:tag")
    assert updated is not None
    assert updated.space == "plant_a"
    assert updated.end_node.external_id == "asset-1"
    # Creating in a new space must not carry the pattern-space version (causes DMS version conflict).
    assert updated.existing_version is None
    # Relocate is a create: keep the original properties, not only status/tags.
    assert updated.sources is not None
    props = updated.sources[0].properties or {}
    assert props.get("startNodeText") == "12-TW-96195"
    assert props.get("confidence") == 1
    assert props.get("sourceCreatedUser") == "fn_file_annotation"
    assert props.get("status") == "Approved"
    assert "PromotedAuto" in (props.get("tags") or [])
    assert raw_row is not None


def test_run_deletes_pattern_space_copy_when_edge_is_relocated() -> None:
    config = _config()
    view_id = config.data_model_views.core_annotation_view.as_view_id()
    edge_apply = EdgeApply(
        space="plant_a",
        external_id="pattern:file:tag",
        type=DirectRelationReference(space="cdf_cdm", external_id=ASSET_LINK),
        start_node=DirectRelationReference(space="plant_a", external_id="PID-1"),
        end_node=DirectRelationReference(space="plant_a", external_id="asset-1"),
        sources=[
            NodeOrEdgeData(
                source=view_id,
                properties={"startNodeText": "P-101", "status": "Approved", "tags": ["PromotedAuto"]},
            )
        ],
    )
    edge = MagicMock()
    edge.space = "patterns"
    edge.external_id = "pattern:file:tag"
    edge.start_node.space = "plant_a"
    edge.start_node.external_id = "PID-1"
    edge.type.external_id = ASSET_LINK
    edge.properties = {view_id: {"startNodeText": "P-101", "tags": []}}

    client = MagicMock()
    entity_search = MagicMock()
    entity_search.find_entity.return_value = [MagicMock(space="plant_a", external_id="asset-1")]
    entity_search.generate_text_variations.return_value = ["P-101"]
    cache = MagicMock()
    cache.get.return_value = None
    cache.is_ambiguous_in_memory.return_value = False
    cache.is_no_match_in_memory.return_value = False

    service = GeneralPromoteService(client, config, MagicMock(), MagicMock(), entity_search, cache)
    service._get_promote_candidates = MagicMock(return_value=[edge])
    service._find_entity_with_cache = MagicMock(return_value=[MatchedEntity(space="plant_a", external_id="asset-1")])
    service._prepare_edge_update = MagicMock(return_value=(edge_apply, None, EdgeId("patterns", "pattern:file:tag")))

    service.run()

    client.data_modeling.instances.apply.assert_called_once_with(edges=[edge_apply])
    client.data_modeling.instances.delete.assert_called_once_with(edges=[EdgeId("patterns", "pattern:file:tag")])


def test_prepare_ambiguous_creates_one_suggested_edge_with_alternatives_in_description() -> None:
    config = _config(asset_suggest=0.6)
    view_id = config.data_model_views.core_annotation_view.as_view_id()
    edge = MagicMock()
    edge.space = "patterns"
    edge.external_id = "pattern:file:23-ESDV-92501-A:assetlink:abc"
    edge.start_node = DirectRelationReference(space="plant_a", external_id="PID-1")
    edge.type = DirectRelationReference(space="cdf_cdm", external_id=ASSET_LINK)
    edge.properties = {
        view_id: {
            "startNodeText": "23-ESDV-92501-A",
            "confidence": 1,
            "sourceCreatedUser": "fn_file_annotation",
            "status": "Suggested",
            "tags": [],
        }
    }

    client = MagicMock()
    client.raw.rows.retrieve.return_value = None
    service = GeneralPromoteService(client, config, MagicMock(), MagicMock(), MagicMock(), MagicMock())

    apply, raw_row, delete_id = service._prepare_ambiguous_edge(
        edge,
        [
            MatchedEntity(space="plant_a", external_id="23-ESDV-92501-A-1"),
            MatchedEntity(space="plant_a", external_id="23-ESDV-92501-A-2"),
        ],
    )

    assert delete_id == EdgeId("patterns", "pattern:file:23-ESDV-92501-A:assetlink:abc")
    assert apply is not None
    assert apply.space == "plant_a"
    assert apply.external_id == edge.external_id
    assert apply.existing_version is None
    assert apply.end_node.external_id == "23-ESDV-92501-A-1"
    assert apply.sources is not None
    props = apply.sources[0].properties or {}
    assert props.get("status") == "Suggested"
    assert props.get("confidence") == 0.6
    assert props.get("startNodeText") == "23-ESDV-92501-A"
    assert props.get("description") == "AmbiguousMatch alternatives: plant_a/23-ESDV-92501-A-2"
    assert "AmbiguousMatch" in (props.get("tags") or [])
    assert "PromoteAttempted" in (props.get("tags") or [])
    assert raw_row is not None
    assert raw_row.key == edge.external_id
    assert raw_row.columns.get("confidence") == 0.6
    assert raw_row.columns.get("endNode") == "23-ESDV-92501-A-1"


def test_prepare_ambiguous_rejects_when_all_candidates_are_self_references() -> None:
    """Unusable ambiguous matches must be rejected so they are not re-queued forever."""
    config = _config()
    view_id = config.data_model_views.core_annotation_view.as_view_id()
    edge_apply = EdgeApply(
        space="patterns",
        external_id="pattern:file:PID-1:assetlink:abc",
        type=DirectRelationReference(space="cdf_cdm", external_id=ASSET_LINK),
        start_node=DirectRelationReference(space="plant_a", external_id="PID-1"),
        end_node=DirectRelationReference(space="patterns", external_id="pattern_sink"),
        sources=[
            NodeOrEdgeData(
                source=view_id,
                properties={
                    "startNodeText": "PID-1",
                    "confidence": 1,
                    "status": "Suggested",
                    "tags": [],
                },
            )
        ],
    )
    edge = MagicMock()
    edge.space = "patterns"
    edge.external_id = "pattern:file:PID-1:assetlink:abc"
    edge.start_node = DirectRelationReference(space="plant_a", external_id="PID-1")
    edge.type = DirectRelationReference(space="cdf_cdm", external_id=ASSET_LINK)
    edge.properties = {
        view_id: {
            "startNodeText": "PID-1",
            "confidence": 1,
            "status": "Suggested",
            "tags": [],
        }
    }
    edge.as_write.return_value = edge_apply

    client = MagicMock()
    client.raw.rows.retrieve.return_value = None
    service = GeneralPromoteService(client, config, MagicMock(), MagicMock(), MagicMock(), MagicMock())
    assert service.delete_rejected_edges is True

    apply, raw_row, delete_id = service._prepare_ambiguous_edge(
        edge,
        [
            MatchedEntity(space="plant_a", external_id="PID-1"),
            MatchedEntity(space="plant_a", external_id="PID-1"),
        ],
    )

    assert apply is None
    assert delete_id == EdgeId("patterns", "pattern:file:PID-1:assetlink:abc")
    assert raw_row is not None
    assert raw_row.columns.get("status") == "Rejected"
    assert "PromoteAttempted" in (raw_row.columns.get("tags") or [])


def test_prepare_ambiguous_rejects_without_delete_when_delete_rejected_disabled() -> None:
    config = _config()
    view_id = config.data_model_views.core_annotation_view.as_view_id()
    edge_apply = EdgeApply(
        space="patterns",
        external_id="pattern:file:PID-1:assetlink:abc",
        type=DirectRelationReference(space="cdf_cdm", external_id=ASSET_LINK),
        start_node=DirectRelationReference(space="plant_a", external_id="PID-1"),
        end_node=DirectRelationReference(space="patterns", external_id="pattern_sink"),
        sources=[
            NodeOrEdgeData(
                source=view_id,
                properties={"startNodeText": "PID-1", "status": "Suggested", "tags": []},
            )
        ],
    )
    edge = MagicMock()
    edge.space = "patterns"
    edge.external_id = "pattern:file:PID-1:assetlink:abc"
    edge.start_node = DirectRelationReference(space="plant_a", external_id="PID-1")
    edge.type = DirectRelationReference(space="cdf_cdm", external_id=ASSET_LINK)
    edge.properties = {view_id: {"startNodeText": "PID-1", "status": "Suggested", "tags": []}}
    edge.as_write.return_value = edge_apply

    client = MagicMock()
    client.raw.rows.retrieve.return_value = None
    service = GeneralPromoteService(client, config, MagicMock(), MagicMock(), MagicMock(), MagicMock())
    service.delete_rejected_edges = False

    apply, raw_row, delete_id = service._prepare_ambiguous_edge(
        edge,
        [
            MatchedEntity(space="plant_a", external_id="PID-1"),
            MatchedEntity(space="plant_a", external_id=""),
        ],
    )

    assert delete_id is None
    assert apply is not None
    assert apply.sources is not None
    props = apply.sources[0].properties or {}
    assert props.get("status") == "Rejected"
    assert "PromoteAttempted" in (props.get("tags") or [])
    assert raw_row is not None
    assert raw_row.columns.get("status") == "Rejected"


def test_run_applies_ambiguous_edge_and_deletes_sink_edge() -> None:
    config = _config()
    view_id = config.data_model_views.core_annotation_view.as_view_id()
    edge = MagicMock()
    edge.space = "patterns"
    edge.external_id = "pattern:file:tag"
    edge.start_node.space = "plant_a"
    edge.start_node.external_id = "PID-1"
    edge.type.external_id = ASSET_LINK
    edge.properties = {view_id: {"startNodeText": "P-101", "tags": []}}

    candidate = EdgeApply(
        space="plant_a",
        external_id="pattern:file:tag",
        type=DirectRelationReference(space="cdf_cdm", external_id=ASSET_LINK),
        start_node=DirectRelationReference(space="plant_a", external_id="PID-1"),
        end_node=DirectRelationReference(space="plant_a", external_id="asset-a"),
        sources=[NodeOrEdgeData(source=view_id, properties={"status": "Suggested", "confidence": 0.6})],
    )

    client = MagicMock()
    service = GeneralPromoteService(client, config, MagicMock(), MagicMock(), MagicMock(), MagicMock())
    service._get_promote_candidates = MagicMock(return_value=[edge])
    service._find_entity_with_cache = MagicMock(
        return_value=[
            MatchedEntity(space="plant_a", external_id="asset-a"),
            MatchedEntity(space="plant_a", external_id="asset-b"),
        ]
    )
    service._prepare_ambiguous_edge = MagicMock(return_value=(candidate, None, EdgeId("patterns", "pattern:file:tag")))

    service.run()

    client.data_modeling.instances.apply.assert_called_once_with(edges=[candidate])
    client.data_modeling.instances.delete.assert_called_once_with(edges=[EdgeId("patterns", "pattern:file:tag")])
