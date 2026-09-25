"""Files in several instance spaces are matched only against entities in their own space."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes import Row
from cognite.client.data_classes.data_modeling import NodeId
from services.ConfigService import Config

ASSET_LINK = "diagrams.AssetLink"


def _config(file_space: str | None, target_space: str | None, state_space: str | None = "sp_state") -> Config:
    return Config.model_validate(
        {
            "parameters": {"rawDb": "db_file_annotation"},
            "data": {
                "fileView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": file_space,
                    "externalId": "CogniteFile",
                    "version": "v1",
                },
                "targetEntitiesView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": target_space,
                    "externalId": "CogniteAsset",
                    "version": "v1",
                },
                "annotationStateView": {
                    "schemaSpace": "sp_hdm",
                    "instanceSpace": state_space,
                    "externalId": "FileAnnotationState",
                    "version": "v1",
                },
                "sinkNode": {"space": "patterns", "externalId": "pattern_sink"},
            },
        }
    )


def _file_node(space: str, external_id: str) -> MagicMock:
    node = MagicMock()
    node.space = space
    node.properties = None
    node.as_id.return_value = NodeId(space, external_id)
    return node


def _launch_service(config: Config):
    import services.LaunchService as launch_service

    return launch_service.GeneralLaunchService(
        client=MagicMock(),
        config=config,
        logger=MagicMock(),
        tracker=MagicMock(),
        data_model_service=MagicMock(),
        cache_service=MagicMock(),
        annotation_service=MagicMock(),
        function_call_info={},
        rate_limit_policy=MagicMock(),
    )


def test_prepare_stores_annotation_state_next_to_the_file_when_no_state_space_is_set() -> None:
    from cognite.client.data_classes.data_modeling import Node
    from services.PrepareService import GeneralPrepareService

    file_node = Node.load(
        {
            "instanceType": "node",
            "space": "plant_a",
            "externalId": "PID-1",
            "version": 1,
            "lastUpdatedTime": 0,
            "createdTime": 0,
            "properties": {"cdf_cdm": {"CogniteFile/v1": {"tags": ["ToAnnotate"]}}},
        }
    )
    data_model_service = MagicMock()
    data_model_service.get_files_to_annotate.return_value = [file_node]
    service = GeneralPrepareService(
        MagicMock(), _config(None, None, state_space=None), MagicMock(), MagicMock(), data_model_service, {}
    )

    service.run()

    (state_apply,) = data_model_service.create_annotation_state.call_args.args[0]
    assert state_apply.space == "plant_a"


def test_launch_batches_files_per_instance_space_when_file_view_has_no_space() -> None:
    plant_a, plant_b = _file_node("plant_a", "PID-1"), _file_node("plant_b", "PID-2")

    batches = _launch_service(_config(None, None))._organize_files_for_processing([plant_b, plant_a])

    assert [(batch.file_space, batch.files) for batch in batches] == [("plant_a", [plant_a]), ("plant_b", [plant_b])]


def test_launch_keeps_a_single_batch_when_every_view_has_a_space() -> None:
    files = [_file_node("files", "PID-1"), _file_node("files", "PID-2")]

    (batch,) = _launch_service(_config("files", "assets"))._organize_files_for_processing(files)

    assert batch.file_space is None
    assert batch.files == files


def _query_page() -> MagicMock:
    page = MagicMock()
    page.__getitem__.return_value = []
    page.cursors = {"entities": None}
    return page


def _entity_space_filters(config: Config, file_space: str | None) -> list[str]:
    from services.DataModelService import GeneralDataModelService

    client = MagicMock()
    client.raw.rows.retrieve.return_value = None
    client.data_modeling.instances.sync.return_value = _query_page()
    GeneralDataModelService(config, client, MagicMock()).get_instances_entities("", None, file_space)
    return [
        str(call.args[0].with_["entities"].filter.dump()) for call in client.data_modeling.instances.sync.call_args_list
    ]


def test_match_entities_come_from_the_file_space_when_views_have_no_space() -> None:
    target_filter, file_filter = _entity_space_filters(_config(None, None), "plant_a")

    assert "'plant_a'" in target_filter
    assert "'plant_a'" in file_filter


def test_a_configured_target_space_is_shared_by_files_from_every_space() -> None:
    target_filter, file_filter = _entity_space_filters(_config(None, "shared_assets"), "plant_a")

    assert "'shared_assets'" in target_filter and "'plant_a'" not in target_filter
    assert "'plant_a'" in file_filter


def test_scope_entities_come_from_the_file_space_and_are_not_written_to_raw() -> None:
    """The entity sync cache holds the entities; a per-scope RAW row keeps only their pattern samples."""
    from services.EntityCacheService import GeneralCacheService

    client = MagicMock()
    client.raw.rows.retrieve.return_value = None
    data_model_service = MagicMock()
    data_model_service.get_instances_entities.return_value = ([], [])
    cache = GeneralCacheService(_config(None, None), client, MagicMock(log_level="INFO"))

    cache.get_entities(data_model_service, "PlantA", None, "plant_a")

    data_model_service.get_instances_entities.assert_called_once_with("PlantA", None, "plant_a")
    (row,) = [call.args[2] for call in client.raw.rows.insert.call_args_list]
    assert row.key == "pattern_samples:plant_a:PlantA"
    assert set(row.columns) == {"fingerprint", "assetPatternSamples", "filePatternSamples"}


def _edge(space: str, text: str) -> MagicMock:
    edge = MagicMock()
    edge.properties = {_config(None, None).data_model_views.core_annotation_view.as_view_id(): {"startNodeText": text}}
    edge.type.external_id = ASSET_LINK
    edge.start_node.space = space
    return edge


def test_promote_searches_each_text_in_the_space_of_the_file_it_was_found_in() -> None:
    from services.PromoteService import GeneralPromoteService

    entity_search = MagicMock()
    entity_search.find_entity.return_value = []
    cache = MagicMock()
    cache.get.return_value = None
    cache.is_ambiguous_in_memory.return_value = False
    cache.is_no_match_in_memory.return_value = False
    service = GeneralPromoteService(MagicMock(), _config(None, None), MagicMock(), MagicMock(), entity_search, cache)
    service._get_promote_candidates = MagicMock(return_value=[_edge("plant_a", "P-101"), _edge("plant_b", "P-101")])
    service._prepare_edge_update = MagicMock(return_value=(None, None))

    service.run()

    searched = sorted(call.args[2] for call in entity_search.find_entity.call_args_list)
    assert searched == ["plant_a", "plant_b"]


def test_promote_cache_does_not_return_an_entity_from_another_space() -> None:
    from services.PromoteCacheService import CacheService

    client = MagicMock()
    client.raw.rows.retrieve.return_value = Row(
        key="P-101", columns={"annotationType": ASSET_LINK, "endNode": "asset-b", "endNodeSpace": "plant_b"}
    )
    cache = CacheService(_config(None, None), client, MagicMock(), normalize_fn=lambda text, _: text)

    assert cache.get("P-101", ASSET_LINK, "plant_a") is None
    cached = cache.get("P-101", ASSET_LINK, "plant_b")
    assert cached is not None and cached.external_id == "asset-b"


def test_promote_runs_without_a_file_view_space() -> None:
    from services.EntitySearchService import EntitySearchService

    EntitySearchService(_config(None, None), MagicMock(), MagicMock())
