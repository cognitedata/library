"""Files in several instance spaces are matched only against entities in their own space."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes import Row
from cognite.client.data_classes.data_modeling import NodeId
from services.config_service import Config

ASSET_LINK = "diagrams.AssetLink"


def _config(file_space: str | None, target_space: str | None, state_space: str | None = "sp_state") -> Config:
    return Config.model_validate(
        {
            "parameters": {},
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
                    "schemaSpace": "dm_sol_file_annotation",
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
    import services.launch_service as launch_service

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
    from services.prepare_service import GeneralPrepareService

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


def test_launch_groups_files_when_a_scope_value_is_missing() -> None:
    """A file with no scope text used to put None in the sort key and raise TypeError."""
    from cognite.client.data_classes.data_modeling import Node

    def scoped_file(external_id: str, properties: dict[str, object]) -> Node:
        return Node.load(
            {
                "instanceType": "node",
                "space": "files",
                "externalId": external_id,
                "version": 1,
                "lastUpdatedTime": 0,
                "createdTime": 0,
                "properties": {"cdf_cdm": {"CogniteFile/v1": properties}},
            }
        )

    present = scoped_file("with-site", {"site": "PlantA", "unit": "U100"})
    missing = scoped_file("without-site", {})
    config = _config("files", "assets")
    config.launch_function.primary_scope_property = "site"
    config.launch_function.secondary_scope_property = "unit"

    batches = _launch_service(config)._organize_files_for_processing([missing, present])

    assert [(batch.primary_scope_value, batch.secondary_scope_value) for batch in batches] == [
        ("", None),
        ("PlantA", "U100"),
    ]


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
    from services.data_model_service import GeneralDataModelService

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
    from services.entity_cache_service import GeneralCacheService

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


def _edge(space: str, text: str, file_external_id: str = "PID-1") -> MagicMock:
    edge = MagicMock()
    edge.properties = {_config(None, None).data_model_views.core_annotation_view.as_view_id(): {"startNodeText": text}}
    edge.type.external_id = ASSET_LINK
    edge.start_node.space = space
    edge.start_node.external_id = file_external_id
    return edge


def _scoped_file(external_id: str, properties: dict[str, object]) -> object:
    from cognite.client.data_classes.data_modeling import Node

    return Node.load(
        {
            "instanceType": "node",
            "space": "plant_a",
            "externalId": external_id,
            "version": 1,
            "lastUpdatedTime": 0,
            "createdTime": 0,
            "properties": {"cdf_cdm": {"CogniteFile/v1": properties}},
        }
    )


def _scope_config(*, enabled: bool, secondary: str | None = "unit") -> Config:
    config = _config(None, None)
    config.parameters.primary_scope_property = "site"
    config.parameters.secondary_scope_property = secondary
    config.parameters.pattern_promote.filter_pattern_promote_by_scope = enabled
    return config


def _run_promote(config: Config, files: list[object], edges: list[MagicMock]) -> tuple[MagicMock, MagicMock, MagicMock]:
    from services.promote_service import GeneralPromoteService

    client = MagicMock()
    client.data_modeling.instances.retrieve_nodes.return_value = files
    entity_search = MagicMock()
    entity_search.find_entity.return_value = []
    cache = MagicMock()
    cache.get.return_value = None
    cache.is_ambiguous_in_memory.return_value = False
    cache.is_no_match_in_memory.return_value = False
    logger = MagicMock()
    service = GeneralPromoteService(client, config, logger, MagicMock(), entity_search, cache)
    service._get_promote_candidates = MagicMock(return_value=edges)
    service._prepare_edge_update = MagicMock(return_value=(None, None, None))
    service.run()
    return client, entity_search, cache


def test_promote_searches_each_text_in_the_space_of_the_file_it_was_found_in() -> None:
    from services.promote_service import GeneralPromoteService

    entity_search = MagicMock()
    entity_search.find_entity.return_value = []
    cache = MagicMock()
    cache.get.return_value = None
    cache.is_ambiguous_in_memory.return_value = False
    cache.is_no_match_in_memory.return_value = False
    service = GeneralPromoteService(MagicMock(), _config(None, None), MagicMock(), MagicMock(), entity_search, cache)
    service._get_promote_candidates = MagicMock(return_value=[_edge("plant_a", "P-101"), _edge("plant_b", "P-101")])
    service._prepare_edge_update = MagicMock(return_value=(None, None, None))

    service.run()

    searched = sorted(call.args[2] for call in entity_search.find_entity.call_args_list)
    assert searched == ["plant_a", "plant_b"]


def test_promote_cache_does_not_return_an_entity_from_another_space() -> None:
    from services.promote_cache_service import CacheService

    rows = {
        "plant_b:P-101": Row(
            key="plant_b:P-101",
            columns={"annotationType": ASSET_LINK, "endNode": "asset-b", "endNodeSpace": "plant_b"},
        )
    }
    client = MagicMock()
    client.raw.rows.retrieve.side_effect = lambda db_name, table_name, key: rows.get(key)
    cache = CacheService(_config(None, None), client, MagicMock(), normalize_fn=lambda text, _: text)

    assert cache.get("P-101", ASSET_LINK, "plant_a") is None
    cached = cache.get("P-101", ASSET_LINK, "plant_b")
    assert cached is not None and cached.external_id == "asset-b"


def test_promote_cache_persistent_key_includes_space() -> None:
    """Same text in different spaces must not overwrite each other's RAW rows."""
    from services.promote_cache_service import CacheService

    client = MagicMock()
    cache = CacheService(_config(None, None), client, MagicMock(), normalize_fn=lambda text, _: text)

    node_a, node_b = MagicMock(), MagicMock()
    node_a.space, node_a.external_id = "plant_a", "asset-a"
    node_b.space, node_b.external_id = "plant_b", "asset-b"

    cache.set("P-101", ASSET_LINK, "plant_a", node_a)
    cache.set("P-101", ASSET_LINK, "plant_b", node_b)

    keys = [call.kwargs["row"].key for call in client.raw.rows.insert.call_args_list]
    assert keys == ["plant_a:P-101", "plant_b:P-101"]


def _debug_messages(logger: MagicMock) -> list[str]:
    return [call.args[0] for call in logger.debug.call_args_list if call.args]


def test_no_match_cache_log_says_pattern_check_skipped_api_search() -> None:
    """A pattern miss must not read as if the text was searched and then cached."""
    from services.promote_cache_service import CacheService
    from services.promote_service import GeneralPromoteService

    logger = MagicMock()
    cache = CacheService(_config(None, None), MagicMock(), logger, normalize_fn=lambda text, _: text)
    entity_search = MagicMock()
    entity_search.generate_text_variations.return_value = []
    service = GeneralPromoteService(MagicMock(), _config(None, None), logger, MagicMock(), entity_search, cache)

    assert service._find_entity_with_cache("12-TW-96195", ASSET_LINK, "plant_a") == []

    entity_search.find_entity.assert_not_called()
    cache_logs = [message for message in _debug_messages(logger) if "[CACHE] Cached NO_MATCH" in message]
    assert cache_logs == [
        "✓ [CACHE] Cached NO_MATCH marker for '12-TW-96195' "
        "(entityNormalizationPatterns did not match; API search skipped; in-memory only)"
    ]


def test_no_match_cache_log_says_api_search_returned_no_entity() -> None:
    from services.promote_cache_service import CacheService
    from services.promote_service import GeneralPromoteService

    logger = MagicMock()
    cache = CacheService(_config(None, None), MagicMock(), logger, normalize_fn=lambda text, _: text)
    entity_search = MagicMock()
    entity_search.generate_text_variations.return_value = ["12-TW-96195"]
    entity_search.find_entity.return_value = []
    service = GeneralPromoteService(MagicMock(), _config(None, None), logger, MagicMock(), entity_search, cache)

    assert service._find_entity_with_cache("12-TW-96195", ASSET_LINK, "plant_a") == []

    entity_search.find_entity.assert_called_once_with("12-TW-96195", ASSET_LINK, "plant_a")
    cache_logs = [message for message in _debug_messages(logger) if "[CACHE] Cached NO_MATCH" in message]
    assert cache_logs == [
        "✓ [CACHE] Cached NO_MATCH marker for '12-TW-96195' (API search returned no entity; in-memory only)"
    ]


def test_promote_runs_without_a_file_view_space() -> None:
    from services.entity_search_service import EntitySearchService

    EntitySearchService(_config(None, None), MagicMock(), MagicMock())


def test_promote_finds_entities_with_search_and_contains_any_on_aliases() -> None:
    """A /list filter on aliases is not index-backed and times out on large spaces."""
    from services.entity_search_service import EntitySearchService

    client = MagicMock()
    client.data_modeling.instances.search.return_value = []
    logger = MagicMock()
    EntitySearchService(_config(None, None), client, logger).find_entity("P-101", ASSET_LINK, "plant_a")

    variation_logs = [
        call.args[0] for call in logger.debug.call_args_list if call.args and "text variation" in call.args[0]
    ]
    assert len(variation_logs) == 1
    assert variation_logs[0].startswith("Generated 3 text variation(s) for 'P-101':")
    assert "'P-101'" in variation_logs[0]
    assert "'P_101'" in variation_logs[0]
    assert "'P101'" in variation_logs[0]
    assert not any(call.args and "text variation" in call.args[0] for call in logger.info.call_args_list)
    client.data_modeling.instances.list.assert_not_called()
    alias_call, text_call = client.data_modeling.instances.search.call_args_list
    assert alias_call.kwargs["space"] == "plant_a"
    assert alias_call.kwargs.get("query") is None
    assert alias_call.kwargs["operator"] == "AND"
    alias_filter = alias_call.kwargs["filter"].dump()["containsAny"]
    assert list(alias_filter["property"]) == ["cdf_cdm", "CogniteAsset/v1", "aliases"]
    assert "P-101" in alias_filter["values"]
    assert text_call.kwargs["query"] == "P-101"
    assert text_call.kwargs["properties"] == ["name", "description"]
    assert text_call.kwargs["operator"] == "AND"
    assert text_call.kwargs["filter"] is None
    assert text_call.kwargs["space"] == "plant_a"


def test_alias_hit_does_not_query_name_or_description() -> None:
    from services.entity_search_service import EntitySearchService

    client = MagicMock()
    client.data_modeling.instances.search.return_value = [MagicMock()]
    EntitySearchService(_config(None, None), client, MagicMock()).find_entity("P-101", ASSET_LINK, "plant_a")

    client.data_modeling.instances.search.assert_called_once()
    assert client.data_modeling.instances.search.call_args.kwargs.get("query") is None


def test_entity_search_applies_a_scope_filter_to_both_searches() -> None:
    from cognite.client.data_classes.filters import Equals
    from services.entity_search_service import EntitySearchService

    client = MagicMock()
    client.data_modeling.instances.search.return_value = []
    service = EntitySearchService(_config(None, None), client, MagicMock())
    view = service.config.data_model_views.target_entities_view
    scope_filter = Equals(view.as_property_ref("site"), "PlantA")

    service.find_entity("P-101", ASSET_LINK, "plant_a", scope_filter=scope_filter)

    alias_call, text_call = client.data_modeling.instances.search.call_args_list
    alias_dump = str(alias_call.kwargs["filter"].dump())
    assert "containsAny" in alias_dump
    assert "PlantA" in alias_dump
    assert "'and'" in alias_dump or '"and"' in alias_dump
    assert text_call.kwargs["filter"] is scope_filter
    assert text_call.kwargs["query"] == "P-101"


def test_promote_does_not_filter_by_scope_when_the_flag_is_off() -> None:
    client, entity_search, _cache = _run_promote(
        _scope_config(enabled=False),
        [],
        [_edge("plant_a", "P-101", "PID-1"), _edge("plant_a", "P-101", "PID-2")],
    )

    client.data_modeling.instances.retrieve_nodes.assert_not_called()
    entity_search.find_entity.assert_called_once_with("P-101", ASSET_LINK, "plant_a")


def test_promote_filters_matches_by_the_file_scope_when_enabled() -> None:
    """Same tag on two units is searched twice, and ScopeWideDetect stays inside the site."""
    _client, entity_search, cache = _run_promote(
        _scope_config(enabled=True),
        [
            _scoped_file("PID-1", {"site": "PlantA", "unit": "U100"}),
            _scoped_file("PID-2", {"site": "PlantA", "unit": "U200"}),
            _scoped_file("PID-3", {"site": "PlantA", "unit": "U100"}),
        ],
        [
            _edge("plant_a", "P-101", "PID-1"),
            _edge("plant_a", "P-101", "PID-2"),
            _edge("plant_a", "P-101", "PID-3"),
        ],
    )

    assert entity_search.find_entity.call_count == 2
    dumps = sorted(str(call.kwargs["scope_filter"].dump()) for call in entity_search.find_entity.call_args_list)
    assert all("PlantA" in dump and "CogniteAsset/v1" in dump for dump in dumps)
    assert any("U100" in dump and "ScopeWideDetect" in dump for dump in dumps)
    assert any("U200" in dump and "ScopeWideDetect" in dump for dump in dumps)
    scope_keys = sorted(call.kwargs["scope_key"] for call in cache.get.call_args_list)
    assert scope_keys == ["site=PlantA|unit=U100", "site=PlantA|unit=U200"]


def test_promote_scope_filter_uses_only_the_primary_property_when_secondary_is_empty() -> None:
    _client, entity_search, cache = _run_promote(
        _scope_config(enabled=True, secondary=""),
        [_scoped_file("PID-1", {"site": "PlantA"})],
        [_edge("plant_a", "P-101", "PID-1")],
    )

    dumped = str(entity_search.find_entity.call_args.kwargs["scope_filter"].dump())
    assert "PlantA" in dumped
    assert "ScopeWideDetect" not in dumped
    assert "unit" not in dumped
    assert cache.get.call_args.kwargs["scope_key"] == "site=PlantA"


def test_promote_cache_does_not_return_an_entity_from_another_scope() -> None:
    from services.promote_cache_service import CacheService

    rows = {
        "plant_a:site=PlantA:P-101": Row(
            key="plant_a:site=PlantA:P-101",
            columns={"annotationType": ASSET_LINK, "endNode": "asset-a", "endNodeSpace": "plant_a"},
        ),
        "plant_a:site=PlantB:P-101": Row(
            key="plant_a:site=PlantB:P-101",
            columns={"annotationType": ASSET_LINK, "endNode": "asset-b", "endNodeSpace": "plant_a"},
        ),
    }
    client = MagicMock()
    client.raw.rows.retrieve.side_effect = lambda db_name, table_name, key: rows.get(key)
    cache = CacheService(_config(None, None), client, MagicMock(), normalize_fn=lambda text, _: text)

    plant_a = cache.get("P-101", ASSET_LINK, "plant_a", scope_key="site=PlantA")
    plant_b = cache.get("P-101", ASSET_LINK, "plant_a", scope_key="site=PlantB")
    assert plant_a is not None and plant_a.external_id == "asset-a"
    assert plant_b is not None and plant_b.external_id == "asset-b"
    assert cache.get("P-101", ASSET_LINK, "plant_a") is None
