"""Match entities are read once per view through the sync endpoint and filtered in memory."""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes import Row
from cognite.client.data_classes.data_modeling import Node
from cognite.client.exceptions import CogniteAPIError
from services.ConfigService import Config


def _config(primary: str | None = None, secondary: str | None = None) -> Config:
    parameters: dict[str, object] = {"rawDb": "db_file_annotation", "targetEntitiesTags": ["DetectInDiagrams", "OMD"]}
    if primary:
        parameters["primaryScopeProperty"] = primary
    if secondary:
        parameters["secondaryScopeProperty"] = secondary
    return Config.model_validate(
        {
            "parameters": parameters,
            "data": {
                "fileView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "files",
                    "externalId": "CogniteFile",
                    "version": "v1",
                },
                "targetEntitiesView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "assets",
                    "externalId": "CogniteAsset",
                    "version": "v1",
                },
                "annotationStateView": {
                    "schemaSpace": "sp_hdm",
                    "instanceSpace": "files",
                    "externalId": "FileAnnotationState",
                    "version": "v1",
                },
                "sinkNode": {"space": "patterns", "externalId": "pattern_sink"},
            },
        }
    )


def _asset(external_id: str, deleted: bool = False, **properties: object) -> Node:
    node: dict[str, object] = {
        "instanceType": "node",
        "space": "assets",
        "externalId": external_id,
        "version": 1,
        "lastUpdatedTime": 0,
        "createdTime": 0,
        "properties": {} if deleted else {"cdf_cdm": {"CogniteAsset/v1": {"name": external_id, **properties}}},
    }
    if deleted:
        node["deletedTime"] = 1
    return Node.load(node)


def _page(nodes: list[Node], cursor: str = "next") -> MagicMock:
    page = MagicMock()
    page.__getitem__.return_value = nodes
    page.cursors = {"entities": cursor}
    return page


def _client(asset_pages: list[MagicMock], state: Row | None = None, cached: list | None = None) -> MagicMock:
    client = MagicMock()
    client.raw.rows.retrieve.return_value = state
    client.files.download_bytes.return_value = json.dumps(cached or []).encode()
    # The last empty page ends the asset read, the one before it the file-entity read.
    client.data_modeling.instances.sync.side_effect = [*asset_pages, _page([]), _page([])]
    return client


def _targets(config: Config, client: MagicMock, primary: str = "", secondary: str | None = None) -> list[str]:
    from services.DataModelService import GeneralDataModelService

    targets, _ = GeneralDataModelService(config, client, MagicMock()).get_instances_entities(primary, secondary, None)
    return sorted(target.external_id for target in targets)


def test_the_entity_read_filters_on_space_and_view_only() -> None:
    """Tags and scope in the read filter made an OR across containers that DMS could not page."""
    client = _client([_page([_asset("A-1", tags=["OMD"], site="S1")])])

    _targets(_config(primary="site"), client, primary="S1")

    for call in client.data_modeling.instances.sync.call_args_list:
        # Two-phase lets DMS use an index for the initial read of a hasData filter.
        assert call.args[0].with_["entities"].sync_mode == "two_phase"
        read_filter = str(call.args[0].with_["entities"].filter.dump())
        assert "hasData" in read_filter
        assert "'or'" not in read_filter and "tags" not in read_filter and "S1" not in read_filter


def test_entities_are_filtered_by_tag_and_grouped_by_scope_in_memory() -> None:
    client = _client(
        [
            _page(
                [
                    _asset("in-scope", tags=["DetectInDiagrams"], site="S1", unit="U1"),
                    _asset("other-unit", tags=["DetectInDiagrams"], site="S1", unit="U2"),
                    _asset("scope-wide", tags=["ScopeWideDetect"], site="S1", unit="U2"),
                    _asset("other-site", tags=["OMD", "ScopeWideDetect"], site="S2", unit="U1"),
                    _asset("untagged", tags=["Other"], site="S1", unit="U1"),
                ]
            )
        ]
    )

    assert _targets(_config("site", "unit"), client, "S1", "U1") == ["in-scope", "scope-wide"]


def test_an_unchanged_view_is_read_from_the_cached_file() -> None:
    cached = [["assets", "A-1", {"name": "A-1", "tags": ["OMD"]}]]
    client = _client([], state=Row("state", columns={"cursor": "c1", "batchSize": 1000}), cached=cached)

    assert _targets(_config(), client) == ["A-1"]
    assert client.files.download_bytes.called
    client.files.upload_bytes.assert_not_called()
    stored_state = client.raw.rows.insert.call_args.args[2]
    assert stored_state.columns["cursor"] == "next"


def test_changes_are_merged_into_the_cached_entities() -> None:
    """An asset that is deleted or loses its tag leaves the cache; a newly tagged one joins it."""
    cached = [
        ["assets", "deleted", {"name": "deleted", "tags": ["OMD"]}],
        ["assets", "untagged", {"name": "untagged", "tags": ["OMD"]}],
        ["assets", "kept", {"name": "kept", "tags": ["OMD"]}],
    ]
    changes = [_asset("deleted", deleted=True), _asset("untagged", tags=[]), _asset("new", tags=["DetectInDiagrams"])]
    client = _client([_page(changes)], state=Row("state", columns={"cursor": "c1", "batchSize": 1000}), cached=cached)

    assert _targets(_config(), client) == ["kept", "new"]
    stored = json.loads(client.files.upload_bytes.call_args.kwargs["content"])
    assert sorted(entity[1] for entity in stored) == ["kept", "new"]


def test_a_timed_out_page_is_read_again_smaller() -> None:
    responses: list[object] = [CogniteAPIError("Graph query timed out", code=408), _page([]), _page([])]
    limits: list[int] = []

    def sync(query):
        # The query is reused between calls, so its page size is recorded as it is sent.
        limits.append(query.with_["entities"].limit)
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    client = _client([])
    client.data_modeling.instances.sync.side_effect = sync

    _targets(_config(), client)

    assert limits[:2] == [1000, 800]


def _raw_store(client: MagicMock) -> None:
    """Makes the mocked RAW table return the rows written to it."""
    rows: dict[str, Row] = {}

    def insert(db_name: str, table_name: str, row: Row, ensure_parent: bool = False) -> None:
        rows[row.key] = row

    client.raw.rows.insert.side_effect = insert
    client.raw.rows.retrieve.side_effect = lambda db_name, table_name, key: rows.get(key)


def _scope_entities(aliases: list[str]):
    from cognite.client.data_classes.data_modeling import ViewId
    from services.EntitySyncService import EntityInstance

    view_id = ViewId("cdf_cdm", "CogniteAsset", "v1")
    return [EntityInstance("assets", alias, {view_id: {"name": alias, "aliases": [alias]}}) for alias in aliases]


def test_pattern_samples_are_reused_while_the_scope_entities_are_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    """Generating samples runs regexes over every alias, which is wasted work when nothing changed."""
    from services.EntityCacheService import GeneralCacheService

    client = MagicMock()
    _raw_store(client)
    cache = GeneralCacheService(_config(), client, MagicMock(log_level="INFO"))
    generate = MagicMock(wraps=cache._generate_tag_samples_from_entities)
    monkeypatch.setattr(cache, "_generate_tag_samples_from_entities", generate)
    data_model_service = MagicMock()

    data_model_service.get_instances_entities.return_value = (_scope_entities(["23-KA-9101"]), [])
    _, first = cache.get_entities(data_model_service, "", None, None)
    _, reused = cache.get_entities(data_model_service, "", None, None)
    assert generate.call_count == 2  # assets and files, once
    assert reused == first

    data_model_service.get_instances_entities.return_value = (_scope_entities(["23-KA-9101", "23-PB-2001"]), [])
    cache.get_entities(data_model_service, "", None, None)
    assert generate.call_count == 4


def test_a_first_read_that_outlasts_the_budget_is_stored_and_continued_next_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.EntitySyncService as entity_sync
    from services.EntitySyncService import EntitySyncIncompleteError

    monkeypatch.setattr(entity_sync, "ENTITY_SYNC_CHECKPOINT_SECONDS", 0)
    client = _client([_page([_asset("A-1", tags=["OMD"])], cursor="partial")], state=Row("state", {"batchSize": 1}))

    with pytest.raises(EntitySyncIncompleteError):
        _targets(_config(), client)

    assert client.files.upload_bytes.called
    assert client.raw.rows.insert.call_args.args[2].columns["cursor"] == "partial"
