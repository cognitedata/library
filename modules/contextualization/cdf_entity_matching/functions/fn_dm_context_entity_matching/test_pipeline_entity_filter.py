"""Already-linked entities are filtered in Python, not by the DMS query.

`exists` counts an empty array as a value on the query endpoint that `instances.list`
uses, so `NOT exists(assets)` silently drops every entity whose link list was written as
`[]` instead of being left unset.
"""

import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes.data_modeling import DirectRelationReference  # isort: skip

from em_constants import KEY_ENTITY_EXT_ID, PROP_COL_LINK_NAME, PROP_COL_NAME  # isort: skip
from em_pipeline import get_new_entities, get_query_filter  # isort: skip
from test_submit import build_config  # isort: skip


class Instance:
    """Minimal stand-in for a node as returned by instances.list."""

    def __init__(self, space: str, external_id: str, properties: dict[Any, Any]) -> None:
        self.space = space
        self.external_id = external_id
        self.properties = properties


def _client(*entities: Instance) -> MagicMock:
    client = MagicMock()
    client.data_modeling.instances.list.return_value = list(entities)
    return client


def test_entity_with_an_empty_link_list_is_still_matched() -> None:
    config = build_config()
    view_id = config.data.entity_view.as_view_id()
    client = _client(Instance("inst_location", "ts:1", {view_id: {PROP_COL_NAME: "ts:1", PROP_COL_LINK_NAME: []}}))

    entities = get_new_entities(client, config, MagicMock())

    assert [entity[KEY_ENTITY_EXT_ID] for entity in entities] == ["ts:1"]


def test_linked_entity_is_skipped() -> None:
    config = build_config()
    view_id = config.data.entity_view.as_view_id()
    link = DirectRelationReference(space="inst_location", external_id="asset-1")
    client = _client(
        Instance("inst_location", "ts:1", {view_id: {PROP_COL_NAME: "ts:1", PROP_COL_LINK_NAME: [link]}}),
        Instance("inst_location", "ts:2", {view_id: {PROP_COL_NAME: "ts:2"}}),
    )

    entities = get_new_entities(client, config, MagicMock())

    assert [entity[KEY_ENTITY_EXT_ID] for entity in entities] == ["ts:2"]


def test_run_all_keeps_linked_entities() -> None:
    config = build_config()
    config.parameters.run_all = True
    view_id = config.data.entity_view.as_view_id()
    link = DirectRelationReference(space="inst_location", external_id="asset-1")
    client = _client(Instance("inst_location", "ts:1", {view_id: {PROP_COL_NAME: "ts:1", PROP_COL_LINK_NAME: [link]}}))

    entities = get_new_entities(client, config, MagicMock())

    assert [entity[KEY_ENTITY_EXT_ID] for entity in entities] == ["ts:1"]


def test_query_filter_does_not_filter_on_the_link_property() -> None:
    config = build_config()

    query_filter = get_query_filter(config.data.entity_view, MagicMock())

    assert query_filter is not None
    assert PROP_COL_LINK_NAME not in str(query_filter.dump())


def test_count_and_duplicate_warning_use_unmatched_entities() -> None:
    """Linked entities are gone before the count log and the duplicate-space warning."""
    config = build_config()
    config.data.entity_view.instance_spaces = ["inst_ts_a", "inst_ts_b"]
    view_id = config.data.entity_view.as_view_id()
    link = DirectRelationReference(space="inst_location", external_id="asset-1")
    logger = MagicMock()
    client = _client(
        Instance("inst_ts_a", "ts:same", {view_id: {PROP_COL_NAME: "ts:same", PROP_COL_LINK_NAME: [link]}}),
        Instance("inst_ts_b", "ts:same", {view_id: {PROP_COL_NAME: "ts:same", PROP_COL_LINK_NAME: [link]}}),
        Instance("inst_ts_a", "ts:new", {view_id: {PROP_COL_NAME: "ts:new"}}),
    )

    entities = get_new_entities(client, config, logger)

    assert [entity[KEY_ENTITY_EXT_ID] for entity in entities] == ["ts:new"]
    count_logs = [
        call.args[0] for call in logger.info.call_args_list if "Number of entities to process" in call.args[0]
    ]
    assert count_logs == [
        "Number of entities to process: 1 (skipped 2 already linked) "
        "NOTE: Rule based regular expressions are applied to the 'name' property"
    ]
    logger.warning.assert_not_called()
