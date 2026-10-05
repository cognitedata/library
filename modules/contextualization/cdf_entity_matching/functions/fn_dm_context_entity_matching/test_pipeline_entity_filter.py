"""Reading the entities, and filtering already-linked ones in Python rather than in DMS.

`exists` counts an empty array as a value on the query endpoint, so `NOT exists(assets)`
silently drops every entity whose link list was written as `[]` instead of being left unset.
"""

import re
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from cognite.client.exceptions import CogniteAPIError
from tenacity import wait_none

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes.data_modeling import DirectRelationReference  # isort: skip

from em_constants import (  # isort: skip
    COL_KEY_RULE_REGEXP_ENTITY,
    ENTITY_PAGE_SIZE,
    KEY_ENTITY_EXT_ID,
    KEY_RULE,
    KEY_RULE_KEYS,
    PROP_COL_LINK_NAME,
    PROP_COL_NAME,
)
from em_pipeline import get_new_entities, get_query_filter, list_instances_by_external_id_direct  # isort: skip
from em_pipeline_optimizations import RobustAPIClient  # isort: skip
from test_submit import build_config  # isort: skip


class Instance:
    """Minimal stand-in for a node as returned by a query."""

    def __init__(self, space: str, external_id: str, properties: dict[Any, Any]) -> None:
        self.space = space
        self.external_id = external_id
        self.properties = properties


class Page:
    """Stand-in for a query result: one page of nodes and the cursor to the next."""

    def __init__(self, nodes: list[Any], cursor: str | None = None) -> None:
        self.nodes = nodes
        self.cursors = {"entities": cursor}

    def __getitem__(self, key: str) -> list[Any]:
        return self.nodes


def _client(*entities: Instance) -> MagicMock:
    client = MagicMock()
    client.data_modeling.instances.query.return_value = Page(list(entities))
    return client


def test_entities_are_read_a_page_at_a_time_until_the_cursor_runs_out() -> None:
    config = build_config()
    view_id = config.data.entity_view.as_view_id()
    first = [
        Instance("inst_location", f"ts:{i}", {view_id: {PROP_COL_NAME: f"ts:{i}"}}) for i in range(ENTITY_PAGE_SIZE)
    ]
    client = MagicMock()
    client.data_modeling.instances.query.side_effect = [
        Page(first, "c1"),
        Page([Instance("inst_location", "ts:last", {view_id: {PROP_COL_NAME: "ts:last"}})]),
    ]

    entities = get_new_entities(client, config, MagicMock())

    assert len(entities) == ENTITY_PAGE_SIZE + 1
    assert client.data_modeling.instances.query.call_args_list[1].args[0].cursors == {"entities": "c1"}


def test_only_the_properties_matching_uses_are_read() -> None:
    config = build_config()
    config.parameters.primary_scope_property = "site"
    client = _client()

    get_new_entities(client, config, MagicMock())

    query = client.data_modeling.instances.query.call_args.args[0]
    assert query.select["entities"].sources[0].properties == ["name", "alias", "assets", "site"]


def test_a_transient_read_failure_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(RobustAPIClient.robust_api_call.retry, "wait", wait_none())  # pyright: ignore[reportFunctionMemberAccess]
    config = build_config()
    client = MagicMock()
    client.data_modeling.instances.query.side_effect = [CogniteAPIError("Unavailable", code=503), Page([])]

    assert get_new_entities(client, config, MagicMock()) == []
    assert client.data_modeling.instances.query.call_count == 2


def test_manual_mapping_lookup_reads_the_same_way() -> None:
    config = build_config()
    view_id = config.data.entity_view.as_view_id()
    client = _client(Instance("inst_location", "ts:1", {view_id: {PROP_COL_NAME: "ts:1"}}))

    found = list_instances_by_external_id_direct(client, config, ["ts:1"], MagicMock())

    assert [node.external_id for node in found] == ["ts:1"]
    client.data_modeling.instances.list.assert_not_called()


def test_entities_carry_the_captured_groups_of_each_matching_rule() -> None:
    config = build_config()
    view_id = config.data.entity_view.as_view_id()
    client = _client(Instance("inst_location", "ts:1", {view_id: {PROP_COL_NAME: "23-KA-9101"}}))
    rules = [
        {KEY_RULE: "1", COL_KEY_RULE_REGEXP_ENTITY: re.compile("([0-9]+)-(X)?-?([A-Z]+)")},
        {KEY_RULE: "2", COL_KEY_RULE_REGEXP_ENTITY: re.compile("no match")},
    ]

    entities = get_new_entities(client, config, MagicMock(), rule_mappings=rules)  # type: ignore[arg-type]

    assert entities[0][KEY_RULE_KEYS] == ["1_23KA"], "a group outside the match is left out"


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
