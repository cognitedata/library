"""Tests for reading from and writing to several instance spaces.

`instanceSpace` accepts a single space or a list of spaces. Reads must cover every
configured space, and each write must land in the space the instance was read from -
never in a fixed space from the config.
"""

import json
import re
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.data_modeling import DirectRelationReference, NodeApply

sys.path.append(str(Path(__file__).parent))

from config import Config, ViewPropertyConfig
from constants import (
    COL_KEY_MAN_MAPPING_ENTITY,
    COL_KEY_MAN_MAPPING_TARGET,
    COL_KEY_RULE_REGEXP_ENTITY,
    FILTER_PATH_NODE_EXTERNAL_ID,
    KEY_ENTITY_EXT_ID,
    KEY_ENTITY_SPACE,
    KEY_MATCHES,
    KEY_NAME,
    KEY_ORG_NAME,
    KEY_RULE,
    KEY_RULE_KEYS,
    KEY_SCORE,
    KEY_SOURCE,
    KEY_TARGET,
    KEY_TARGET_EXT_ID,
    KEY_TARGET_LINKS,
    KEY_TARGET_SPACE,
    PROP_COL_LINK_NAME,
    PROP_COL_NAME,
)
from pipeline import (
    add_to_items,
    apply_manual_mappings,
    apply_rule_mappings,
    clean_links,
    get_all_targets,
    get_new_entities,
    remember_link_spaces,
    select_and_apply_matches,
    warn_on_cross_space_duplicates,
    write_mapping_to_raw,
)

PAGE_SIZE = 1000  # batch_size used by get_all_targets


class Instance:
    """Minimal stand-in for a node as returned by instances.list."""

    def __init__(self, space: str, external_id: str, properties: dict[Any, Any]) -> None:
        self.space = space
        self.external_id = external_id
        self.properties = properties


def _config(
    target_space: str | list[str],
    entity_space: str | list[str],
    search_property: str = PROP_COL_NAME,
) -> Config:
    return Config.model_validate(
        {
            "parameters": {
                "debug": False,
                "runAll": True,
                "dmUpdate": True,
                "removeOldLinks": False,
                "rawDb": "db_test",
                "rawTableState": "state",
                "rawTableCtxGood": "good",
                "rawTableCtxBad": "bad",
                "autoApprovalThreshold": 0.85,
            },
            "data": {
                "targetView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": target_space,
                    "externalId": "CogniteAsset",
                    "version": "v1",
                    "searchProperty": search_property,
                },
                "entityView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": entity_space,
                    "externalId": "CogniteTimeSeries",
                    "version": "v1",
                    "searchProperty": search_property,
                },
            },
        }
    )


def _view(instance_space: str | list[str]) -> ViewPropertyConfig:
    return ViewPropertyConfig.model_validate(
        {
            "schemaSpace": "cdf_cdm",
            "instanceSpace": instance_space,
            "externalId": "CogniteAsset",
            "version": "v1",
        }
    )


# --- Fake DMS list endpoint ------------------------------------------------------------
# Honours the space scope, sort and paging cursor so the keyset pagination in
# get_all_targets is genuinely exercised instead of being handed scripted pages.
# Range filters on space are rejected, as the real API does.


def _matches(clause: dict[str, Any], instance: Instance) -> bool:
    """Evaluate the subset of the filter DSL that the paging cursor uses."""
    if "and" in clause:
        return all(_matches(sub, instance) for sub in clause["and"])
    if "range" in clause:
        body = clause["range"]
        if body["property"] != FILTER_PATH_NODE_EXTERNAL_ID:
            raise AssertionError(f"The API only supports range filters on externalId: {body}")
        if set(body) != {"property", "gt"}:
            raise AssertionError(f"Only 'gt' ranges are expected in the cursor: {body}")
        return instance.external_id > body["gt"]
    raise AssertionError(f"Unexpected filter clause in paging cursor: {clause}")


def _ordered(rows: list[Instance], sort: Any) -> list[Instance]:
    """Order rows the way the query asked to have them ordered.

    Without a sort the API gives no ordering guarantee, so rows come back in an order a
    keyset cursor cannot accidentally page through correctly. That way dropping the sort
    from a paged query fails the paging tests instead of passing by luck.
    """
    if sort is None:
        return list(reversed(rows))

    for entry in sort if isinstance(sort, list) else [sort]:
        if list(entry.property) != FILTER_PATH_NODE_EXTERNAL_ID or entry.direction != "ascending":
            raise AssertionError(f"Unexpected sort for a paged query: {entry.property} {entry.direction}")
    return sorted(rows, key=lambda item: item.external_id)


def _fake_instances_list(dataset: list[Instance]) -> MagicMock:
    def _list(
        space: Any = None,
        sources: Any = None,
        filter: Any = None,  # mirrors the SDK parameter name
        sort: Any = None,
        limit: int | None = None,
        **_: Any,
    ) -> list[Instance]:
        scope = [space] if isinstance(space, str) else space
        rows = _ordered([item for item in dataset if item.space in scope], sort)
        if filter is not None:
            rows = [item for item in rows if _matches(filter.dump(), item)]
        if limit is not None and limit > 0:
            rows = rows[:limit]
        return rows

    return MagicMock(side_effect=_list)


def _requested_spaces(list_mock: MagicMock) -> set[str]:
    return {call.kwargs["space"] for call in list_mock.call_args_list}


def _links(item: NodeApply) -> list[DirectRelationReference]:
    """Direct relations queued on a node update."""
    links = item.sources[0].properties[PROP_COL_LINK_NAME]  # type: ignore[index, union-attr]
    assert isinstance(links, list)
    return links


def _target(view_id: Any, space: str, external_id: str) -> Instance:
    return Instance(space, external_id, {view_id: {PROP_COL_NAME: external_id}})


def _entity(view_id: Any, space: str, external_id: str) -> Instance:
    return Instance(space, external_id, {view_id: {PROP_COL_NAME: external_id, PROP_COL_LINK_NAME: []}})


# --- Reads ----------------------------------------------------------------------------


def test_targets_are_read_from_every_configured_space() -> None:
    """A target list must cover all configured spaces, not just the first."""
    config = _config(["inst_asset_a", "inst_asset_b"], "inst_ts")
    view_id = config.data.target_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list = _fake_instances_list(
        [_target(view_id, "inst_asset_a", "asset-a"), _target(view_id, "inst_asset_b", "asset-b")]
    )

    targets = get_all_targets(client, MagicMock(), config)

    assert _requested_spaces(client.data_modeling.instances.list) == {"inst_asset_a", "inst_asset_b"}
    assert {target[KEY_TARGET_SPACE] for target in targets} == {"inst_asset_a", "inst_asset_b"}


def test_page_boundary_does_not_skip_a_duplicate_external_id_in_another_space() -> None:
    """External IDs are unique per space only, so paging must run one space at a time.

    The dataset is built so a full page of one space ends on an external ID that also
    exists in the other space. Paging all spaces in one query with a cursor on external
    ID alone would never return the second instance.
    """
    config = _config(["inst_asset_a", "inst_asset_b"], "inst_ts")
    view_id = config.data.target_view.as_view_id()
    dataset = [_target(view_id, "inst_asset_a", f"asset-{i:04d}") for i in range(PAGE_SIZE - 1)]
    dataset.append(_target(view_id, "inst_asset_a", "asset-dup"))
    dataset.append(_target(view_id, "inst_asset_b", "asset-dup"))
    client = MagicMock()
    client.data_modeling.instances.list = _fake_instances_list(dataset)

    targets = get_all_targets(client, MagicMock(), config)

    assert len(targets) == len(dataset)
    duplicate_spaces = {t[KEY_TARGET_SPACE] for t in targets if t[KEY_TARGET_EXT_ID] == "asset-dup"}
    assert duplicate_spaces == {"inst_asset_a", "inst_asset_b"}


def test_target_paging_never_filters_on_space() -> None:
    """The API rejects range filters on space, so the cursor must only use externalId.

    The fake list endpoint raises on any other property, so a full page - which forces a
    second, cursor-carrying request - is enough to pin this.
    """
    config = _config(["inst_asset_a", "inst_asset_b"], "inst_ts")
    view_id = config.data.target_view.as_view_id()
    dataset = [_target(view_id, "inst_asset_a", f"asset-{i:04d}") for i in range(PAGE_SIZE)]
    client = MagicMock()
    client.data_modeling.instances.list = _fake_instances_list(dataset)

    targets = get_all_targets(client, MagicMock(), config)

    assert len(targets) == PAGE_SIZE
    assert client.data_modeling.instances.list.call_count > len(config.data.target_view.instance_spaces)


def test_target_paging_sorts_by_external_id() -> None:
    """The cursor is a keyset on externalId, which only pages correctly on ordered pages."""
    config = _config("inst_asset", "inst_ts")
    view_id = config.data.target_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list = _fake_instances_list([_target(view_id, "inst_asset", "asset-a")])

    get_all_targets(client, MagicMock(), config)

    sort = client.data_modeling.instances.list.call_args.kwargs["sort"]
    assert list(sort.property) == FILTER_PATH_NODE_EXTERNAL_ID
    assert sort.direction == "ascending"


def test_entities_are_read_from_every_configured_space() -> None:
    """Entities spread across source-specific spaces must all be picked up."""
    config = _config("inst_asset", ["inst_ts_pi", "inst_ts_sap"])
    view_id = config.data.entity_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list.return_value = [
        _entity(view_id, "inst_ts_pi", "pi:1"),
        _entity(view_id, "inst_ts_sap", "sap:1"),
    ]

    entities = get_new_entities(client, config, MagicMock())

    assert client.data_modeling.instances.list.call_args.kwargs["space"] == ["inst_ts_pi", "inst_ts_sap"]
    assert len(entities) == 2


def test_entity_without_links_is_recorded_as_an_empty_list() -> None:
    """An entity with no links must not be serialised as JSON null.

    "null" reads back as None, and both the rule matching and the write path call len()
    on it.
    """
    config = _config("inst_asset", "inst_ts")
    view_id = config.data.entity_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list.return_value = [
        Instance("inst_ts", "ts:1", {view_id: {PROP_COL_NAME: "ts:1", PROP_COL_LINK_NAME: None}})
    ]

    entities = get_new_entities(client, config, MagicMock())

    assert json.loads(entities[0][KEY_TARGET_LINKS]) == []


def test_entity_with_no_link_property_at_all_is_recorded_as_an_empty_list() -> None:
    """An unset property is left out of the response, so the key itself can be missing."""
    config = _config("inst_asset", "inst_ts")
    view_id = config.data.entity_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list.return_value = [Instance("inst_ts", "ts:1", {view_id: {PROP_COL_NAME: "ts:1"}})]

    entities = get_new_entities(client, config, MagicMock())

    assert json.loads(entities[0][KEY_TARGET_LINKS]) == []


def test_manual_mapping_of_a_never_linked_entity_is_applied() -> None:
    """The link property is missing for an entity that has never been linked.

    That is the normal state of an entity someone maps by hand, and reading the property
    unguarded aborts every mapping in the run through the surrounding error handler.
    """
    config = _config("inst_asset", "inst_ts")
    view_id = config.data.entity_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list.return_value = [
        Instance("inst_ts", "ts:1", {view_id: {PROP_COL_NAME: "ts:1"}})
    ]
    mappings = [
        {
            KEY_RULE: "row-1",
            COL_KEY_MAN_MAPPING_ENTITY: "ts:1",
            COL_KEY_MAN_MAPPING_TARGET: "asset-a",
        }
    ]

    good_matches, count = apply_manual_mappings(
        client, MagicMock(), config, MagicMock(), mappings, {"row-1": {}}
    )

    assert [match[KEY_ENTITY_EXT_ID] for match in good_matches] == ["ts:1"]
    assert count == 1


def test_manual_mapping_missing_view_properties_is_skipped() -> None:
    """A node without the entity view payload must not abort the rest of the run."""
    config = _config("inst_asset", "inst_ts")
    view_id = config.data.entity_view.as_view_id()
    other_view_id = config.data.target_view.as_view_id()
    client = MagicMock()
    logger = MagicMock()
    client.data_modeling.instances.list.return_value = [
        Instance("inst_ts", "ts-none", None),
        Instance("inst_ts", "ts-other", {other_view_id: {PROP_COL_NAME: "other"}}),
        Instance("inst_ts", "ts-empty", {view_id: {}}),
        Instance("inst_ts", "ts-ok", {view_id: {PROP_COL_NAME: "ts-ok"}}),
    ]
    mappings = [
        {
            KEY_RULE: f"row-{entity}",
            COL_KEY_MAN_MAPPING_ENTITY: entity,
            COL_KEY_MAN_MAPPING_TARGET: "asset-a",
        }
        for entity in ("ts-none", "ts-other", "ts-empty", "ts-ok")
    ]
    mapping_input = {f"row-{entity}": {} for entity in ("ts-none", "ts-other", "ts-empty", "ts-ok")}

    good_matches, _count = apply_manual_mappings(client, logger, config, MagicMock(), mappings, mapping_input)

    assert [match[KEY_ENTITY_EXT_ID] for match in good_matches] == ["ts-ok"]
    warned = " ".join(call[0][0] for call in logger.warning.call_args_list)
    assert "ts-none" in warned
    assert "ts-other" in warned
    assert "ts-empty" in warned


def test_single_space_string_is_read_as_that_one_space() -> None:
    """The single-space configuration must keep behaving exactly as before."""
    config = _config("inst_asset", "inst_ts")
    view_id = config.data.target_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list = _fake_instances_list([_target(view_id, "inst_asset", "asset-a")])

    targets = get_all_targets(client, MagicMock(), config)

    assert _requested_spaces(client.data_modeling.instances.list) == {"inst_asset"}
    assert [target[KEY_TARGET_SPACE] for target in targets] == ["inst_asset"]


def test_target_missing_view_properties_is_skipped() -> None:
    """A node without the target view payload must not abort the rest of the run."""
    config = _config("inst_asset", "inst_ts")
    view_id = config.data.target_view.as_view_id()
    other_view_id = config.data.entity_view.as_view_id()
    client = MagicMock()
    logger = MagicMock()
    client.data_modeling.instances.list = _fake_instances_list(
        [
            Instance("inst_asset", "asset-none", None),
            Instance("inst_asset", "asset-other", {other_view_id: {PROP_COL_NAME: "other"}}),
            Instance("inst_asset", "asset-empty", {view_id: {}}),
            _target(view_id, "inst_asset", "asset-ok"),
        ]
    )

    targets = get_all_targets(client, logger, config)

    assert [target[KEY_TARGET_EXT_ID] for target in targets] == ["asset-ok"]
    warned = " ".join(call[0][0] for call in logger.warning.call_args_list)
    assert "asset-none" in warned
    assert "asset-other" in warned
    assert "asset-empty" in warned


def test_entity_missing_view_properties_is_skipped() -> None:
    """A node without the entity view payload must not abort the rest of the run."""
    config = _config("inst_asset", "inst_ts")
    view_id = config.data.entity_view.as_view_id()
    other_view_id = config.data.target_view.as_view_id()
    client = MagicMock()
    logger = MagicMock()
    client.data_modeling.instances.list.return_value = [
        Instance("inst_ts", "ts-none", None),
        Instance("inst_ts", "ts-other", {other_view_id: {PROP_COL_NAME: "other"}}),
        Instance("inst_ts", "ts-empty", {view_id: {}}),
        _entity(view_id, "inst_ts", "ts-ok"),
    ]

    entities = get_new_entities(client, config, logger)

    assert [entity[KEY_ENTITY_EXT_ID] for entity in entities] == ["ts-ok"]
    warned = " ".join(call[0][0] for call in logger.warning.call_args_list)
    assert "ts-none" in warned
    assert "ts-other" in warned
    assert "ts-empty" in warned


def test_rule_key_leaves_out_an_unmatched_optional_group() -> None:
    """Rule regexes are operator-authored, so an optional group is theirs to configure.

    An optional group that does not participate captures None, which cannot be joined
    into the rule key.
    """
    config = _config("inst_asset", "inst_ts")
    view_id = config.data.entity_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list.return_value = [
        Instance("inst_ts", "ts:1", {view_id: {PROP_COL_NAME: "1234 discharge"}})
    ]
    rules = [{KEY_RULE: "pump", COL_KEY_RULE_REGEXP_ENTITY: re.compile(r"([A-Z]{3})?[-_]?(\d{4})")}]

    entities = get_new_entities(client, config, MagicMock(), None, rules)

    assert [entity[KEY_RULE_KEYS] for entity in entities] == [["pump_1234"]]


# --- Empty search property ------------------------------------------------------------
# An unset property is left out of the response, but an empty one is not. Both carry
# nothing to match on, so both fall back to the name instead of dropping the instance or
# matching it on an empty string.

ALIAS_PROP = "aliases"


def test_entity_with_an_empty_alias_list_matches_on_its_name() -> None:
    """An empty list used to leave the entity out of the match set entirely."""
    config = _config("inst_asset", "inst_ts", ALIAS_PROP)
    view_id = config.data.entity_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list.return_value = [
        Instance("inst_ts", "ts:1", {view_id: {PROP_COL_NAME: "23-KA-9101", ALIAS_PROP: []}})
    ]

    entities = get_new_entities(client, config, MagicMock())

    assert [entity[KEY_NAME] for entity in entities] == ["23-KA-9101"]


def test_entity_with_a_blank_alias_matches_on_its_name() -> None:
    """A blank string is not something the matching model can use either."""
    config = _config("inst_asset", "inst_ts", ALIAS_PROP)
    view_id = config.data.entity_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list.return_value = [
        Instance("inst_ts", "ts:1", {view_id: {PROP_COL_NAME: "23-KA-9101", ALIAS_PROP: "  "}})
    ]

    entities = get_new_entities(client, config, MagicMock())

    assert [entity[KEY_NAME] for entity in entities] == ["23-KA-9101"]


def test_entity_keeps_the_usable_aliases_and_drops_the_empty_ones() -> None:
    """A partly populated list must still match on the values it does have."""
    config = _config("inst_asset", "inst_ts", ALIAS_PROP)
    view_id = config.data.entity_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list.return_value = [
        Instance("inst_ts", "ts:1", {view_id: {PROP_COL_NAME: "23-KA-9101", ALIAS_PROP: ["pi:1", ""]}})
    ]

    entities = get_new_entities(client, config, MagicMock())

    assert [entity[KEY_NAME] for entity in entities] == ["pi:1"]


def test_target_with_an_empty_alias_list_matches_on_its_name() -> None:
    """The target side reads the search property the same way."""
    config = _config("inst_asset", "inst_ts", ALIAS_PROP)
    view_id = config.data.target_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list = _fake_instances_list(
        [Instance("inst_asset", "asset-a", {view_id: {PROP_COL_NAME: "23-KA-9101", ALIAS_PROP: []}})]
    )

    targets = get_all_targets(client, MagicMock(), config)

    assert [target[KEY_NAME] for target in targets] == ["23-KA-9101"]


# --- Writes ---------------------------------------------------------------------------


def test_match_is_written_to_the_space_each_instance_came_from() -> None:
    """Neither side of a match may be written to a space it does not live in."""
    config = _config(["inst_asset_a", "inst_asset_b"], ["inst_ts_pi", "inst_ts_sap"])
    entity_view_id = config.data.entity_view.as_view_id()

    items = add_to_items(
        config,
        MagicMock(),
        [],
        ["asset-b"],
        "sap:1",
        entity_view_id,
        entity_space="inst_ts_sap",
        target_spaces={"asset-b": "inst_asset_b"},
    )

    assert items[0].space == "inst_ts_sap"
    assert [(link.space, link.external_id) for link in _links(items[0])] == [("inst_asset_b", "asset-b")]


def test_write_falls_back_to_the_first_configured_space() -> None:
    """When a caller cannot supply the spaces, the first configured one is used."""
    config = _config(["inst_asset_a", "inst_asset_b"], ["inst_ts_pi", "inst_ts_sap"])

    items = add_to_items(config, MagicMock(), [], ["asset-x"], "pi:1", config.data.entity_view.as_view_id())

    assert items[0].space == "inst_ts_pi"
    assert _links(items[0])[0].space == "inst_asset_a"


def test_single_space_string_is_written_to_that_one_space() -> None:
    """The single-space configuration must keep behaving exactly as before."""
    config = _config("inst_asset", "inst_ts")

    items = add_to_items(config, MagicMock(), [], ["asset-x"], "ts:1", config.data.entity_view.as_view_id())

    assert items[0].space == "inst_ts"
    assert _links(items[0])[0].space == "inst_asset"


def test_existing_links_keep_their_space_when_reapplied() -> None:
    """Re-applied links must not be moved into the first configured target space."""
    config = _config(["inst_asset_a", "inst_asset_b"], "inst_ts")
    existing = json.dumps([{"space": "inst_asset_b", "externalId": "asset-old"}])

    items = add_to_items(
        config,
        MagicMock(),
        [],
        ["asset-new"],
        "ts:1",
        config.data.entity_view.as_view_id(),
        existing,
        entity_space="inst_ts",
        target_spaces={"asset-new": "inst_asset_a"},
    )

    links = {link.external_id: link.space for link in _links(items[0])}
    assert links == {"asset-old": "inst_asset_b", "asset-new": "inst_asset_a"}


def test_target_view_space_wins_over_a_stale_existing_link() -> None:
    """The target query is authoritative, so a re-applied link must not keep a stale space."""
    config = _config(["inst_asset_a", "inst_asset_b"], "inst_ts")
    existing = json.dumps([{"space": "inst_asset_b", "externalId": "asset-1"}])

    items = add_to_items(
        config,
        MagicMock(),
        [],
        ["asset-1"],
        "ts:1",
        config.data.entity_view.as_view_id(),
        existing,
        entity_space="inst_ts",
        target_spaces={"asset-1": "inst_asset_a"},
    )

    assert {link.external_id: link.space for link in _links(items[0])} == {"asset-1": "inst_asset_a"}


def test_null_existing_links_do_not_break_the_write() -> None:
    """The links string round-trips through the matching API, so tolerate a JSON null."""
    config = _config("inst_asset", "inst_ts")

    items = add_to_items(
        config,
        MagicMock(),
        [],
        ["asset-a"],
        "ts:1",
        config.data.entity_view.as_view_id(),
        "null",
    )

    assert [link.external_id for link in _links(items[0])] == ["asset-a"]


def test_invalid_json_existing_links_do_not_break_the_write() -> None:
    """A truncated or non-JSON string from the matching API must not abort the write."""
    logger = MagicMock()
    config = _config("inst_asset", "inst_ts")

    items = add_to_items(
        config,
        logger,
        [],
        ["asset-a"],
        "ts:1",
        config.data.entity_view.as_view_id(),
        "{not-json",
    )

    assert [link.external_id for link in _links(items[0])] == ["asset-a"]
    assert any("ts:1" in call[0][0] for call in logger.warning.call_args_list)


def test_non_list_existing_links_do_not_break_the_write() -> None:
    """JSON that is not a list (e.g. a boolean) must not abort the write."""
    config = _config("inst_asset", "inst_ts")

    items = add_to_items(
        config,
        MagicMock(),
        [],
        ["asset-a"],
        "ts:1",
        config.data.entity_view.as_view_id(),
        "true",
    )

    assert [link.external_id for link in _links(items[0])] == ["asset-a"]


def test_existing_links_as_a_list_are_applied() -> None:
    """The matching API may echo existing links already parsed, not as a JSON string."""
    config = _config("inst_asset", "inst_ts")
    existing = [{"space": "inst_asset", "externalId": "asset-old"}]

    items = add_to_items(
        config,
        MagicMock(),
        [],
        ["asset-new"],
        "ts:1",
        config.data.entity_view.as_view_id(),
        existing,
        entity_space="inst_ts",
        target_spaces={"asset-old": "inst_asset", "asset-new": "inst_asset"},
    )

    assert {link.external_id: link.space for link in _links(items[0])} == {
        "asset-old": "inst_asset",
        "asset-new": "inst_asset",
    }


def test_incomplete_existing_links_are_skipped_on_write() -> None:
    """Malformed existing links are dropped; complete ones and new targets still write."""
    config = _config("inst_asset", "inst_ts")
    existing = json.dumps(
        [
            "not-a-link",
            42,
            {"externalId": "asset-broken"},
            {"space": "inst_asset"},
            {"space": "inst_asset", "externalId": "asset-keep"},
        ]
    )

    items = add_to_items(
        config,
        MagicMock(),
        [],
        ["asset-new"],
        "ts:1",
        config.data.entity_view.as_view_id(),
        existing,
        entity_space="inst_ts",
        target_spaces={"asset-keep": "inst_asset", "asset-new": "inst_asset"},
    )

    assert {link.external_id: link.space for link in _links(items[0])} == {
        "asset-keep": "inst_asset",
        "asset-new": "inst_asset",
    }


def test_clean_links_resets_the_entity_in_its_own_space() -> None:
    """Clearing links must target the existing node, not a new one in another space."""
    config = _config("inst_asset", ["inst_ts_pi", "inst_ts_sap"])

    items = clean_links(config, "inst_ts_sap", "sap:1", [])

    assert items[0].space == "inst_ts_sap"
    assert items[0].sources[0].properties == {PROP_COL_LINK_NAME: None}


# --- Unknown spaces -------------------------------------------------------------------
# Falling back to the first configured space keeps a write from failing outright, but with
# several spaces configured it can point a link at a space the node does not live in. The
# fallback must therefore be visible in the log.


def test_unknown_entity_space_warns_when_several_are_configured() -> None:
    logger = MagicMock()
    config = _config("inst_asset", ["inst_ts_pi", "inst_ts_sap"])

    add_to_items(config, logger, [], ["asset-a"], "ts:1", config.data.entity_view.as_view_id())

    assert any("ts:1" in call[0][0] for call in logger.warning.call_args_list)


def test_unknown_target_space_warns_when_several_are_configured() -> None:
    logger = MagicMock()
    config = _config(["inst_asset_a", "inst_asset_b"], "inst_ts")

    add_to_items(
        config,
        logger,
        [],
        ["asset-a"],
        "ts:1",
        config.data.entity_view.as_view_id(),
        entity_space="inst_ts",
    )

    assert any("asset-a" in call[0][0] for call in logger.warning.call_args_list)


def test_unknown_space_is_not_flagged_with_one_space() -> None:
    """With one space per view the fallback is always the right space."""
    logger = MagicMock()
    config = _config("inst_asset", "inst_ts")

    add_to_items(config, logger, [], ["asset-a"], "ts:1", config.data.entity_view.as_view_id())

    logger.warning.assert_not_called()


def test_known_spaces_are_not_flagged() -> None:
    logger = MagicMock()
    config = _config(["inst_asset_a", "inst_asset_b"], ["inst_ts_pi", "inst_ts_sap"])

    add_to_items(
        config,
        logger,
        [],
        ["asset-a"],
        "ts:1",
        config.data.entity_view.as_view_id(),
        entity_space="inst_ts_sap",
        target_spaces={"asset-a": "inst_asset_b"},
    )

    logger.warning.assert_not_called()


# --- Space bookkeeping helpers --------------------------------------------------------


def test_existing_link_keeps_its_own_space() -> None:
    """A link to a target outside the current target set must not be moved."""
    target_spaces: dict[str, str] = {}

    remember_link_spaces(target_spaces, [{"space": "inst_asset_sap", "externalId": "23-KA-9101"}])

    assert target_spaces == {"23-KA-9101": "inst_asset_sap"}


def test_space_from_target_view_wins_over_link() -> None:
    """The target query is authoritative; a stale link must not overwrite it."""
    target_spaces = {"23-KA-9101": "inst_asset_workmate"}

    remember_link_spaces(target_spaces, [{"space": "inst_asset_sap", "externalId": "23-KA-9101"}])

    assert target_spaces == {"23-KA-9101": "inst_asset_workmate"}


def test_incomplete_links_are_ignored() -> None:
    """Links that are not a complete space/externalId object carry no usable space."""
    target_spaces: dict[str, str] = {}
    links: list[Any] = [
        "not-a-link",
        42,
        {"externalId": "23-KA-9101"},
        {"space": "inst_asset_sap"},
    ]

    remember_link_spaces(target_spaces, links)

    assert target_spaces == {}


def _node(space: str, external_id: str) -> MagicMock:
    node = MagicMock()
    node.space = space
    node.external_id = external_id
    return node


def test_duplicate_external_id_across_spaces_warns() -> None:
    """Target links are resolved by external ID, so the operator must hear about collisions."""
    logger = MagicMock()
    instances = [_node("inst_a", "23-KA-9101"), _node("inst_b", "23-KA-9101")]

    warn_on_cross_space_duplicates(instances, "assets", _view(["inst_a", "inst_b"]), logger)

    logger.warning.assert_called_once()
    assert "23-KA-9101" in logger.warning.call_args[0][0]


def test_same_external_id_in_one_space_does_not_warn() -> None:
    """Distinct external IDs across spaces are the expected, supported case."""
    logger = MagicMock()
    instances = [_node("inst_a", "23-KA-9101"), _node("inst_b", "11-PT-2222")]

    warn_on_cross_space_duplicates(instances, "assets", _view(["inst_a", "inst_b"]), logger)

    logger.warning.assert_not_called()


def test_single_space_skips_the_check() -> None:
    """With one configured space a cross-space collision cannot happen."""
    logger = MagicMock()
    instances = [_node("inst_a", "23-KA-9101"), _node("inst_a", "23-KA-9101")]

    warn_on_cross_space_duplicates(instances, "assets", _view("inst_a"), logger)

    logger.warning.assert_not_called()


# --- Duplicate external IDs across spaces ---------------------------------------------
# An external ID is unique per space only, so the same one in two spaces is two distinct
# instances. Both must be matched and each written to its own space, rather than one
# standing in for the other.


def _entity_record(space: str, external_id: str, rule_keys: list[str] | None = None) -> dict[str, Any]:
    """A source record shaped the way get_new_entities builds it."""
    return {
        KEY_ENTITY_EXT_ID: external_id,
        KEY_ENTITY_SPACE: space,
        KEY_NAME: external_id,
        KEY_ORG_NAME: external_id,
        KEY_TARGET_LINKS: json.dumps([]),
        KEY_RULE_KEYS: rule_keys,
    }


def _target_record(space: str, external_id: str, rule_keys: list[str] | None = None) -> dict[str, Any]:
    """A target record shaped the way get_all_targets builds it."""
    return {
        KEY_TARGET_EXT_ID: external_id,
        KEY_TARGET_SPACE: space,
        KEY_NAME: external_id,
        KEY_ORG_NAME: external_id,
        KEY_RULE_KEYS: rule_keys,
    }


def _match_result(entity_space: str, entity_ext_id: str, target: dict[str, Any], score: float) -> dict[str, Any]:
    """A prediction as the entity matching API returns it, echoing our own dicts."""
    return {
        KEY_SOURCE: _entity_record(entity_space, entity_ext_id),
        KEY_MATCHES: [{KEY_SCORE: score, KEY_TARGET: target}],
    }


def _applied(client: MagicMock) -> list[NodeApply]:
    """Every node update handed to instances.apply."""
    items: list[NodeApply] = []
    for call in client.data_modeling.instances.apply.call_args_list:
        items.extend(call.args[0])
    return items


def test_entity_matched_in_one_space_keeps_its_twin_in_another() -> None:
    """Skipping already-matched entities must not skip a same-named one elsewhere."""
    config = _config("inst_asset", ["inst_ts_pi", "inst_ts_sap"])
    view_id = config.data.entity_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list.return_value = [
        _entity(view_id, "inst_ts_pi", "pi:1"),
        _entity(view_id, "inst_ts_sap", "pi:1"),
    ]

    entities = get_new_entities(client, config, MagicMock(), [("inst_ts_pi", "pi:1")])

    assert [(entity[KEY_ENTITY_SPACE], entity[KEY_ENTITY_EXT_ID]) for entity in entities] == [
        ("inst_ts_sap", "pi:1")
    ]


def test_rule_match_links_both_copies_in_their_own_space() -> None:
    """Two entities sharing an external ID each need their own node update."""
    config = _config("inst_asset", ["inst_ts_pi", "inst_ts_sap"])
    client = MagicMock()
    targets = [_target_record("inst_asset", "asset-a", ["k1"])]
    entities = [
        _entity_record("inst_ts_pi", "pi:1", ["k1"]),
        _entity_record("inst_ts_sap", "pi:1", ["k1"]),
    ]

    good_matches, _ = apply_rule_mappings(client, config, MagicMock(), [], targets, entities)

    assert {(match[KEY_ENTITY_SPACE], match[KEY_ENTITY_EXT_ID]) for match in good_matches} == {
        ("inst_ts_pi", "pi:1"),
        ("inst_ts_sap", "pi:1"),
    }
    applied = _applied(client)
    assert {item.space for item in applied} == {"inst_ts_pi", "inst_ts_sap"}
    for item in applied:
        assert [(link.space, link.external_id) for link in _links(item)] == [("inst_asset", "asset-a")]


def test_ml_match_links_both_copies_in_their_own_space() -> None:
    """The same holds for matches coming back from the matching model."""
    config = _config("inst_asset", ["inst_ts_pi", "inst_ts_sap"])
    client = MagicMock()
    target = _target_record("inst_asset", "asset-a")
    match_results = [
        _match_result("inst_ts_pi", "pi:1", target, 0.95),
        _match_result("inst_ts_sap", "pi:1", target, 0.95),
    ]

    good_matches, _, count = select_and_apply_matches(client, config, MagicMock(), [], match_results)

    assert count == 2
    assert {(match[KEY_ENTITY_SPACE], match[KEY_ENTITY_EXT_ID]) for match in good_matches} == {
        ("inst_ts_pi", "pi:1"),
        ("inst_ts_sap", "pi:1"),
    }
    assert {item.space for item in _applied(client)} == {"inst_ts_pi", "inst_ts_sap"}


def test_manually_matched_entity_does_not_block_its_twin() -> None:
    """A manual match in one space must leave the other space's entity to be matched."""
    config = _config("inst_asset", ["inst_ts_pi", "inst_ts_sap"])
    client = MagicMock()
    target = _target_record("inst_asset", "asset-a")
    already_matched = [
        {
            KEY_ENTITY_EXT_ID: "pi:1",
            KEY_ENTITY_SPACE: "inst_ts_pi",
            KEY_TARGET_EXT_ID: "asset-a",
            KEY_TARGET_SPACE: "inst_asset",
        }
    ]

    _, _, count = select_and_apply_matches(
        client, config, MagicMock(), already_matched, [_match_result("inst_ts_sap", "pi:1", target, 0.95)]
    )

    assert count == 1
    assert [item.space for item in _applied(client)] == ["inst_ts_sap"]


def test_unreadable_match_results_fail_the_run() -> None:
    """A parse failure must fail the run rather than read back as "nothing matched"."""
    config = _config("inst_asset", "inst_ts")

    with pytest.raises(KeyError):
        select_and_apply_matches(MagicMock(), config, MagicMock(), [], [{"unexpected": "shape"}])


def test_raw_report_keeps_a_row_per_space() -> None:
    """The good/bad tables are keyed per entity, so the key needs the space too."""
    config = _config("inst_asset", ["inst_ts_pi", "inst_ts_sap"])
    raw_uploader = MagicMock()
    good_matches = [
        {KEY_ENTITY_EXT_ID: "pi:1", KEY_ENTITY_SPACE: "inst_ts_pi"},
        {KEY_ENTITY_EXT_ID: "pi:1", KEY_ENTITY_SPACE: "inst_ts_sap"},
    ]

    write_mapping_to_raw(MagicMock(), config, raw_uploader, good_matches, [], MagicMock())

    row_keys = [call.args[2].key for call in raw_uploader.add_to_upload_queue.call_args_list]
    assert len(set(row_keys)) == 2


def test_raw_report_row_key_is_the_external_id_with_one_space() -> None:
    """A single-space project must keep the row keys it already has."""
    config = _config("inst_asset", "inst_ts")
    raw_uploader = MagicMock()
    good_matches = [{KEY_ENTITY_EXT_ID: "pi:1", KEY_ENTITY_SPACE: "inst_ts"}]

    write_mapping_to_raw(MagicMock(), config, raw_uploader, good_matches, [], MagicMock())

    assert [call.args[2].key for call in raw_uploader.add_to_upload_queue.call_args_list] == ["pi:1"]


# --- Batched writes -------------------------------------------------------------------
# Node updates are flushed every BATCH_SIZE_API_SUBMIT items so a long run makes
# incremental progress instead of holding every update until the end.


def _applied_batch_sizes(client: MagicMock) -> list[int]:
    """How many nodes went out in each call to instances.apply."""
    return [len(call.args[0]) for call in client.data_modeling.instances.apply.call_args_list]


def test_rule_matches_are_written_in_capped_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    """The batch counter has to advance with the write loop, not sit at a constant."""
    monkeypatch.setattr("pipeline.BATCH_SIZE_API_SUBMIT", 2)
    config = _config("inst_asset", "inst_ts")
    client = MagicMock()
    targets = [_target_record("inst_asset", f"asset-{idx}", [f"k{idx}"]) for idx in range(3)]
    entities = [_entity_record("inst_ts", f"ts:{idx}", [f"k{idx}"]) for idx in range(3)]

    apply_rule_mappings(client, config, MagicMock(), [], targets, entities)

    assert _applied_batch_sizes(client) == [2, 1]


def test_manual_matches_are_written_in_capped_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    """A skipped entity advances the counter without queueing anything.

    Counting entities rather than queued updates lets the batch overshoot the cap, and a
    skipped entity landing on a multiple of it misses the flush altogether.
    """
    monkeypatch.setattr("pipeline.BATCH_SIZE_API_SUBMIT", 2)
    config = _config("inst_asset", "inst_ts", ALIAS_PROP)
    view_id = config.data.entity_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list.return_value = [
        Instance("inst_ts", f"ts:{idx}", {view_id: {PROP_COL_NAME: f"ts:{idx}"}}) for idx in range(1, 5)
    ]
    # ts:2 has no target, so it is skipped after the counter has already moved on.
    targets_by_entity = {"ts:1": "asset-1", "ts:2": "", "ts:3": "asset-3", "ts:4": "asset-4"}
    mappings = [
        {
            KEY_RULE: f"row-{entity}",
            COL_KEY_MAN_MAPPING_ENTITY: entity,
            COL_KEY_MAN_MAPPING_TARGET: target,
        }
        for entity, target in targets_by_entity.items()
    ]
    mapping_input = {f"row-{entity}": {} for entity in targets_by_entity}

    apply_manual_mappings(client, MagicMock(), config, MagicMock(), mappings, mapping_input)

    assert _applied_batch_sizes(client) == [2, 1]


def test_ml_matches_are_written_in_capped_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    """The same counter mistake sat in the matching-model write loop."""
    monkeypatch.setattr("pipeline.BATCH_SIZE_API_SUBMIT", 2)
    config = _config("inst_asset", "inst_ts")
    client = MagicMock()
    match_results = [
        _match_result("inst_ts", f"ts:{idx}", _target_record("inst_asset", f"asset-{idx}"), 0.95)
        for idx in range(3)
    ]

    select_and_apply_matches(client, config, MagicMock(), [], match_results)

    assert _applied_batch_sizes(client) == [2, 1]
