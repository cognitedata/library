"""Tests for reading from and writing to several instance spaces.

`instanceSpace` accepts a single space or a list of spaces. Reads must cover every
configured space, and each write must land in the space the instance was read from -
never in a fixed space from the config.
"""

import json
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

sys.path.append(str(Path(__file__).parent))

from config import Config, ViewPropertyConfig
from constants import (
    KEY_TARGET_EXT_ID,
    KEY_TARGET_SPACE,
    PROP_COL_LINK_NAME,
    PROP_COL_NAME,
)
from pipeline import (
    add_to_items,
    clean_links,
    get_all_targets,
    get_new_entities,
    remember_link_spaces,
    warn_on_cross_space_duplicates,
)

PAGE_SIZE = 1000  # batch_size used by get_all_targets


class Instance:
    """Minimal stand-in for a node as returned by instances.list."""

    def __init__(self, space: str, external_id: str, properties: dict[Any, Any]) -> None:
        self.space = space
        self.external_id = external_id
        self.properties = properties


def _config(target_space: str | list[str], entity_space: str | list[str]) -> Config:
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
                    "searchProperty": PROP_COL_NAME,
                },
                "entityView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": entity_space,
                    "externalId": "CogniteTimeSeries",
                    "version": "v1",
                    "searchProperty": PROP_COL_NAME,
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


def _property_value(property_ref: list[str], instance: Instance) -> str:
    if property_ref == ["node", "externalId"]:
        return instance.external_id
    if property_ref == ["node", "space"]:
        return instance.space
    raise AssertionError(f"Unexpected property in paging cursor: {property_ref}")


def _matches(clause: dict[str, Any], instance: Instance) -> bool:
    """Evaluate the subset of the filter DSL that the paging cursor uses."""
    if "or" in clause:
        return any(_matches(sub, instance) for sub in clause["or"])
    if "and" in clause:
        return all(_matches(sub, instance) for sub in clause["and"])
    if "equals" in clause:
        body = clause["equals"]
        return _property_value(body["property"], instance) == body["value"]
    if "range" in clause:
        body = clause["range"]
        if set(body) != {"property", "gt"}:
            raise AssertionError(f"Only 'gt' ranges are expected in the cursor: {body}")
        return _property_value(body["property"], instance) > body["gt"]
    raise AssertionError(f"Unexpected filter clause in paging cursor: {clause}")


def _fake_instances_list(dataset: list[Instance]) -> MagicMock:
    def _list(
        space: Any = None,
        sources: Any = None,
        filter: Any = None,  # mirrors the SDK parameter name
        sort: Any = None,
        limit: int | None = None,
        **_: Any,
    ) -> list[Instance]:
        rows = [item for item in dataset if item.space in space]
        rows.sort(key=lambda item: (item.external_id, item.space))
        if filter is not None:
            rows = [item for item in rows if _matches(filter.dump(), item)]
        if limit is not None and limit > 0:
            rows = rows[:limit]
        return rows

    return MagicMock(side_effect=_list)


def _target(view_id: Any, space: str, external_id: str) -> Instance:
    return Instance(space, external_id, {view_id: {PROP_COL_NAME: external_id}})


def _entity(view_id: Any, space: str, external_id: str) -> Instance:
    return Instance(space, external_id, {view_id: {PROP_COL_NAME: external_id, PROP_COL_LINK_NAME: []}})


# --- Reads ----------------------------------------------------------------------------


def test_targets_are_read_from_every_configured_space() -> None:
    """A target list must be scoped to all configured spaces, not just the first."""
    config = _config(["inst_asset_a", "inst_asset_b"], "inst_ts")
    view_id = config.data.target_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list = _fake_instances_list(
        [_target(view_id, "inst_asset_a", "asset-a"), _target(view_id, "inst_asset_b", "asset-b")]
    )

    targets = get_all_targets(client, MagicMock(), config)

    assert client.data_modeling.instances.list.call_args.kwargs["space"] == ["inst_asset_a", "inst_asset_b"]
    assert {target[KEY_TARGET_SPACE] for target in targets} == {"inst_asset_a", "inst_asset_b"}


def test_page_boundary_does_not_skip_a_duplicate_external_id_in_another_space() -> None:
    """External IDs are unique per space, so the paging cursor must include the space.

    The dataset is built so a full page ends on the first of two instances sharing an
    external ID. A cursor keyed on external ID alone would never return the second.
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


def test_single_space_string_is_read_as_that_one_space() -> None:
    """The single-space configuration must keep behaving exactly as before."""
    config = _config("inst_asset", "inst_ts")
    view_id = config.data.target_view.as_view_id()
    client = MagicMock()
    client.data_modeling.instances.list = _fake_instances_list([_target(view_id, "inst_asset", "asset-a")])

    targets = get_all_targets(client, MagicMock(), config)

    assert client.data_modeling.instances.list.call_args.kwargs["space"] == ["inst_asset"]
    assert [target[KEY_TARGET_SPACE] for target in targets] == ["inst_asset"]


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
    links = items[0].sources[0].properties[PROP_COL_LINK_NAME]
    assert [(link.space, link.external_id) for link in links] == [("inst_asset_b", "asset-b")]


def test_write_falls_back_to_the_first_configured_space() -> None:
    """When a caller cannot supply the spaces, the first configured one is used."""
    config = _config(["inst_asset_a", "inst_asset_b"], ["inst_ts_pi", "inst_ts_sap"])

    items = add_to_items(config, MagicMock(), [], ["asset-x"], "pi:1", config.data.entity_view.as_view_id())

    assert items[0].space == "inst_ts_pi"
    assert items[0].sources[0].properties[PROP_COL_LINK_NAME][0].space == "inst_asset_a"


def test_single_space_string_is_written_to_that_one_space() -> None:
    """The single-space configuration must keep behaving exactly as before."""
    config = _config("inst_asset", "inst_ts")

    items = add_to_items(config, MagicMock(), [], ["asset-x"], "ts:1", config.data.entity_view.as_view_id())

    assert items[0].space == "inst_ts"
    assert items[0].sources[0].properties[PROP_COL_LINK_NAME][0].space == "inst_asset"


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

    links = {link.external_id: link.space for link in items[0].sources[0].properties[PROP_COL_LINK_NAME]}
    assert links == {"asset-old": "inst_asset_b", "asset-new": "inst_asset_a"}


def test_clean_links_resets_the_entity_in_its_own_space() -> None:
    """Clearing links must target the existing node, not a new one in another space."""
    config = _config("inst_asset", ["inst_ts_pi", "inst_ts_sap"])

    items = clean_links(config, "inst_ts_sap", "sap:1", [])

    assert items[0].space == "inst_ts_sap"
    assert items[0].sources[0].properties == {PROP_COL_LINK_NAME: None}


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
    """Links without both space and externalId carry no usable space."""
    target_spaces: dict[str, str] = {}

    remember_link_spaces(target_spaces, [{"externalId": "23-KA-9101"}, {"space": "inst_asset_sap"}])

    assert target_spaces == {}


def _node(space: str, external_id: str) -> MagicMock:
    node = MagicMock()
    node.space = space
    node.external_id = external_id
    return node


def test_duplicate_external_id_across_spaces_warns() -> None:
    """Matching is keyed on external ID, so the operator must be told about collisions."""
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
