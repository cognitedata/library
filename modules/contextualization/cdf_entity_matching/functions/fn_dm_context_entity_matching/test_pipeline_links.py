"""Direct-relation links from DMS must not be treated as dictionaries."""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes.data_modeling import DirectRelationReference  # isort: skip

from em_constants import PROP_COL_LINK_NAME  # isort: skip
from em_pipeline import (  # isort: skip
    _links_as_json,
    add_to_items,
    get_links_from_entity,
    remember_link_spaces,
)
from test_submit import build_config  # isort: skip


def test_get_links_from_entity_accepts_direct_relations() -> None:
    link = DirectRelationReference(space="inst_asset", external_id="23-KA-9101")

    assert get_links_from_entity([link]) == ["23-KA-9101"]
    assert get_links_from_entity([{"space": "inst_asset", "externalId": "pump-1"}]) == ["pump-1"]
    assert get_links_from_entity(None) == []


def test_remember_link_spaces_accepts_direct_relations() -> None:
    target_spaces: dict[str, str] = {}
    link = DirectRelationReference(space="inst_asset_sap", external_id="23-KA-9101")

    remember_link_spaces(target_spaces, [link])

    assert target_spaces == {"23-KA-9101": "inst_asset_sap"}


def test_existing_links_json_serializes_direct_relations() -> None:
    link = DirectRelationReference(space="inst_asset", external_id="23-KA-9101")

    dumped = _links_as_json([link])

    assert json.loads(dumped) == [{"space": "inst_asset", "externalId": "23-KA-9101"}]


def test_add_to_items_keeps_direct_relation_existing_links() -> None:
    config = build_config()
    logger = MagicMock()
    existing = DirectRelationReference(space="inst_asset", external_id="already-linked")

    items = add_to_items(
        config,
        logger,
        [],
        ["new-target"],
        "ts-1",
        config.data.entity_view.as_view_id(),
        entity_targets=[existing],
        entity_space="inst_location",
        target_spaces={"new-target": "inst_location", "already-linked": "inst_asset"},
    )

    sources = items[0].sources
    assert sources is not None
    properties = sources[0].properties
    assert properties is not None
    links = properties[PROP_COL_LINK_NAME]
    assert isinstance(links, list)
    assert [link.external_id for link in links] == ["already-linked", "new-target"]
    logger.warning.assert_not_called()
