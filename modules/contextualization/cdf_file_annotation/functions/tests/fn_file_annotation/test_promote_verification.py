"""A single promote match marks the linked diagram-parsing entity as verified."""

from unittest.mock import MagicMock

from cognite.client.exceptions import CogniteAPIError
from services.PromoteService import GeneralPromoteService
from test_multi_space import _config


def _service() -> tuple[GeneralPromoteService, MagicMock]:
    client = MagicMock()
    client.config.project = "acme"
    service = GeneralPromoteService(client, _config(None, None), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    return service, client


def _edge(service: GeneralPromoteService, page: int | None = None) -> MagicMock:
    edge = MagicMock()
    edge.space = "sp_dat_pattern_mode_results"
    edge.external_id = "pattern:file_PH-25578-P-4110006-001.pdf:12-TW-96195:assetlink:6516f10657"
    edge.start_node.space = "inst_location"
    edge.start_node.external_id = "file_PH-25578-P-4110006-001.pdf"
    if page is None:
        edge.properties = {}
    else:
        edge.properties = {service.core_annotation_view.as_view_id(): {"startNodePageNumber": page}}
    return edge


def test_single_match_sets_is_asset_verified_when_the_diagram_entity_is_unverified() -> None:
    service, client = _service()
    edge = _edge(service)
    client.get.return_value.json.return_value = {
        "entities": [
            {
                "externalId": "entity-unverified",
                "isAssetVerified": False,
                "annotationId": {"space": edge.space, "externalId": edge.external_id},
            },
            {
                "externalId": "entity-other",
                "isAssetVerified": False,
                "annotationId": {"space": "inst_location", "externalId": "some-other-annotation"},
            },
        ]
    }

    service._verify_promoted_diagram_entities([edge])

    client.post.assert_called_once()
    body = client.post.call_args.kwargs["json"]
    assert body == {"items": [{"externalId": "entity-unverified", "update": {"isAssetVerified": True}}]}
    assert client.post.call_args.kwargs["headers"]["cdf-version"] == "20230101-alpha"
    assert "/diagram-parsing/diagrams/inst_location/file_PH-25578-P-4110006-001.pdf/1" in client.get.call_args.args[0]


def test_single_match_leaves_an_already_verified_diagram_entity_unchanged() -> None:
    service, client = _service()
    edge = _edge(service, page=2)
    client.get.return_value.json.return_value = {
        "entities": [
            {
                "externalId": "entity-verified",
                "isAssetVerified": True,
                "annotationId": {"space": edge.space, "externalId": edge.external_id},
            }
        ]
    }

    service._verify_promoted_diagram_entities([edge])

    client.post.assert_not_called()
    assert client.get.call_args.args[0].endswith("/2")


def test_missing_parsed_diagram_does_not_fail_promote() -> None:
    service, client = _service()
    client.get.side_effect = CogniteAPIError("Diagram not found", 404)

    service._verify_promoted_diagram_entities([_edge(service)])

    client.post.assert_not_called()
