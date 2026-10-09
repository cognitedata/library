"""Promote fails on API errors it cannot recover from and never loses annotations on a failed search."""

from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.data_modeling import EdgeId
from cognite.client.exceptions import CogniteAPIError
from services.entity_search_service import EntitySearchService
from services.promote_service import GeneralPromoteService
from test_promote_relocate import ASSET_LINK, _config


def _edge(view_id: object, text: str | None) -> MagicMock:
    edge = MagicMock()
    edge.space = "patterns"
    edge.external_id = f"pattern:file:{text}"
    edge.start_node.space = "plant_a"
    edge.start_node.external_id = "PID-1"
    edge.type.external_id = ASSET_LINK
    edge.properties = {view_id: {"startNodeText": text, "tags": []}}
    return edge


def test_search_error_propagates_instead_of_reading_as_no_match() -> None:
    client = MagicMock()
    client.data_modeling.instances.search.side_effect = CogniteAPIError("unavailable", code=503)
    service = EntitySearchService(_config(), client, MagicMock())

    with pytest.raises(CogniteAPIError):
        service.find_global_entity(["P-101"], service.target_entities_view_id, "plant_a", "P-101")


def test_failed_search_does_not_delete_the_edge() -> None:
    config = _config()
    view_id = config.data_model_views.core_annotation_view.as_view_id()
    client = MagicMock()
    service = GeneralPromoteService(client, config, MagicMock(), MagicMock(), MagicMock(), MagicMock())
    service._get_promote_candidates = MagicMock(return_value=[_edge(view_id, "P-101")])
    service._find_entity_with_cache = MagicMock(side_effect=CogniteAPIError("unavailable", code=503))

    with pytest.raises(CogniteAPIError):
        service.run()

    client.data_modeling.instances.delete.assert_not_called()


def test_permission_error_on_candidates_is_raised(monkeypatch: pytest.MonkeyPatch) -> None:
    service = GeneralPromoteService(MagicMock(), _config(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    service._get_promote_candidates = MagicMock(side_effect=CogniteAPIError("forbidden", code=403))

    with pytest.raises(CogniteAPIError):
        service.run()


def test_transient_error_on_candidates_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    import services.promote_service as promote_module

    monkeypatch.setattr(promote_module.time, "sleep", lambda seconds: None)
    service = GeneralPromoteService(MagicMock(), _config(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    service._get_promote_candidates = MagicMock(side_effect=CogniteAPIError("busy", code=429))

    assert service.run() is None


def test_edge_without_text_is_rejected_so_it_is_not_selected_again() -> None:
    config = _config()
    view_id = config.data_model_views.core_annotation_view.as_view_id()
    edge = _edge(view_id, None)
    client = MagicMock()
    client.raw.rows.retrieve.return_value = None
    service = GeneralPromoteService(client, config, MagicMock(), MagicMock(), MagicMock(), MagicMock())
    service._get_promote_candidates = MagicMock(return_value=[edge])

    service.run()

    if service.delete_rejected_edges:
        client.data_modeling.instances.delete.assert_called_once_with(edges=[EdgeId(edge.space, edge.external_id)])
    else:
        applied = client.data_modeling.instances.apply.call_args.kwargs["edges"]
        assert [e.external_id for e in applied] == [edge.external_id]
