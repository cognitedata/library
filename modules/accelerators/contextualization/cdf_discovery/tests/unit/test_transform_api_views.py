"""Tests for transform API view property helpers."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from ui.server.transform_api import _find_view_in_space, _property_names_from_view


def test_property_names_from_view_uses_properties_dict() -> None:
    view = SimpleNamespace(properties={"name": object(), "description": object()})
    assert _property_names_from_view(view) == ["description", "name"]


def test_find_view_in_space_falls_back_to_listing() -> None:
    listed = SimpleNamespace(
        space="sp",
        external_id="Asset",
        version="v1",
        properties={"name": object()},
    )
    client = MagicMock()
    client.data_modeling.views.retrieve.return_value = []
    client.data_modeling.views.return_value = [[listed]]

    found = _find_view_in_space(client, space="sp", external_id="Asset", version="v1")
    assert found is listed
