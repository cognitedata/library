"""Tests for transform API view property helpers."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from ui.server.transform_api import (
    _find_view_in_space,
    _property_names_from_view,
    _view_fields_from_view,
)


def test_property_names_from_view_uses_properties_dict() -> None:
    view = SimpleNamespace(properties={"name": object(), "description": object()})
    assert _property_names_from_view(view) == ["description", "name"]


def test_view_fields_from_view_mapped_property() -> None:
    prop_type = SimpleNamespace(type="text", list=False, nullable=True)
    mapped = SimpleNamespace(type=prop_type, source=None)
    view = SimpleNamespace(properties={"name": mapped})
    fields = _view_fields_from_view(view)
    assert len(fields) == 1
    assert fields[0]["name"] == "name"
    assert fields[0]["kind"] == "mapped"
    assert fields[0]["type"] == "text"
    assert fields[0]["nullable"] is True


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
