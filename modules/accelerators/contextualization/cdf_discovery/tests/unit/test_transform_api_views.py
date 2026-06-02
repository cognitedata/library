"""Tests for transform API view property helpers."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from ui.server.transform_api import (
    _delete_raw_table_if_exists,
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


def test_delete_raw_table_if_exists_reports_deleted() -> None:
    client = MagicMock()
    out = _delete_raw_table_if_exists(client, "db", "cohort__incremental")
    assert out == {
        "raw_db": "db",
        "raw_table": "cohort__incremental",
        "status": "deleted",
    }
    client.raw.tables.delete.assert_called_once_with("db", "cohort__incremental")


def test_delete_raw_table_if_exists_reports_not_found_for_404() -> None:
    class _Err(Exception):
        code = 404

    client = MagicMock()
    client.raw.tables.delete.side_effect = _Err("not found")
    out = _delete_raw_table_if_exists(client, "db", "cohort__file_state")
    assert out == {
        "raw_db": "db",
        "raw_table": "cohort__file_state",
        "status": "not_found",
    }


def test_delete_raw_table_if_exists_reports_error_for_non_404() -> None:
    class _Err(Exception):
        code = 500

    client = MagicMock()
    client.raw.tables.delete.side_effect = _Err("boom")
    out = _delete_raw_table_if_exists(client, "db", "cohort__file_state")
    assert out["raw_db"] == "db"
    assert out["raw_table"] == "cohort__file_state"
    assert out["status"] == "error"
    assert "boom" in str(out.get("error"))
