"""Per-view instance space resolution for metadata index scans."""

from __future__ import annotations

from inverted_index.config import INDEX_FIELD_CONFIG, view_query_instance_spaces


def test_view_query_instance_spaces_empty_means_all() -> None:
    assert view_query_instance_spaces({"instance_spaces": []}) == [None]
    assert view_query_instance_spaces({}) == [None]


def test_view_query_instance_spaces_filters_configured() -> None:
    spaces = view_query_instance_spaces(
        {"instance_spaces": ["springfield_instances", "cdf_cdm"]}
    )
    assert spaces == ["springfield_instances", "cdf_cdm"]


def test_index_field_config_includes_instance_spaces() -> None:
    for view_cfg in INDEX_FIELD_CONFIG:
        assert "instance_spaces" in view_cfg
        assert isinstance(view_cfg["instance_spaces"], list)
