"""Tests for recursive source view filter building (AND/OR/NOT, PREFIX, RANGE)."""

from __future__ import annotations

from cognite.client.data_classes.data_modeling.ids import ViewId

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.source_view_filter_build import (
    build_source_view_query_filter,
    build_source_view_user_filters,
    filter_dict_to_dm,
)


def test_filter_dict_nested_and_or_dump() -> None:
    vid = ViewId(space="vs", external_id="V", version="v1")
    node = {
        "and": [
            {"operator": "EQUALS", "target_property": "equipmentType", "values": ["pump"]},
            {
                "or": [
                    {"operator": "EQUALS", "target_property": "zone", "values": ["A"]},
                    {"operator": "EQUALS", "target_property": "zone", "values": ["B"]},
                ]
            },
        ]
    }
    f = filter_dict_to_dm(vid, node)
    dumped = f.dump()
    assert "and" in dumped


def test_filter_dict_not_leaf() -> None:
    vid = ViewId(space="vs", external_id="V", version="v1")
    node = {
        "not": {"operator": "EQUALS", "target_property": "deprecated", "values": [True]}
    }
    f = filter_dict_to_dm(vid, node)
    dumped = f.dump()
    assert "not" in dumped


def test_build_query_includes_has_data() -> None:
    vid = ViewId(space="vs", external_id="V", version="v1")
    f = build_source_view_query_filter(vid, [])
    dumped = f.dump()
    assert "hasData" in dumped


def test_build_user_filters_matches_pydantic_leaf() -> None:
    from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.config import (
        EntityType,
        FilterConfig,
        SourceViewConfig,
    )
    from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.DataStructures import (
        FilterOperator,
    )

    cfg = SourceViewConfig(
        view_external_id="CogniteAsset",
        view_space="cdf_cdm",
        view_version="v1",
        entity_type=EntityType.ASSET,
        batch_size=10,
        resource_property="name",
        include_properties=[],
        instance_space=None,
        filters=[
            FilterConfig(
                operator=FilterOperator.IN,
                target_property="space",
                values=["a", "b"],
                property_scope="node",
            )
        ],
    )
    a = cfg.build_filter().dump()
    b = build_source_view_user_filters(cfg.as_view_id(), cfg.filters or []).dump()
    assert a == b
