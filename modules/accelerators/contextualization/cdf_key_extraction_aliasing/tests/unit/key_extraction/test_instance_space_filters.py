"""Optional instance_space and node-scoped DM filters."""

from __future__ import annotations

from cognite.client.data_classes.data_modeling.ids import ViewId

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.config import (
    EntityType,
    FilterConfig,
    SourceViewConfig,
    ViewPropertyConfig,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.DataStructures import (
    FilterOperator,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.dm_filter_utils import (
    property_reference_for_filter,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.main import (
    _source_view_matches_instance_space,
)


def test_property_reference_for_filter_node_vs_view() -> None:
    vid = ViewId(space="vs", external_id="MyView", version="v1")
    assert property_reference_for_filter(vid, "space", "node") == ("node", "space")
    ref = property_reference_for_filter(vid, "title", "view")
    assert ref == ("vs", "MyView/v1", "title")


def test_filter_config_node_space_equals_dump() -> None:
    f = FilterConfig(
        operator=FilterOperator.EQUALS,
        target_property="space",
        values="sp_x",
        property_scope="node",
    )
    vp = ViewPropertyConfig(
        schema_space="vs",
        instance_space=None,
        external_id="V",
        version="v1",
        search_property="",
    )
    flt = f.as_filter(vp)
    assert flt.dump() == {"equals": {"property": ("node", "space"), "value": "sp_x"}}


def test_source_view_config_instance_space_optional() -> None:
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
    assert cfg.instance_space is None
    built = cfg.build_filter()
    assert "in" in built.dump()


def test_cli_instance_space_filter_matches_node_filter() -> None:
    v_field = {"instance_space": "sp_enterprise_schema"}
    assert _source_view_matches_instance_space(v_field, "sp_enterprise_schema")
    v_node = {
        "filters": [
            {
                "property_scope": "node",
                "target_property": "space",
                "operator": "IN",
                "values": ["sp_a", "sp_b"],
            }
        ]
    }
    assert _source_view_matches_instance_space(v_node, "sp_b")
    assert not _source_view_matches_instance_space(v_node, "sp_c")
