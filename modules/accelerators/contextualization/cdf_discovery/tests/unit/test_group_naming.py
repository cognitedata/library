from governance_build.group_naming import (
    default_group_name,
    enrich_groups_naming_context,
    resolve_group_name,
)
from governance_build.dimensions_registry import default_naming_dimensions


def test_default_group_name_cdf_pattern():
    assert default_group_name(data_type="asset", location_id="site_a", access_type_id="read") == (
        "gp_asset_site_a_read"
    )


def test_enrich_groups_naming_context_location_and_access_type():
    ctx = {"scope_id": "SITE_A", "scope_id_snake": "site_a", "site_id": "SITE_A"}
    combo = {"access_type": {"id": "extractor", "name": "Extractor"}}
    out = enrich_groups_naming_context(
        ctx,
        data_type="asset",
        levels=["site", "unit"],
        combine_names=["access_type"],
        combo=combo,
    )
    assert out["data_type_id"] == "asset"
    assert out["location_id"] == "site_a"
    assert out["access_type_id"] == "extractor"
    assert out["access_level_id"] == "extractor"


def test_resolve_group_name_from_template():
    ctx = {"site_id": "SITE_A", "scope_id_snake": "site_a"}
    combo = {"access_type": {"id": "processing"}}
    name = resolve_group_name(
        template="gp_{{ data_type_id }}_{{ location_id }}_{{ access_type_id }}",
        ctx=ctx,
        data_type="asset",
        levels=["site"],
        combine_names=["access_type"],
        combo=combo,
    )
    assert name == "gp_asset_site_a_processing"


def test_default_naming_dimensions_keys():
    dims = default_naming_dimensions()
    for key in ("data_type", "source", "pipeline_type", "operation_type", "access_type"):
        assert key in dims
        assert dims[key]["naming_element"] == key
    assert len(dims["source"]["items"]) >= 6
    assert {it["id"] for it in dims["pipeline_type"]["items"]} == {"src", "ctx", "uc"}
