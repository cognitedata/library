"""Instance space id template and defaults."""

from governance_build.space_naming import (
    default_instance_space_external_id,
    merge_list_combo_into_context,
    resolve_instance_space_external_id,
)


def test_default_matches_cdf_inst_data_type_source_scope():
    combo = {"source": {"id": "erp", "name": "ERP"}}
    out = default_instance_space_external_id(
        "SITE_A", combo, ["source"], data_type="dm"
    )
    assert out == "inst_dm_erp_site_a"


def test_default_scope_only():
    assert (
        default_instance_space_external_id("SITE_A__U1", {}, [], data_type="asset")
        == "inst_asset_site_a_u1"
    )


def test_jinja_template_custom_order():
    ctx = merge_list_combo_into_context(
        {"scope_id": "SITE_A", "scope_id_snake": "site_a", "scope_snake": "site_a"},
        {"source_system": {"id": "erp", "name": "ERP"}},
    )
    out = resolve_instance_space_external_id(
        template="inst_{{ scope_id_snake }}_{{ source_system_id }}",
        ctx=ctx,
        scope_id="SITE_A",
        combo={"source_system": {"id": "erp"}},
        combine_names=["source_system"],
    )
    assert out == "inst_site_a_erp"
