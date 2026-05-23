"""Tests for hierarchy walks and naming."""

from governance_build.context import (
    instance_space_external_id,
    scope_id_to_snake,
    scope_tree_folder_parts,
    top_level_scope_folder,
)
from governance_build.hierarchy import (
    collect_scope_rows,
    parse_levels,
    scope_binding_from_row,
    synthetic_scope_binding,
)


def test_scope_id_to_snake():
    assert scope_id_to_snake("SITE_A__UNIT_1") == "site_a_unit_1"


def test_instance_space_external_id():
    assert instance_space_external_id("SITE_A__X") == "inst_site_a_x"
    assert (
        instance_space_external_id("SITE_A__X", source_system_id="erp")
        == "inst_erp_site_a_x"
    )


def test_top_level_scope_folder():
    assert top_level_scope_folder(["SITE_A", "UNIT_1"]) == "site_a"


def test_scope_tree_folder_parts_nested():
    assert scope_tree_folder_parts(["SITE_A", "UNIT_1", "AREA_1"]) == [
        "site_a",
        "unit_1",
        "area_1",
    ]


def test_collect_all_nodes():
    block = {
        "levels": ["site", "unit"],
        "locations": [
            {
                "id": "S1",
                "locations": [{"id": "U1"}, {"id": "U2"}],
            }
        ],
    }
    assert parse_levels(block) == ["site", "unit"]
    rows = collect_scope_rows(block, nodes_mode="all")
    assert len(rows) == 3
    binding = scope_binding_from_row(["site", "unit"], rows[0])
    assert binding.scope_id == "S1"
    assert binding.segments == ["S1"]
    assert binding.path[0].level == "site"
    assert binding.path[0].segment_id == "S1"


def test_collect_leaves_only():
    block = {
        "levels": ["site", "unit"],
        "locations": [
            {
                "id": "S1",
                "locations": [{"id": "U1"}, {"id": "U2"}],
            }
        ],
    }
    rows = collect_scope_rows(block, nodes_mode="leaves")
    assert len(rows) == 2
    assert rows[0][0] == "S1__U1"


def test_synthetic_scope_binding():
    b = synthetic_scope_binding(
        ["site"], scope_id="org", name="Org-wide", description="All sites"
    )
    assert b.scope_id == "org"
    assert b.path[0].name == "Org-wide"
