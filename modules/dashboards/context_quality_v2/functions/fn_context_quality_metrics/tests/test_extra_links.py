"""Extra links: the config file's `View.property` entries, parsed, counted per facility and written as rows."""

from typing import Any

import pytest
from cq.availability import extra_link_truncation_warnings
from cq.config import build_config
from cq.extra_links import ExtraLink, count_links, extra_facility_spaces, find_gaps, parse_extra_links
from cq.writer import extra_link_gap_rows, extra_link_rows

CORE = "cdf_cdm:CogniteCore:v1"


def test_a_list_of_models_with_view_dot_property_links_becomes_one_entry_per_link() -> None:
    raw = [
        {"model": CORE, "links": ["CogniteAsset.type", "CogniteEquipment.equipmentType"]},
        {"model": "my_space:MyModel:v2", "links": ["MyView.owner"]},
    ]

    assert parse_extra_links(raw) == [
        {"model": CORE, "view": "CogniteAsset", "property": "type"},
        {"model": CORE, "view": "CogniteEquipment", "property": "equipmentType"},
        {"model": "my_space:MyModel:v2", "view": "MyView", "property": "owner"},
    ]


def test_no_extra_links_is_an_empty_list() -> None:
    assert parse_extra_links(None) == []
    assert parse_extra_links([]) == []


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ({"model": CORE, "links": []}, "must be a list"),
        (["CogniteAsset.type"], "must be a mapping"),
        ([{"links": ["CogniteAsset.type"]}], "model"),
        ([{"model": "cdf_cdm:CogniteCore", "links": ["CogniteAsset.type"]}], "space:externalId:version"),
        ([{"model": "a::v1", "links": ["CogniteAsset.type"]}], "space:externalId:version"),
        ([{"model": CORE}], "links"),
        ([{"model": CORE, "links": []}], "links"),
        ([{"model": CORE, "links": ["CogniteAsset"]}], "View.property"),
        ([{"model": CORE, "links": ["CogniteAsset.type.x"]}], "View.property"),
        ([{"model": CORE, "links": [".type"]}], "View.property"),
        ([{"model": CORE, "links": [3]}], "View.property"),
        ([{"model": CORE, "links": ["CogniteAsset.type", "CogniteAsset.type"]}], "twice"),
    ],
)
def test_a_malformed_file_is_rejected_with_a_message_that_names_the_problem(raw: Any, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_extra_links(raw)


def link(view: str, prop: str, model: str = CORE) -> ExtraLink:
    return {"model": model, "view": view, "property": prop}


def ref(xid: str) -> dict[str, str]:
    return {"space": "ref", "externalId": xid}


FACILITIES = {"plant_a": "Plant A", "plant_b": "Plant B"}


def test_a_link_counts_the_instances_where_it_is_set_per_facility() -> None:
    nodes = {
        (CORE, "CogniteAsset"): [
            ("plant_a", "node", {"type": ref("t1")}),
            ("plant_a", "node", {"type": None}),
            ("plant_a", "node", {}),
            ("plant_b", "node", {"type": ref("t2")}),
        ]
    }

    counts = count_links([link("CogniteAsset", "type")], nodes, FACILITIES)

    assert counts == {
        (CORE, "CogniteAsset", "type", "plant_a"): [3, 1, 2],
        (CORE, "CogniteAsset", "type", "plant_b"): [1, 1, 0],
    }


def test_a_list_property_is_set_only_when_it_holds_a_reference() -> None:
    nodes = {
        (CORE, "CogniteTimeSeries"): [
            ("plant_a", "node", {"assets": [ref("a1"), ref("a2")]}),
            ("plant_a", "node", {"assets": []}),
            ("plant_a", "node", {"assets": None}),
            ("plant_a", "node", {"assets": [{"space": "ref"}]}),
        ]
    }

    counts = count_links([link("CogniteTimeSeries", "assets")], nodes, FACILITIES)

    assert counts[(CORE, "CogniteTimeSeries", "assets", "plant_a")] == [4, 1, 3]


def test_instances_outside_the_facilities_are_not_counted() -> None:
    nodes = {(CORE, "CogniteAsset"): [("somewhere_else", "node", {}), ("plant_a", "node", {"type": ref("t1")})]}

    counts = count_links([link("CogniteAsset", "type")], nodes, FACILITIES)

    assert counts[(CORE, "CogniteAsset", "type", "plant_a")] == [1, 1, 0]
    assert all(key[3] in FACILITIES for key in counts)


def test_a_facility_with_no_instances_of_the_view_gets_no_row_for_it() -> None:
    nodes = {(CORE, "CogniteAsset"): [("plant_a", "node", {"type": ref("t")})]}

    counts = count_links([link("CogniteAsset", "type")], nodes, FACILITIES)

    assert counts == {(CORE, "CogniteAsset", "type", "plant_a"): [1, 1, 0]}


def test_a_view_with_no_instances_anywhere_still_shows_up_with_nothing_to_count() -> None:
    counts = count_links([link("CogniteAsset", "type")], {(CORE, "CogniteAsset"): []}, FACILITIES)

    assert counts == {
        (CORE, "CogniteAsset", "type", "plant_a"): [0, 0, 0],
        (CORE, "CogniteAsset", "type", "plant_b"): [0, 0, 0],
    }


def test_two_links_of_the_same_view_are_counted_from_the_same_instances() -> None:
    nodes = {(CORE, "CogniteAsset"): [("plant_a", "node", {"type": ref("t"), "assetClass": None})]}

    counts = count_links([link("CogniteAsset", "type"), link("CogniteAsset", "assetClass")], nodes, FACILITIES)

    assert counts[(CORE, "CogniteAsset", "type", "plant_a")] == [1, 1, 0]
    assert counts[(CORE, "CogniteAsset", "assetClass", "plant_a")] == [1, 0, 1]


def test_the_same_view_name_in_two_models_is_counted_separately() -> None:
    other = "other:Model:v1"
    nodes = {
        (CORE, "Asset"): [("plant_a", "node", {"type": ref("t")})],
        (other, "Asset"): [("plant_a", "node", {})],
    }

    counts = count_links([link("Asset", "type"), link("Asset", "type", other)], nodes, {"plant_a": "A"})

    assert counts[(CORE, "Asset", "type", "plant_a")] == [1, 1, 0]
    assert counts[(other, "Asset", "type", "plant_a")] == [1, 0, 1]


def test_rows_carry_the_raw_counts_and_name_the_model_view_and_property() -> None:
    counts = {(CORE, "CogniteAsset", "type", "plant_a"): [3, 1, 2]}

    ((key, props),) = extra_link_rows(counts, FACILITIES)

    assert key.startswith("xlink:")
    assert props == {
        "rowKind": "extraLink",
        "metricId": "type",
        "dimensionId": f"{CORE}/CogniteAsset",
        "reportingUnitId": "plant_a",
        "facilityId": "Plant A",
        "eligibleCount": 3,
        "passingCount": 1,
        "issueCount": 2,
    }


def test_a_link_with_nothing_to_count_is_not_applicable() -> None:
    ((_, props),) = extra_link_rows({(CORE, "CogniteAsset", "type", "plant_a"): [0, 0, 0]}, FACILITIES)

    assert props["notApplicable"] is True


def test_row_keys_are_unique_per_model_view_property_and_facility() -> None:
    counts = {
        (CORE, "CogniteAsset", "type", "plant_a"): [1, 1, 0],
        (CORE, "CogniteAsset", "type", "plant_b"): [1, 1, 0],
        (CORE, "CogniteAsset", "assetClass", "plant_a"): [1, 1, 0],
        ("o:M:v1", "CogniteAsset", "type", "plant_a"): [1, 1, 0],
    }

    keys = [key for key, _ in extra_link_rows(counts, FACILITIES)]

    assert len(set(keys)) == 4


def many(space: str, count: int) -> list[tuple[str, dict[str, Any]]]:
    return [(space, "node", {}) for _ in range(count)]


CFG = build_config({"schemaSpace": "metrics_schema", "instanceSpace": "metrics_data"})


def test_the_facilities_stay_the_core_spaces_when_the_views_live_in_them() -> None:
    nodes = {(CORE, "CogniteAsset"): many("plant_a", 12)}

    assert extra_facility_spaces(nodes, FACILITIES, CFG) == FACILITIES


def test_a_space_holding_enough_instances_of_a_configured_view_becomes_a_facility() -> None:
    nodes = {("o:Model:v1", "Ucp"): many("sp_ucp_north", 10) + many("plant_a", 11)}

    facilities = extra_facility_spaces(nodes, FACILITIES, CFG)

    assert facilities == {**FACILITIES, "sp_ucp_north": "sp_ucp_north"}


def test_the_spaces_of_every_configured_view_are_added() -> None:
    nodes = {("o:Model:v1", "Ucp"): many("sp_ucp", 10), ("o:Model:v1", "Spf"): many("sp_spf", 10)}

    assert set(extra_facility_spaces(nodes, FACILITIES, CFG)) == {*FACILITIES, "sp_ucp", "sp_spf"}


def test_the_instances_of_two_views_in_one_space_add_up_to_reach_the_minimum() -> None:
    nodes = {("o:M:v1", "A"): many("sp_x", 6), ("o:M:v1", "B"): many("sp_x", 4)}

    assert "sp_x" in extra_facility_spaces(nodes, FACILITIES, CFG)


def test_a_space_with_only_a_few_instances_is_not_a_facility() -> None:
    nodes = {("o:Model:v1", "Ucp"): many("sp_reference", 9)}

    assert extra_facility_spaces(nodes, FACILITIES, CFG) == FACILITIES


def test_the_excluded_and_the_metrics_spaces_never_become_facilities() -> None:
    nodes = {("o:Model:v1", "Ucp"): many("cdf_cdm", 50) + many("metrics_schema", 50) + many("metrics_data", 50)}

    assert extra_facility_spaces(nodes, FACILITIES, CFG) == FACILITIES


def test_a_new_facility_takes_its_display_name_from_facility_names() -> None:
    cfg = build_config({**CFG, "facilityNames": {"sp_ucp_north": "North"}})
    nodes = {("o:Model:v1", "Ucp"): many("sp_ucp_north", 10)}

    assert extra_facility_spaces(nodes, FACILITIES, cfg)["sp_ucp_north"] == "North"


def test_an_explicit_facility_list_is_the_only_set_of_facilities() -> None:
    cfg = build_config({**CFG, "facilitySpaces": {"plant_a": "A"}})
    nodes = {("o:Model:v1", "Ucp"): many("sp_ucp", 50)}

    assert extra_facility_spaces(nodes, {"plant_a": "A"}, cfg) == {"plant_a": "A"}


def test_the_core_facilities_are_never_dropped_or_renamed() -> None:
    assert extra_facility_spaces({}, FACILITIES, CFG) == FACILITIES


def test_the_instances_without_the_link_are_listed_with_their_name_or_external_id() -> None:
    nodes = {
        (CORE, "CogniteAsset"): [
            ("plant_a", "a1", {"type": ref("t1"), "name": "Set"}),
            ("plant_a", "a2", {"type": None, "name": "Pump P-101"}),
            ("plant_a", "a3", {}),
            ("plant_b", "a4", {"name": 7}),
            ("elsewhere", "a5", {}),
        ]
    }

    gaps = find_gaps([link("CogniteAsset", "type")], nodes, FACILITIES)

    assert [(gap["space"], gap["external_id"], gap["label"]) for gap in gaps] == [
        ("plant_a", "a2", "Pump P-101"),
        ("plant_a", "a3", "a3"),
        ("plant_b", "a4", "a4"),
    ]
    assert {(gap["model"], gap["view"], gap["property"]) for gap in gaps} == {(CORE, "CogniteAsset", "type")}


def test_a_list_link_with_no_reference_is_a_gap() -> None:
    nodes = {
        (CORE, "CogniteTimeSeries"): [("plant_a", "t1", {"assets": []}), ("plant_a", "t2", {"assets": [ref("x")]})]
    }

    gaps = find_gaps([link("CogniteTimeSeries", "assets")], nodes, FACILITIES)

    assert [gap["external_id"] for gap in gaps] == ["t1"]


def gap_rows_for(count: int, cap: int) -> list[Any]:
    nodes = {(CORE, "CogniteAsset"): [("plant_a", f"a{i}", {}) for i in range(count)]}
    return extra_link_gap_rows(find_gaps([link("CogniteAsset", "type")], nodes, FACILITIES), FACILITIES, cap)


def test_a_gap_row_names_the_instance_the_view_the_property_and_the_facility() -> None:
    (key, props), *_ = gap_rows_for(1, 500)

    assert key.startswith("xgap:")
    assert props == {
        "rowKind": "extraLinkGap",
        "metricId": "type",
        "dimensionId": f"{CORE}/CogniteAsset",
        "reportingUnitId": "plant_a",
        "facilityId": "Plant A",
        "entityId": "a0",
        "entityLabel": "a0",
        "detail": "link not set",
    }


def test_the_gap_rows_are_cut_at_the_cap_per_link_and_facility_and_have_distinct_keys() -> None:
    rows = gap_rows_for(7, 5)

    assert len(rows) == 5
    assert len({key for key, _ in rows}) == 5


def test_a_list_cut_at_the_cap_is_reported_with_what_was_dropped() -> None:
    nodes = {(CORE, "CogniteAsset"): [("plant_a", f"a{i}", {}) for i in range(7)] + [("plant_b", "b0", {})]}
    gaps = find_gaps([link("CogniteAsset", "type")], nodes, FACILITIES)

    (warning,) = extra_link_truncation_warnings(gaps, 5)

    assert warning["code"] == "drilldown-truncated"
    assert (warning["subject"], warning["limit"], warning["dropped"], warning["units"]) == (
        "CogniteAsset.type",
        5,
        2,
        1,
    )
    assert extra_link_truncation_warnings(gaps, 500) == []
