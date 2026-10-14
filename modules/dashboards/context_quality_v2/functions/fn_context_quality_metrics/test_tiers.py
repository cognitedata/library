"""Tier assignment: an asset's tier comes from its own type mapping, other entities inherit it, nothing is invented."""

from typing import Any

from cq.config import DEFAULT_CONFIG, build_config
from cq.metrics import compute
from sample_rules import SAMPLE_RULES
from tiny_data import NOW, REF, annotation, asset, data, equipment, file, notification, object3d, timeseries, work_order

MAPPING = {"tierByTypeCode": {"pump": 1, "valve": 2}}
SP = "sp_a"


def run(loaded: dict[str, Any], overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    return compute(loaded, build_config(overrides), {}, now=NOW)  # type: ignore[arg-type]


def tiers(result: dict[str, Any], metric: str, sub: str | None = None) -> dict[int | None, list[int]]:
    """tier -> [eligible, passing] for one metric."""
    out: dict[int | None, list[int]] = {}
    for key, value in result["agg"].items():
        if key[0] == metric and key[8] == sub and value[0]:
            row = out.setdefault(key[7], [0, 0])
            row[0] += value[0]
            row[1] += value[1]
    return out


def assets_by_type() -> list[dict[str, Any]]:
    return [
        asset(SP, "site"),
        asset(SP, "pump", parent=(SP, "site"), type_code="pump"),
        asset(SP, "valve", parent=(SP, "site"), type_code="valve"),
        asset(SP, "widget", parent=(SP, "site"), type_code="widget"),  # a type the mapping does not know
        asset(SP, "untyped", parent=(SP, "site")),
    ]


def test_an_asset_takes_its_tier_from_its_own_type_mapping() -> None:
    loaded = data(assets=assets_by_type())

    run(loaded, MAPPING)

    assert {a["id"]: a["tier"] for a in loaded["assets"]} == {
        "site": None,
        "pump": 1,
        "valve": 2,
        "widget": None,
        "untyped": None,
    }


def test_an_unmapped_or_untyped_asset_is_unassigned_not_tier_3() -> None:
    result = run(data(assets=assets_by_type()), MAPPING)

    assert tiers(result, "orphanRate") == {1: [1, 1], 2: [1, 1], None: [3, 3]}
    assert 3 not in {key[7] for key in result["agg"]}
    assert {item["g"]["tier"] for item in result["drilldown"] if item["id"] in ("widget", "untyped")} == {None}


def test_tier_3_is_measured_only_where_it_is_configured() -> None:
    loaded = data(assets=assets_by_type())

    result = run(loaded, {"tierByTypeCode": {"pump": 1, "widget": 3}})

    assert {a["id"]: a["tier"] for a in loaded["assets"]}["widget"] == 3
    assert tiers(result, "orphanRate") == {1: [1, 1], 3: [1, 1], None: [3, 3]}


def test_other_entities_inherit_the_tier_of_the_asset_they_link_to() -> None:
    pump, valve, widget = (SP, "pump"), (SP, "valve"), (SP, "widget")
    loaded = data(
        assets=assets_by_type(),
        equipment_=[equipment(SP, "e1", pump), equipment(SP, "e2", valve), equipment(SP, "e3", widget)],
        timeseries_=[timeseries(SP, "t1", pump)],
        files=[file(SP, "f1", valve)],
        work_orders=[work_order(SP, "w1", pump)],
        notifications=[notification(SP, "n1", valve)],
        objects3d=[object3d(SP, "o1")],
        annotations=[annotation(SP, "an1", pump)],
    )
    loaded["assets"][1]["object3d"] = (SP, "o1")

    result = run(loaded, MAPPING)

    assert tiers(result, "associationRate") == {1: [1, 1], 2: [1, 1], None: [1, 1]}
    assert tiers(result, "tsToAssetRate") == {1: [1, 1]}
    assert tiers(result, "fileToAssetRate") == {2: [1, 1]}
    assert tiers(result, "woToAssetRate") == {1: [1, 1]}
    assert tiers(result, "notificationToAssetRate") == {2: [1, 1]}
    assert tiers(result, "threeDToAssetRate") == {1: [1, 1]}
    assert tiers(result, "annotationCount") == {1: [1, 1]}


def test_an_entity_without_a_linked_asset_has_no_tier() -> None:
    loaded = data(
        assets=assets_by_type(),
        equipment_=[equipment(SP, "e1")],
        timeseries_=[timeseries(SP, "t1")],
        files=[file(SP, "f1")],
        work_orders=[work_order(SP, "w1")],
        notifications=[notification(SP, "n1")],
        objects3d=[object3d(SP, "o1")],
    )

    result = run(loaded, MAPPING)

    for metric in ("associationRate", "tsToAssetRate", "fileToAssetRate", "woToAssetRate", "notificationToAssetRate"):
        assert tiers(result, metric) == {None: [1, 0]}, metric
    assert tiers(result, "threeDToAssetRate") == {None: [1, 0]}
    assert 3 not in {key[7] for key in result["agg"]}
    assert {item["g"]["tier"] for item in result["drilldown"] if item["id"] in ("e1", "t1", "f1", "w1", "n1")} == {None}


def test_an_entity_linked_to_an_unmapped_asset_has_no_tier() -> None:
    loaded = data(assets=assets_by_type(), equipment_=[equipment(SP, "e1", (SP, "widget"))])

    assert tiers(run(loaded, MAPPING), "associationRate") == {None: [1, 1]}


def test_an_entity_linked_to_an_asset_that_was_not_loaded_has_no_tier() -> None:
    loaded = data(assets=assets_by_type(), timeseries_=[timeseries(SP, "t1", ("sp_elsewhere", "pump"))])

    assert tiers(run(loaded, MAPPING), "tsToAssetRate") == {None: [1, 1]}


def test_coverage_metrics_only_cover_mapped_tier_1_and_2_assets() -> None:
    result = run(data(assets=assets_by_type()), MAPPING)

    assert tiers(result, "monitoringCoverage") == {1: [1, 0], 2: [1, 0]}
    assert tiers(result, "asset3dCoverage") == {1: [1, 0]}


def test_the_asset_classification_rows_carry_the_assets_tier() -> None:
    loaded = data(assets=assets_by_type())
    loaded["assets"][1]["type"] = (REF, "type-pump")
    loaded["class_labels"]["assetType"] = {(REF, "type-pump"): "Pump", (REF, "type-widget"): "Widget"}

    result = run(loaded, MAPPING)

    assert {key[9] for key in result["classifications"]} == {1, 2, None}


def test_an_empty_mapping_assigns_no_tier_anywhere() -> None:
    loaded = data(assets=assets_by_type(), equipment_=[equipment(SP, "e1", (SP, "pump"))])

    result = run(loaded, {"tierByTypeCode": {}})

    assert {a["tier"] for a in loaded["assets"]} == {None}
    assert {key[7] for key in result["agg"]} == {None}
    assert tiers(result, "monitoringCoverage") == {}


def test_the_production_defaults_carry_no_project_specific_tier_mapping() -> None:
    assert DEFAULT_CONFIG["tierByTypeCode"] == {}
    assert build_config(None)["tierByTypeCode"] == {}


def test_the_sample_rules_reproduces_the_sample_tiers() -> None:
    loaded = data(
        assets=[asset(SP, "a", type_code="pump"), asset(SP, "b", type_code="sensor")], type_codes=("pump", "sensor")
    )

    run(loaded, SAMPLE_RULES)

    assert [a["tier"] for a in loaded["assets"]] == [1, 2]


def test_tier_assignment_does_not_depend_on_the_order_of_the_assets() -> None:
    forward, backward = data(assets=assets_by_type()), data(assets=list(reversed(assets_by_type())))

    assert run(forward, MAPPING)["agg"] == run(backward, MAPPING)["agg"]
