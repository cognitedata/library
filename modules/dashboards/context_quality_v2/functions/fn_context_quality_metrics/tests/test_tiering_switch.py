"""Tiering is off by default: no asset gets a tier and the coverage metrics measure every asset; a project turns it on."""

from typing import Any

import pytest
from cq.config import DEFAULT_CONFIG, build_config
from cq.metrics import compute
from tiny_data import NOW, asset, data, equipment, notification, object3d, timeseries, work_order

SP = "sp_a"
MAPPING = {"pump": 1, "valve": 2}


def run(loaded: dict[str, Any], overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    return compute(loaded, build_config(overrides), {}, now=NOW)  # type: ignore[arg-type]


def loaded_assets() -> dict[str, Any]:
    return data(
        assets=[
            asset(SP, "site"),
            asset(SP, "pump", parent=(SP, "site"), type_code="pump"),
            asset(SP, "valve", parent=(SP, "site"), type_code="valve"),
            asset(SP, "untyped", parent=(SP, "site")),
        ],
        work_orders=[work_order(SP, "w1", (SP, "pump"))],
        notifications=[notification(SP, "n1")],
        equipment_=[equipment(SP, "e1", (SP, "pump"))],
        timeseries_=[timeseries(SP, "t1", (SP, "valve"))],
        objects3d=[object3d(SP, "o1")],
    )


def totals(result: dict[str, Any], metric: str) -> list[int]:
    elig = passing = 0
    for key, value in result["agg"].items():
        if key[0] == metric:
            elig, passing = elig + value[0], passing + value[1]
    return [elig, passing]


def warnings(result: dict[str, Any], code: str) -> list[dict[str, Any]]:
    return [w for w in result["warnings"] if w["code"] == code]


def test_tiering_is_off_by_default() -> None:
    assert DEFAULT_CONFIG["tieringEnabled"] is False
    assert build_config(None)["tieringEnabled"] is False


@pytest.mark.parametrize("bad", ["true", 1, None])
def test_the_switch_must_be_a_boolean(bad: Any) -> None:
    with pytest.raises(ValueError, match="tieringEnabled"):
        build_config({"tieringEnabled": bad})


def test_while_off_no_asset_gets_a_tier_even_if_a_mapping_is_set() -> None:
    loaded = loaded_assets()

    result = run(loaded, {"tierByTypeCode": MAPPING})

    assert {a["id"]: a["tier"] for a in loaded["assets"]} == {
        "site": None,
        "pump": None,
        "valve": None,
        "untyped": None,
    }
    assert {key[7] for key in result["agg"]} == {None}
    assert {item["g"]["tier"] for item in result["drilldown"]} <= {None}


def test_while_off_the_coverage_metrics_measure_every_asset_that_is_not_a_root() -> None:
    result = run(loaded_assets())

    assert totals(result, "monitoringCoverage") == [3, 1]
    assert totals(result, "assetEquipmentCoverage") == [3, 1]
    assert totals(result, "asset3dCoverage") == [3, 0]
    assert {
        item["detail"] for item in result["drilldown"] if item["metric"] in ("tier1CoverageGaps", "unlinkedTier1")
    } == {
        "asset without time series",
        "asset without 3D object",
    }


def test_while_off_every_work_order_without_a_notification_is_listed() -> None:
    result = run(loaded_assets())

    items = [item for item in result["drilldown"] if item["metric"] == "woWithoutNotification"]
    assert [item["detail"] for item in items] == ["work order without notification"]


def test_while_off_there_are_no_tier_warnings_and_no_tier_metric_is_not_applicable() -> None:
    result = run(loaded_assets())

    assert [w for w in result["warnings"] if w.get("subject") == "tierByTypeCode"] == []
    assert warnings(result, "unmapped-asset-types") == []
    assert [
        w for w in warnings(result, "metric-not-applicable") if w.get("reason") == "mapping-empty:tierByTypeCode"
    ] == []
    assert not [k for k, v in result["agg"].items() if v[0] == 0 and k[0] in ("monitoringCoverage", "asset3dCoverage")]


def test_while_off_a_tier_mapping_that_is_set_is_reported_as_ignored() -> None:
    result = run(loaded_assets(), {"tierByTypeCode": MAPPING})

    (note,) = warnings(result, "tiering-off")
    assert note["subject"] == "tierByTypeCode"
    assert warnings(run(loaded_assets()), "tiering-off") == []


def test_when_turned_on_tiers_come_from_the_mapping_and_the_coverage_metrics_cover_tier_1_and_2_only() -> None:
    loaded = loaded_assets()

    result = run(loaded, {"tieringEnabled": True, "tierByTypeCode": MAPPING})

    assert {a["id"]: a["tier"] for a in loaded["assets"]}["pump"] == 1
    assert totals(result, "monitoringCoverage") == [2, 1]
    assert warnings(result, "tiering-off") == []


def test_when_turned_on_without_a_mapping_the_tier_metrics_are_not_applicable_as_before() -> None:
    result = run(loaded_assets(), {"tieringEnabled": True})

    assert "tierByTypeCode" in [w["subject"] for w in warnings(result, "mapping-empty")]
    assert {k[0] for k, v in result["agg"].items() if v[0] == 0} >= {"monitoringCoverage", "asset3dCoverage"}
