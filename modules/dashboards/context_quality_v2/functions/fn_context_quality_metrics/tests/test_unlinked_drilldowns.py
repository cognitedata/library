"""Every population a link metric counts as unlinked can be listed: each unlinked entity gets a drill-down row."""

from typing import Any

from cq.config import build_config
from cq.metrics import compute
from tiny_data import NOW, asset, data, equipment, notification, object3d, work_order

SP = "sp_a"


def run(loaded: dict[str, Any], overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    return compute(loaded, build_config(overrides), {}, now=NOW)  # type: ignore[arg-type]


def listed(result: dict[str, Any], metric: str) -> dict[str, str]:
    """entity id -> detail, for one drill-down metric."""
    return {item["id"]: item["detail"] for item in result["drilldown"] if item["metric"] == metric}


def hierarchy() -> list[dict[str, Any]]:
    return [
        asset(SP, "site"),
        asset(SP, "pump", parent=(SP, "site"), type_code="pump"),
        asset(SP, "valve", parent=(SP, "site"), type_code="valve"),
    ]


def test_a_3d_object_with_no_asset_is_listed() -> None:
    loaded = data(assets=hierarchy(), objects3d=[object3d(SP, "linked"), object3d(SP, "loose")])
    loaded["assets"][1]["object3d"] = (SP, "linked")

    result = run(loaded)

    assert listed(result, "unlinked3dObjects") == {"loose": "3D object without asset"}


def test_a_notification_with_no_work_order_is_listed() -> None:
    loaded = data(assets=hierarchy(), notifications=[notification(SP, "n1"), notification(SP, "n2")])
    loaded["notifications"][0]["work_order"] = (SP, "w1")
    loaded["work_orders"] = [work_order(SP, "w1")]

    result = run(loaded)

    assert listed(result, "notificationsWithoutWorkOrder") == {"n2": "notification without work order"}


def test_an_asset_with_no_equipment_is_listed_and_a_root_is_not() -> None:
    loaded = data(assets=hierarchy(), equipment_=[equipment(SP, "e1", (SP, "pump"))])

    result = run(loaded)

    assert listed(result, "assetsWithoutEquipment") == {"valve": "asset without equipment"}


def test_with_tiering_on_only_the_tier_1_and_2_assets_without_equipment_are_listed() -> None:
    loaded = data(assets=[*hierarchy(), asset(SP, "widget", parent=(SP, "site"), type_code="widget")])

    result = run(loaded, {"tieringEnabled": True, "tierByTypeCode": {"pump": 1, "valve": 2}})

    assert listed(result, "assetsWithoutEquipment") == {
        "pump": "tier 1 asset without equipment",
        "valve": "tier 2 asset without equipment",
    }
