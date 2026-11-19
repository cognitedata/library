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


def test_equipment_missing_a_field_is_listed_per_field() -> None:
    bare = equipment(SP, "bare", (SP, "pump"))
    bare["serial"], bare["manufacturer"] = None, None
    serial_only = equipment(SP, "no-serial", (SP, "pump"))
    serial_only["serial"], serial_only["manufacturer"] = None, "Acme"

    result = run(data(assets=hierarchy(), equipment_=[bare, serial_only]))

    assert listed(result, "equipmentMissingSerial") == {
        "bare": "equipment without serial number",
        "no-serial": "equipment without serial number",
    }
    assert listed(result, "equipmentMissingManufacturer") == {"bare": "equipment without manufacturer"}


def test_work_orders_that_are_not_completed_are_listed() -> None:
    done, open_ = work_order(SP, "done"), work_order(SP, "open")
    done["completed"], open_["completed"] = True, False

    result = run(data(assets=hierarchy(), work_orders=[done, open_]))

    assert listed(result, "incompleteWorkOrders") == {"open": "work order not completed"}


def test_annotations_that_are_not_approved_are_listed_with_their_status() -> None:
    from tiny_data import annotation

    def with_status(xid: str, status: str | None) -> dict[str, Any]:
        return {**annotation(SP, xid, (SP, "pump")), "status": status}

    annotations = [with_status("a1", "Approved"), with_status("a2", "Suggested"), with_status("a3", "Rejected")]
    annotations.append(with_status("a4", None))

    result = run(data(assets=hierarchy(), annotations=annotations))

    assert listed(result, "unapprovedAnnotations") == {
        "a2": "Suggested, confidence 0.9",
        "a3": "Rejected, confidence 0.9",
        "a4": "no status, confidence 0.9",
    }
