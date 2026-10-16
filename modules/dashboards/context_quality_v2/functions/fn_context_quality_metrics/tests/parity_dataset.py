"""A small, hand-built dataset that exercises every metric branch of the function.

It deliberately imports nothing from the function: the same file is used to produce the expected output from the
original implementation and to feed the port, so both see exactly the same input.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

NOW = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)
REF, A, B = "sp_ref", "sp_a", "sp_b"
FACILITIES = {A: "alpha", B: "beta"}


def _asset(
    space: str, xid: str, name: str, *, parent=None, type_=None, aclass=None, tags=(), object3d=None
) -> dict[str, Any]:
    return {
        "space": space,
        "id": xid,
        "name": name,
        "tags": list(tags),
        "parent": parent,
        "type": type_,
        "aclass": aclass,
        "object3d": object3d,
    }


def _ref(space: str, xid: str) -> tuple[str, str]:
    return (space, xid)


def build_dataset() -> tuple[dict[str, Any], dict[tuple[str, str], datetime | None], datetime]:
    root = _ref(REF, "root")
    t_pump, t_sensor, t_valve, t_widget = (_ref(REF, f"type-{n}") for n in ("pump", "sensor", "valve", "widget"))
    c_rot, c_inst = _ref(REF, "class-rot"), _ref(REF, "class-inst")
    assets = [
        _asset(A, "site-a", "Site A", parent=root, tags=["division:north"]),
        _asset(A, "area-a", "Area A", parent=_ref(A, "site-a"), type_=t_widget, tags=["division:north"]),
        _asset(
            A,
            "p-1",
            "P-1",
            parent=_ref(A, "area-a"),
            type_=t_pump,
            aclass=c_rot,
            tags=["division:north"],
            object3d=_ref(A, "o-1"),
        ),
        _asset(
            A,
            "p-2",
            "P-2",
            parent=_ref(A, "area-a"),
            type_=t_pump,
            aclass=c_rot,
            tags=["division:north", "lifecycle:retired"],
        ),
        _asset(A, "p-2-child", "P-2 child", parent=_ref(A, "p-2"), type_=t_valve, tags=["division:north"]),
        _asset(A, "s-1", "S-1", parent=_ref(A, "area-a"), type_=t_sensor, aclass=c_inst, tags=["division:north"]),
        _asset(A, "s-2", "S-2", parent=_ref(A, "area-a"), type_=t_sensor, tags=["division:south"]),
        _asset(A, "untyped", "Untyped", parent=_ref(A, "area-a")),
        _asset(B, "site-b", "Site B", parent=root),
        _asset(B, "p-3", "P-3", parent=_ref(B, "site-b"), type_=t_pump, aclass=c_rot, object3d=_ref(B, "o-2")),
        _asset(B, "orphan", "Orphan", type_=t_valve),
        _asset(B, "stray", "Stray", type_=t_widget, tags=["lifecycle:decommissioned"]),
    ]
    previous = _ref(B, "site-b")
    for level in range(1, 10):  # a chain deeper than maxDepth
        assets.append(_asset(B, f"deep-{level}", f"Deep {level}", parent=previous, type_=t_valve))
        previous = _ref(B, f"deep-{level}")

    equipment = [
        {
            "space": A,
            "id": "e-1",
            "name": "E-1",
            "tags": [],
            "asset": _ref(A, "p-1"),
            "etype": _ref(REF, "et-pump"),
            "serial": "SN1",
            "manufacturer": "Acme",
        },
        {
            "space": A,
            "id": "e-2",
            "name": "E-2",
            "tags": [],
            "asset": _ref(A, "p-1"),
            "etype": _ref(REF, "et-valve"),
            "serial": None,
            "manufacturer": "Acme",
        },
        {
            "space": A,
            "id": "e-3",
            "name": "E-3",
            "tags": [],
            "asset": None,
            "etype": None,
            "serial": "SN3",
            "manufacturer": None,
        },
        {
            "space": A,
            "id": "e-4",
            "name": "E-4",
            "tags": ["lifecycle:decommissioned"],
            "asset": _ref(A, "p-2"),
            "etype": None,
            "serial": None,
            "manufacturer": None,
        },
        {
            "space": B,
            "id": "e-5",
            "name": "E-5",
            "tags": [],
            "asset": _ref(B, "p-3"),
            "etype": _ref(REF, "et-pump"),
            "serial": "SN5",
            "manufacturer": "Beta",
        },
    ]
    timeseries = [
        {
            "space": A,
            "id": "ts-1",
            "name": "PI-COMP3-001",
            "description": "d",
            "assets": [_ref(A, "p-1")],
            "unit": "bar",
        },
        {"space": A, "id": "ts-2", "name": "FLOW_02", "description": None, "assets": [_ref(A, "s-1")], "unit": None},
        {"space": A, "id": "ts-3", "name": "XYZ-9", "description": None, "assets": [], "unit": None},
        {"space": A, "id": "ts-4", "name": "CALC-1", "description": None, "assets": [], "unit": None},
        {"space": B, "id": "ts-5", "name": "TEMP_05", "description": "d", "assets": [_ref(B, "p-3")], "unit": "C"},
        {"space": B, "id": "ts-6", "name": None, "description": None, "assets": [], "unit": None},
    ]
    latest = {
        (A, "ts-1"): NOW - timedelta(days=1),
        (A, "ts-2"): NOW - timedelta(days=45),
        (A, "ts-3"): None,
        (B, "ts-5"): NOW - timedelta(days=3),
        (B, "ts-6"): NOW - timedelta(days=100),
    }
    files = [
        {
            "space": A,
            "id": "f-1",
            "name": "datasheet.pdf",
            "description": "d",
            "source_id": "s",
            "category": _ref(REF, "cat-ds"),
            "assets": [_ref(A, "p-1")],
        },
        {
            "space": A,
            "id": "f-2",
            "name": "log.pdf",
            "description": None,
            "source_id": None,
            "category": _ref(REF, "cat-safety"),
            "assets": [],
        },
        {
            "space": A,
            "id": "f-3",
            "name": "Photo.PNG",
            "description": None,
            "source_id": None,
            "category": None,
            "assets": [],
        },
        {
            "space": B,
            "id": "f-4",
            "name": "manual.pdf",
            "description": None,
            "source_id": "s",
            "category": _ref(REF, "cat-ds"),
            "assets": [],
        },
        {
            "space": B,
            "id": "f-5",
            "name": None,
            "description": "d",
            "source_id": None,
            "category": None,
            "assets": [_ref(B, "p-3")],
        },
    ]
    objects3d = [
        {"space": A, "id": "o-1", "name": "O-1", "bbox": True},
        {"space": A, "id": "o-3", "name": "O-3", "bbox": False},
        {"space": B, "id": "o-2", "name": "O-2", "bbox": True},
    ]
    work_orders = [
        {"space": A, "id": "w-1", "name": "W-1", "main_asset": _ref(A, "p-1"), "completed": True},
        {"space": A, "id": "w-2", "name": "W-2", "main_asset": _ref(A, "s-1"), "completed": False},
        {"space": A, "id": "w-3", "name": "W-3", "main_asset": None, "completed": False},
        {"space": B, "id": "w-4", "name": "W-4", "main_asset": _ref(B, "p-3"), "completed": True},
    ]
    notifications = [
        {"space": A, "id": "n-1", "name": "N-1", "asset": _ref(A, "p-1"), "work_order": _ref(A, "w-1")},
        {"space": A, "id": "n-2", "name": "N-2", "asset": None, "work_order": None},
        {"space": B, "id": "n-3", "name": "N-3", "asset": _ref(B, "p-3"), "work_order": None},
    ]
    annotations = [
        {
            "space": A,
            "id": "an-1",
            "file": (A, "f-1"),
            "asset": (A, "p-1"),
            "confidence": 0.95,
            "status": "Approved",
            "created": NOW - timedelta(days=5),
        },
        {
            "space": A,
            "id": "an-2",
            "file": (A, "f-1"),
            "asset": (A, "s-1"),
            "confidence": 0.3,
            "status": "Suggested",
            "created": NOW - timedelta(days=9),
        },
        {
            "space": A,
            "id": "an-3",
            "file": (A, "f-1"),
            "asset": (A, "s-2"),
            "confidence": None,
            "status": "Rejected",
            "created": None,
        },
        {
            "space": B,
            "id": "an-4",
            "file": (B, "f-4"),
            "asset": (B, "p-3"),
            "confidence": 0.7,
            "status": "Suggested",
            "created": NOW - timedelta(days=2),
        },
    ]
    data = {
        "asset_types": {t_pump: "pump", t_sensor: "sensor", t_valve: "valve", t_widget: "widget"},
        "equipment_types": {_ref(REF, "et-pump"): "centrifugal_pump", _ref(REF, "et-valve"): "control_valve"},
        "file_categories": {_ref(REF, "cat-ds"): "datasheet", _ref(REF, "cat-safety"): "safety_log"},
        "class_labels": {
            "assetType": {t_pump: "Pump", t_sensor: "Sensor", t_valve: "Valve", t_widget: "Widget"},
            "assetClass": {c_rot: "Rotating equipment", c_inst: "Instrumentation"},
            "equipmentType": {_ref(REF, "et-pump"): "Centrifugal Pump", _ref(REF, "et-valve"): "Control Valve"},
            "fileCategory": {_ref(REF, "cat-ds"): "Datasheet", _ref(REF, "cat-safety"): "Safety log"},
        },
        "assets": assets,
        "equipment": equipment,
        "timeseries": timeseries,
        "files": files,
        "objects3d": objects3d,
        "work_orders": work_orders,
        "notifications": notifications,
        "annotations": annotations,
        "facility_spaces": dict(FACILITIES),
    }
    return data, latest, NOW
