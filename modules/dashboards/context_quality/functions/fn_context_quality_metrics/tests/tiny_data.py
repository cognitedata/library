"""Hand-built loaded data for tests of tiers and hierarchy. Imports nothing from the function."""

from datetime import UTC, datetime
from typing import Any

NOW = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)
REF = "sp_ref"


def asset(
    space: str,
    xid: str,
    *,
    parent: tuple[str, str] | None = None,
    type_code: str | None = None,
    tags: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "space": space,
        "id": xid,
        "name": xid.upper(),
        "tags": list(tags),
        "parent": parent,
        "type": (REF, f"type-{type_code}") if type_code else None,
        "aclass": None,
        "object3d": None,
    }


def equipment(
    space: str, xid: str, asset_ref: tuple[str, str] | None = None, tags: tuple[str, ...] = ()
) -> dict[str, Any]:
    return {
        "space": space,
        "id": xid,
        "name": xid.upper(),
        "tags": list(tags),
        "asset": asset_ref,
        "etype": None,
        "serial": "s",
        "manufacturer": "m",
    }


def timeseries(space: str, xid: str, asset_ref: tuple[str, str] | None = None) -> dict[str, Any]:
    return {
        "space": space,
        "id": xid,
        "name": xid.upper(),
        "description": "d",
        "assets": [asset_ref] if asset_ref else [],
        "unit": "u",
    }


def file(space: str, xid: str, asset_ref: tuple[str, str] | None = None) -> dict[str, Any]:
    return {
        "space": space,
        "id": xid,
        "name": f"{xid}.pdf",
        "description": "d",
        "source_id": "s",
        "category": None,
        "assets": [asset_ref] if asset_ref else [],
    }


def work_order(space: str, xid: str, asset_ref: tuple[str, str] | None = None) -> dict[str, Any]:
    return {"space": space, "id": xid, "name": xid.upper(), "main_asset": asset_ref, "completed": False}


def notification(space: str, xid: str, asset_ref: tuple[str, str] | None = None) -> dict[str, Any]:
    return {"space": space, "id": xid, "name": xid.upper(), "asset": asset_ref, "work_order": None}


def object3d(space: str, xid: str) -> dict[str, Any]:
    return {"space": space, "id": xid, "name": xid.upper(), "bbox": True}


def annotation(space: str, xid: str, asset_ref: tuple[str, str]) -> dict[str, Any]:
    return {
        "space": space,
        "id": xid,
        "file": (space, "f"),
        "asset": asset_ref,
        "confidence": 0.9,
        "status": "Approved",
        "created": NOW,
    }


def data(
    *,
    spaces: tuple[str, ...] = ("sp_a",),
    assets: list[dict[str, Any]] | None = None,
    equipment_: list[dict[str, Any]] | None = None,
    timeseries_: list[dict[str, Any]] | None = None,
    files: list[dict[str, Any]] | None = None,
    objects3d: list[dict[str, Any]] | None = None,
    work_orders: list[dict[str, Any]] | None = None,
    notifications: list[dict[str, Any]] | None = None,
    annotations: list[dict[str, Any]] | None = None,
    type_codes: tuple[str, ...] = ("pump", "valve", "widget"),
    typeless_type_nodes: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Asset-type nodes exist for `type_codes`; `typeless_type_nodes` are type nodes that carry no code."""
    asset_types: dict[tuple[str, str], str | None] = {(REF, f"type-{code}"): code for code in type_codes}
    asset_types.update({(REF, f"type-{name}"): None for name in typeless_type_nodes})
    return {
        "asset_types": asset_types,
        "equipment_types": {},
        "file_categories": {},
        "class_labels": {"assetType": {}, "assetClass": {}, "equipmentType": {}, "fileCategory": {}},
        "assets": assets or [],
        "equipment": equipment_ or [],
        "timeseries": timeseries_ or [],
        "files": files or [],
        "objects3d": objects3d or [],
        "work_orders": work_orders or [],
        "notifications": notifications or [],
        "annotations": annotations or [],
        "facility_spaces": {space: space for space in spaces},
    }
