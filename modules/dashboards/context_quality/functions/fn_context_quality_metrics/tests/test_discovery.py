"""Facility and reference-space discovery, and classification matching. Offline."""

from typing import Any

from cq.classification import compute_classifications, discover, entity_classes
from cq.config import DEFAULT_CONFIG, build_config
from cq.discovery import count_entities_by_space, discover_facility_spaces

ENTITY_KINDS = (
    "assets",
    "equipment",
    "timeseries",
    "files",
    "objects3d",
    "work_orders",
    "notifications",
    "annotations",
)


def entities(space: str, n: int) -> list[dict[str, Any]]:
    return [{"space": space, "id": f"{space}-{i}"} for i in range(n)]


def data_with(**counts: dict[str, int]) -> dict[str, Any]:
    base: dict[str, Any] = {kind: [] for kind in ENTITY_KINDS}
    for space, per_kind in counts.items():
        for kind, n in per_kind.items():
            base[kind] += entities(space, n)
    return base


def test_defaults_carry_no_facility_names() -> None:
    assert DEFAULT_CONFIG["facilitySpaces"] == {}
    assert DEFAULT_CONFIG["facilityNames"] == {}
    assert "sp_cq_demo" not in str(DEFAULT_CONFIG["excludeSpaces"])


def test_counts_entities_across_every_view() -> None:
    data = data_with(a={"assets": 3, "equipment": 2}, b={"files": 4})

    assert count_entities_by_space(data) == {"a": 5, "b": 4}


def test_every_populated_space_becomes_a_facility_named_by_its_id() -> None:
    found = discover_facility_spaces({"plant_a": 40, "plant_b": 25}, build_config(None))

    assert found == {"plant_a": "plant_a", "plant_b": "plant_b"}


def test_a_tiny_shared_space_is_not_a_facility() -> None:
    found = discover_facility_spaces({"plant_a": 40, "shared_root": 1}, build_config(None))

    assert found == {"plant_a": "plant_a"}


def test_system_and_output_spaces_are_ignored_even_when_large() -> None:
    cfg = build_config(None)

    found = discover_facility_spaces({"cdf_cdm": 500, cfg["instanceSpace"]: 500, "plant_a": 40}, cfg)

    assert found == {"plant_a": "plant_a"}


def test_names_rename_a_discovered_space() -> None:
    cfg = build_config({"facilityNames": {"plant_a": "north"}})

    assert discover_facility_spaces({"plant_a": 40, "plant_b": 40}, cfg) == {"plant_a": "north", "plant_b": "plant_b"}


def test_an_explicit_allowlist_overrides_discovery() -> None:
    cfg = build_config({"facilitySpaces": {"plant_a": "north"}})

    assert discover_facility_spaces({"plant_a": 40, "plant_b": 40}, cfg) == {"plant_a": "north"}


def test_minimum_is_configurable() -> None:
    cfg = build_config({"minFacilityEntities": 1})

    assert discover_facility_spaces({"shared_root": 1}, cfg) == {"shared_root": "shared_root"}


def asset(xid: str, type_ref: tuple[str, str], *, orphan: bool) -> dict[str, Any]:
    return {
        "space": "plant",
        "id": xid,
        "type": type_ref,
        "aclass": None,
        "root": False,
        "orphan": orphan,
        "div": None,
        "disc": None,
        "tier": None,
    }


def test_classification_nodes_are_matched_by_space_and_external_id() -> None:
    # the same externalId in two spaces must not be confused
    labels = {("ref_a", "pump"): "Pump (A)", ("ref_b", "pump"): "Pump (B)"}
    data = {
        "class_labels": {"assetType": labels, "assetClass": {}, "equipmentType": {}, "fileCategory": {}},
        "facility_spaces": {"plant": "plant"},
        "assets": [asset("a1", ("ref_a", "pump"), orphan=False), asset("a2", ("ref_b", "pump"), orphan=False)],
        "equipment": [],
        "files": [],
    }

    assert discover(data) == ["assetType"]
    assert entity_classes(data) == {
        ("plant", "a1"): {"assetType": "Pump (A)"},
        ("plant", "a2"): {"assetType": "Pump (B)"},
    }
    rows = compute_classifications(data, build_config(None), {}, None, lambda _dimension: "owner")
    assert sorted(key[2] for key in rows) == ["Pump (A)", "Pump (B)"]


def test_an_entity_pointing_at_a_missing_node_is_unclassified() -> None:
    data = {
        "class_labels": {
            "assetType": {("ref", "pump"): "Pump"},
            "assetClass": {},
            "equipmentType": {},
            "fileCategory": {},
        },
        "facility_spaces": {"plant": "plant"},
        "assets": [asset("a1", ("ref", "gone"), orphan=True), asset("a2", ("ref", "pump"), orphan=False)],
        "equipment": [],
        "files": [],
    }

    rows = compute_classifications(data, build_config(None), {}, None, lambda _dimension: "owner")

    assert sorted(key[2] for key in rows) == ["Pump", "Unclassified"]
