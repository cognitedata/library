"""Location filters as a scope: which locations a run records, and what is said about the ones it cannot use."""

from typing import Any

import pytest
from cognite.client.exceptions import CogniteAPIError
from cq import locations, runner, writer
from cq.config import build_config
from sample_rules import SAMPLE_RESOURCES
from test_runner import RecordingClient, canned_run, props, records

FACILITIES = {"sp_north": "north", "sp_south": "south", "sp_central": "central"}


def location(external_id: str, **fields: Any) -> dict[str, Any]:
    return {"id": hash(external_id) % 10_000, "externalId": external_id, "name": external_id.title(), **fields}


def test_a_location_that_lists_instance_spaces_covers_the_facilities_of_those_spaces() -> None:
    north_south = location("region", instanceSpaces=["sp_south", "sp_north"])

    found, left_out = locations.resolve([north_south], FACILITIES)

    assert left_out == {}
    assert found == [
        {
            "id": "region",
            "name": "Region",
            "parent": None,
            "spaces": ["sp_north", "sp_south"],
            "facilityIds": ["north", "south"],
        }
    ]


def test_a_child_names_its_parent_by_external_id() -> None:
    parent = location("region", instanceSpaces=["sp_north", "sp_south"])
    child = location("site", instanceSpaces=["sp_north"], parentId=parent["id"])

    found, _ = locations.resolve([parent, child], FACILITIES)

    assert {entry["id"]: entry["parent"] for entry in found} == {"region": None, "site": "region"}


def test_only_the_instance_spaces_that_are_facilities_count() -> None:
    mixed = location("mixed", instanceSpaces=["sp_north", "sp_elsewhere"])

    found, _ = locations.resolve([mixed], FACILITIES)

    assert found[0]["spaces"] == ["sp_north"]


@pytest.mark.parametrize(
    ("fields", "reason"),
    [
        ({}, "no-instance-spaces"),
        ({"assetCentric": {"dataSetIds": [1]}}, "no-instance-spaces"),
        ({"instanceSpaces": ["sp_north"], "assetCentric": {"dataSetIds": [1]}}, "asset-centric-filters"),
        ({"instanceSpaces": ["sp_elsewhere"]}, "no-facility-spaces"),
    ],
)
def test_a_location_that_cannot_be_told_from_the_rows_is_left_out_and_counted(
    fields: dict[str, Any], reason: str
) -> None:
    found, left_out = locations.resolve([location("other", **fields)], FACILITIES)

    assert found == []
    assert left_out == {reason: 1}


def test_the_warning_counts_the_locations_left_out_and_names_none() -> None:
    warnings = locations.location_warnings({"no-instance-spaces": 2})

    assert [(w["code"], w["reason"], w["locations"]) for w in warnings] == [
        ("location-unsupported", "no-instance-spaces", 2)
    ]
    assert all("other" not in w["message"] for w in warnings)


def test_unreadable_locations_are_a_warning_not_a_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def denied(_client: Any) -> list[dict[str, Any]]:
        raise CogniteAPIError("Forbidden", 403)

    monkeypatch.setattr(locations, "list_locations", denied)

    found, warnings = locations.read(object(), FACILITIES)  # type: ignore[arg-type]

    assert found == []
    assert [(w["code"], w["subject"]) for w in warnings] == [("locations-unavailable", "locationfilters")]


def test_location_rows_carry_the_name_the_parent_and_the_covered_facilities() -> None:
    found, _ = locations.resolve([location("site", instanceSpaces=["sp_north"])], FACILITIES)

    ((key, row),) = writer.location_rows(found)

    assert key.startswith("loc:")
    assert row["rowKind"] == "location"
    assert (row["entityId"], row["entityLabel"]) == ("site", "Site")
    assert "detail" not in row
    assert row["cellsJson"] == '{"facilityIds": ["north"], "spaces": ["sp_north"]}'


def test_a_run_writes_the_supported_locations_and_warns_about_the_rest(monkeypatch: pytest.MonkeyPatch) -> None:
    canned_run(monkeypatch)
    facilities = ["sp_a", "sp_b"]
    monkeypatch.setattr(
        locations,
        "list_locations",
        lambda _client: [
            location("site", instanceSpaces=["sp_a"]),
            location("by_data_set", assetCentric={"dataSetIds": [1]}),
        ],
    )
    monkeypatch.setattr(runner, "compute_run", _with_facilities(runner.compute_run, facilities))
    client = RecordingClient()

    summary = runner.compute_metrics(client, build_config(SAMPLE_RESOURCES), {})  # type: ignore[arg-type]

    rows = [props(item) for item in records(client) if props(item)["rowKind"] == "location"]
    assert [row["entityId"] for row in rows] == ["site"]
    assert summary["locationRows"] == 1
    assert [w["code"] for w in summary["warnings"] if w["code"].startswith("location")] == ["location-unsupported"]


def _with_facilities(inner: Any, spaces: list[str]) -> Any:
    def run(client: Any, cfg: Any) -> Any:
        data, result = inner(client, cfg)
        data["facility_spaces"] = {space: space.removeprefix("sp_") for space in spaces}
        return data, result

    return run
