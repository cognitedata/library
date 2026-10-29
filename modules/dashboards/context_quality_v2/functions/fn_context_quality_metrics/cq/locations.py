"""Location filters as a scope: which facilities a location covers.

A location filter is a named filter on the project, set up under Locations in Fusion. A location that lists instance
spaces is a named group of facility spaces, so the run only needs to record which facilities each one covers; the
dashboard sums the rows of those facilities. A location that is defined in any other way (by data set, asset sub-tree or
external id prefix) cannot be told from the metric rows, which are counted per space, so it is left out and reported.
"""

from typing import Any

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError

NO_INSTANCE_SPACES = "no-instance-spaces"
ASSET_CENTRIC_FILTERS = "asset-centric-filters"
NO_FACILITY_SPACES = "no-facility-spaces"


def list_locations(client: CogniteClient) -> list[dict[str, Any]]:
    """The project's location filters, as the Locations API returns them (parents are named by internal id)."""
    path = f"/apps/v1/projects/{client.config.project}/storage/config/locationfilters/list"
    items: list[dict[str, Any]] = client.post(path, json={"flat": True}).json().get("items", [])
    return items


def _asset_centric(location: dict[str, Any]) -> bool:
    filters = location.get("assetCentric") or {}
    return any(filters.values())


def resolve(locations: list[dict[str, Any]], facilities: dict[str, str]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """(the supported locations, how many were left out per reason).

    A supported location is {"id", "name", "parent", "spaces", "facilityIds"}: its instance spaces that are facilities of
    the run, and the facility ids of those. `parent` is the external id of the parent location, or None.
    """
    external_id_of = {item["id"]: item["externalId"] for item in locations if "id" in item}
    supported: list[dict[str, Any]] = []
    left_out: dict[str, int] = {}
    for item in sorted(locations, key=lambda entry: entry["externalId"]):
        spaces = sorted(space for space in item.get("instanceSpaces") or [] if space in facilities)
        if not item.get("instanceSpaces"):
            reason = NO_INSTANCE_SPACES
        elif _asset_centric(item):
            reason = ASSET_CENTRIC_FILTERS
        elif not spaces:
            reason = NO_FACILITY_SPACES
        else:
            supported.append(
                {
                    "id": item["externalId"],
                    "name": item.get("name") or item["externalId"],
                    "parent": external_id_of.get(item.get("parentId")),
                    "spaces": spaces,
                    "facilityIds": sorted({facilities[space] for space in spaces}),
                }
            )
            continue
        left_out[reason] = left_out.get(reason, 0) + 1
    return supported, left_out


def location_warnings(left_out: dict[str, int]) -> list[dict[str, Any]]:
    """One warning per reason locations were left out. It counts them and names none, as warnings never name things."""
    return [
        {
            "code": "location-unsupported",
            "message": (
                f"{count} location filter(s) cannot be used as a scope ({reason}): only locations that list instance "
                "spaces, and nothing else, can be."
            ),
            "reason": reason,
            "locations": count,
        }
        for reason, count in sorted(left_out.items())
    ]


def read(client: CogniteClient, facilities: dict[str, str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(the supported locations, the warnings). Locations never fail a run: if they cannot be read, there are none."""
    try:
        found = list_locations(client)
    except CogniteAPIError as error:
        message = f"The location filters could not be read, so locations are not offered as a scope: {error.message}"
        return [], [{"code": "locations-unavailable", "message": message, "subject": "locationfilters"}]
    supported, left_out = resolve(found, facilities)
    return supported, location_warnings(left_out)
