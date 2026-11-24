"""Finds which instance spaces are facilities, from where the data actually lives."""

from typing import Any

from cq.types import LoadedData, RunConfig

ENTITY_KEYS = ("assets", "equipment", "timeseries", "files", "objects3d", "work_orders", "notifications", "annotations")


def count_entities_by_space(data: LoadedData | dict[str, Any]) -> dict[str, int]:
    """Entity nodes (and annotation edges) per instance space, across every entity view."""
    counts: dict[str, int] = {}
    for key in ENTITY_KEYS:
        for entity in data[key]:
            counts[entity["space"]] = counts.get(entity["space"], 0) + 1
    return counts


def discover_facility_spaces(counts: dict[str, int], cfg: RunConfig | dict[str, Any]) -> dict[str, str]:
    """-> {space: facility id}.

    An explicit cfg["facilitySpaces"] wins. Otherwise every space with enough entity nodes is a facility,
    named by cfg["facilityNames"] where given and by the space id where not.
    """
    explicit = cfg["facilitySpaces"]
    if explicit:
        return dict(explicit)
    excluded = set(cfg["excludeSpaces"]) | {cfg["schemaSpace"], cfg["instanceSpace"]}
    names = cfg["facilityNames"]
    return {
        space: names.get(space, space)
        for space, count in sorted(counts.items())
        if space not in excluded and count >= cfg["minFacilityEntities"]
    }
