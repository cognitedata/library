"""Auto-discovered classifications: group entities by the type/category node they reference.

A classification kind is only emitted when the project actually uses it, i.e. at least one entity
references a node of that kind that exists. Nothing here names a specific type; labels come from the data.
"""

from collections import defaultdict
from collections.abc import Callable
from typing import Any

from cq.types import EntityKey, Grain, LoadedData, Ref, RunConfig

UNCLASSIFIED_ID, UNCLASSIFIED_NAME = "__none__", "Unclassified"

# (kind, entity list in the loaded data, reference field on the entity, dimension)
SPECS = (
    ("assetType", "assets", "type", "asset-hierarchy"),
    ("assetClass", "assets", "aclass", "asset-hierarchy"),
    ("equipmentType", "equipment", "etype", "equipment-asset"),
    ("fileCategory", "files", "category", "files"),
)


def discover(data: LoadedData) -> list[str]:
    """Kinds that are populated: some entity references a classification node that exists."""
    found: list[str] = []
    for kind, entities, field, _ in SPECS:
        labels = data["class_labels"].get(kind, {})
        if any(e.get(field) in labels for e in data[entities]):
            found.append(kind)
    return found


def entity_classes(data: LoadedData) -> dict[EntityKey, dict[str, str]]:
    """{(space, externalId): {kind: label}} for entities that reference a classification node.

    Used to tag drilldown rows. Entities with no classification are simply absent, so a row never
    claims a class the entity does not have.
    """
    out: dict[EntityKey, dict[str, str]] = {}
    for kind in discover(data):
        _, entities, field, _ = next(s for s in SPECS if s[0] == kind)
        labels = data["class_labels"][kind]
        for e in data[entities]:
            ref = e.get(field)
            if ref in labels:
                out.setdefault((e["space"], e["id"]), {})[kind] = labels[ref]
    return out


def _participation(
    kind: str, entity: dict[str, Any], A: dict[EntityKey, Any], cfg: RunConfig
) -> tuple[bool, bool, Ref | None]:
    """(eligible, passing, asset_key_for_grain) for one entity, matching the dimension's own rules."""
    if kind in ("assetType", "assetClass"):
        return (not entity["root"], not entity["orphan"], None)
    if kind == "equipmentType":
        decommissioned = any(tag in entity["tags"] for tag in cfg["exclusions"]["decommissionedTags"])
        return (not decommissioned, entity["asset"] is not None, entity["asset"])
    link = entity["assets"][0] if entity["assets"] else None
    return (not entity.get("excluded", False), link is not None, link)


def compute_classifications(
    data: LoadedData,
    cfg: RunConfig,
    A: dict[EntityKey, Any],
    ctx: Callable[[str, Ref | None], Grain],
    owner_for: Callable[[str], str | None],
) -> dict[tuple, list[int]]:
    """-> {(kind, classId, className, dim, unit, facility, div, disc, owner, tier): [eligible, passing]}"""
    out: dict[tuple, list[int]] = defaultdict(lambda: [0, 0])
    for kind in discover(data):
        _, entities, field, dim = next(s for s in SPECS if s[0] == kind)
        labels = data["class_labels"][kind]
        for e in data[entities]:
            eligible, passing, asset_key = _participation(kind, e, A, cfg)
            if not eligible:
                continue
            if entities == "assets":
                g = {
                    "unit": e["space"],
                    "facility": data["facility_spaces"].get(e["space"]),
                    "div": e["div"],
                    "disc": e["disc"],
                    "tier": e["tier"],
                }
            else:
                g = ctx(e["space"], asset_key)
            ref = e.get(field)
            cid, name = (ref[1], labels[ref]) if ref in labels else (UNCLASSIFIED_ID, UNCLASSIFIED_NAME)
            row = out[(kind, cid, name, dim, g["unit"], g["facility"], g["div"], g["disc"], owner_for(dim), g["tier"])]
            row[0] += 1
            row[1] += 1 if passing else 0
    return out
