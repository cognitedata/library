"""What the metrics need to know about entities that are not in the batch being counted.

A shard that holds whole spaces has every entity of its spaces in memory, so a link from one entity to another is
resolved from the loaded lists. A *batch* is a slice of one space (see cq.batching): the entities it links to are not
loaded. `Cross` carries the answers instead, fetched with targeted queries (`cq.lookups`), so a batch counts exactly
what a whole space would, for the entities it holds.
"""

from dataclasses import dataclass, field
from typing import Any

from cq.classification import discover
from cq.types import AssetRow, LoadedData, Ref


@dataclass
class Cross:
    """Answers about the entities of one batch that come from entities outside it.

    assets: the assets the batch links to or descends from, not enriched: the parents up the chain (as far as the depth
        limit looks), and the assets its entities link to when tier or division decides the grain.
    has_children: the batch's own parentless assets that have at least one child anywhere in the facilities.
    ts_assets: the batch's own assets that some time series links to.
    linked_objects: the batch's own 3D objects, each with an asset that links to it.
    wo_with_notification: the batch's own work orders that some notification links to.
    populated: the classification kinds some entity of the whole space references (None: judge from the batch).
    """

    assets: dict[Ref, AssetRow] = field(default_factory=dict)
    has_children: set[Ref] = field(default_factory=set)
    ts_assets: set[Ref] = field(default_factory=set)
    linked_objects: dict[Ref, Ref] = field(default_factory=dict)
    wo_with_notification: set[Ref] = field(default_factory=set)
    populated: set[str] | None = None


def full_cross(data: LoadedData | dict[str, Any]) -> Cross:
    """Every answer, taken from fully loaded data: what the queries return, as a superset, without a client.

    Used by the tests to count a space in pieces and compare with counting it whole.
    """
    return Cross(
        assets={(a["space"], a["id"]): dict(a) for a in data["assets"]},  # type: ignore[misc]
        has_children={a["parent"] for a in data["assets"] if a["parent"]},
        ts_assets={ref for t in data["timeseries"] for ref in t["assets"]},
        linked_objects={a["object3d"]: (a["space"], a["id"]) for a in data["assets"] if a["object3d"]},
        wo_with_notification={n["work_order"] for n in data["notifications"] if n["work_order"]},
        populated=set(discover(data)),  # type: ignore[arg-type]
    )
