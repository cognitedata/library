"""Answering, with targeted queries, what a batch's entities link to outside the batch (see cq.cross).

Every question is asked about the ids of the batch's own entities, in chunks, so the answers stay as small as the batch
however big the space is: which of these assets have a child, which are linked by a time series, which 3D objects an
asset links to, which work orders a notification links to, and the assets up a parent chain. Only the same instance
space is looked at, as for a shard that holds whole spaces.
"""

from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling import NodeId, ViewId
from cognite.client.data_classes.data_modeling import filters as f
from cognite.client.exceptions import CogniteAPIError

from cq.cross import Cross
from cq.reader import _MISSING_VIEW, CDM, IDM, _ref
from cq.types import AssetRow, LoadedData, Ref

CHUNK = 500
TIME_SERIES_CHUNK = 100
GROUP_LIMIT = 1000
WORKERS = 8

ASSET = ViewId(CDM, "CogniteAsset", "v1")
TIME_SERIES = ViewId(CDM, "CogniteTimeSeries", "v1")
NOTIFICATION = ViewId(IDM, "CogniteNotification", "v1")

# the classification kinds of cq.classification.SPECS, as (kind, view, property of the entity that references the node)
CLASSIFIED_BY = (
    ("assetType", "assets", ASSET, "type"),
    ("assetClass", "assets", ASSET, "assetClass"),
    ("equipmentType", "equipment", ViewId(CDM, "CogniteEquipment", "v1"), "equipmentType"),
    ("fileCategory", "files", ViewId(CDM, "CogniteFile", "v1"), "category"),
)


def _chunks(items: Iterable[Any], size: int = CHUNK) -> list[list[Any]]:
    items = list(items)
    return [items[i : i + size] for i in range(0, len(items), size)]


def _dicts(refs: Iterable[Ref]) -> list[dict[str, str | None]]:
    return [{"space": space, "externalId": xid} for space, xid in refs]


def _in_parallel[T, R](work: Callable[[T], R], items: list[T]) -> list[R]:
    if len(items) <= 1:
        return [work(item) for item in items]
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        return list(pool.map(work, items))


def _present(client: CogniteClient, view: ViewId, prop: str, refs: Iterable[Ref], space: str) -> set[Ref]:
    """Which of `refs` are the value of `prop` in some instance of `view` in `space`: one aggregate per chunk."""

    def ask(chunk: list[Ref]) -> list[Ref]:
        groups = client.data_modeling.instances.aggregate(
            view,
            Count("externalId"),
            group_by=prop,
            filter=f.In(view.as_property_ref(prop), _dicts(chunk)),
            space=[space],
            limit=1000,
        )
        return [ref for group in groups if (ref := _ref(group.group.get(prop))) is not None]

    return {ref for found in _in_parallel(ask, _chunks(refs)) for ref in found}


def _asset_row(node: Any, props: dict[str, Any]) -> AssetRow:
    return {  # type: ignore[typeddict-item]
        "space": node.space,
        "id": node.external_id,
        "name": props.get("name"),
        "tags": props.get("tags") or [],
        "parent": _ref(props.get("parent")),
        "type": _ref(props.get("type")),
        "aclass": _ref(props.get("assetClass")),
        "object3d": _ref(props.get("object3D")),
    }


def _assets(client: CogniteClient, refs: Iterable[Ref], space: str) -> dict[Ref, AssetRow]:
    """The assets of `space` named by `refs`, by key. An asset that does not exist, or lies in another space, is absent."""

    def get(chunk: list[Ref]) -> list[AssetRow]:
        nodes = client.data_modeling.instances.retrieve_nodes(
            [NodeId(space_, xid) for space_, xid in chunk], sources=ASSET
        )
        return [_asset_row(node, props) for node in nodes if (props := node.properties.get(ASSET)) is not None]

    wanted = [ref for ref in refs if ref[0] == space]
    return {(row["space"], row["id"]): row for rows in _in_parallel(get, _chunks(wanted)) for row in rows}


def _ancestors(client: CogniteClient, own: list[AssetRow], space: str, levels: int) -> dict[Ref, AssetRow]:
    """The parents of `own`, their parents and so on, `levels` deep. Each level is asked once for all its parents."""
    known: dict[Ref, AssetRow] = {}
    have = {(a["space"], a["id"]) for a in own}
    frontier = {a["parent"] for a in own if a["parent"]} - have
    for _ in range(levels):
        if not frontier:
            break
        found = _assets(client, frontier, space)
        known.update(found)
        have |= set(found)
        frontier = {row["parent"] for row in found.values() if row["parent"]} - have
    return known


def _populated(client: CogniteClient, data: LoadedData, kinds: frozenset[str], space: str) -> set[str]:
    """The classification kinds some entity of the space references an existing node of: judged for the whole space."""
    out: set[str] = set()
    for kind, entities, view, prop in CLASSIFIED_BY:
        if entities not in kinds:
            continue
        labels = data["class_labels"].get(kind, {})
        groups = client.data_modeling.instances.aggregate(
            view, Count("externalId"), group_by=prop, space=[space], limit=1000
        )
        if any(_ref(group.group.get(prop)) in labels for group in groups):
            out.add(kind)
    return out


def _linked_objects(
    client: CogniteClient, objects: list[Ref], space: str, need_assets: bool
) -> tuple[dict[Ref, Ref], dict[Ref, AssetRow]]:
    """(object -> an asset that links to it, those assets). The assets are only fetched when the grain needs them."""
    if not need_assets:
        present = _present(client, ASSET, "object3D", objects, space)
        return {
            ref: ref for ref in present
        }, {}  # only whether it is linked is used, so the object stands for the asset

    def ask(chunk: list[Ref]) -> list[AssetRow]:
        nodes = client.data_modeling.instances.list(
            instance_type="node",
            sources=ASSET,
            space=[space],
            filter=f.In(ASSET.as_property_ref("object3D"), _dicts(chunk)),
            limit=-1,
        )
        return [_asset_row(node, props) for node in nodes if (props := node.properties.get(ASSET)) is not None]

    rows = [row for found in _in_parallel(ask, _chunks(objects)) for row in found]
    linked = {row["object3d"]: (row["space"], row["id"]) for row in rows if row["object3d"]}
    return linked, {(row["space"], row["id"]): row for row in rows}


def _linked_to_notification(client: CogniteClient, orders: list[Ref], space: str) -> set[Ref]:
    try:
        return _present(client, NOTIFICATION, "maintenanceOrder", orders, space)
    except CogniteAPIError as error:
        if error.code == 400 and _MISSING_VIEW.search(str(error.message)):
            return set()  # without the view there are no notifications
        raise


def _with_time_series(client: CogniteClient, assets: list[Ref], space: str) -> set[Ref]:
    """Which of `assets` a time series links to.

    The groups of an answer are the assets of the matching time series, which include assets outside the chunk; the
    service returns at most 1000 groups. A chunk whose answer is full is asked again in halves, so none is missed.
    """

    def ask(chunk: list[Ref]) -> set[Ref]:
        groups = client.data_modeling.instances.aggregate(
            TIME_SERIES,
            Count("externalId"),
            group_by="assets",
            filter=f.ContainsAny(TIME_SERIES.as_property_ref("assets"), _dicts(chunk)),
            space=[space],
            limit=GROUP_LIMIT,
        )
        if len(groups) >= GROUP_LIMIT and len(chunk) > 1:
            middle = len(chunk) // 2
            return ask(chunk[:middle]) | ask(chunk[middle:])
        wanted = set(chunk)
        return {ref for group in groups if (ref := _ref(group.group.get("assets"))) in wanted}

    return {ref for found in _in_parallel(ask, _chunks(assets, TIME_SERIES_CHUNK)) for ref in found}


def fetch_cross(client: CogniteClient, data: LoadedData, cfg: Any, kinds: frozenset[str], space: str) -> Cross:
    """What the entities of `data` (the batch) need to know that `data` does not hold. See cq.cross.Cross."""
    cross = Cross(populated=_populated(client, data, kinds, space))
    # the grain of an entity comes from the asset it links to, but only when tier or division is configured
    grain_needs_assets = bool(cfg["tieringEnabled"] or cfg["divisionTagPrefix"])
    wanted: set[Ref] = set()
    if "assets" in kinds:
        own = data["assets"]
        parentless = [(a["space"], a["id"]) for a in own if a["parent"] is None]
        cross.has_children = _present(client, ASSET, "parent", parentless, space)
        cross.assets.update(_ancestors(client, own, space, int(cfg["maxDepth"]) + 2))
        cross.ts_assets = _with_time_series(client, [(a["space"], a["id"]) for a in own], space)
    if "objects3d" in kinds:
        objects = [(o["space"], o["id"]) for o in data["objects3d"]]
        cross.linked_objects, found = _linked_objects(client, objects, space, grain_needs_assets)
        cross.assets.update(found)
    if "work_orders" in kinds:
        cross.wo_with_notification = _linked_to_notification(
            client, [(w["space"], w["id"]) for w in data["work_orders"]], space
        )
    if grain_needs_assets:
        wanted |= {e["asset"] for e in data["equipment"] if e["asset"]}
        wanted |= {ref for t in data["timeseries"] for ref in t["assets"][:1]}
        wanted |= {ref for fl in data["files"] for ref in fl["assets"][:1]}
        wanted |= {w["main_asset"] for w in data["work_orders"] if w["main_asset"]}
        wanted |= {n["asset"] for n in data["notifications"] if n["asset"]}
        wanted |= {an["asset"] for an in data["annotations"]}
        wanted -= {(a["space"], a["id"]) for a in data["assets"]} | set(cross.assets)
        cross.assets.update(_assets(client, wanted, space))
    return cross
