"""Reads the source views from the data model into plain dicts."""

import re
from datetime import UTC, datetime
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling import DirectRelation, MappedProperty, Node, ViewId
from cognite.client.data_classes.filters import Filter
from cognite.client.exceptions import CogniteAPIError

from cq.batching import CountLeaves, Leaf, SampleId, counter, leaf_filter
from cq.discovery import ENTITY_KEYS, count_entities_by_space, discover_facility_spaces
from cq.extra_links import ExtraLink, ViewNodes
from cq.types import (
    LoadedData,
    Ref,
    RunConfig,
)

CDM, IDM = "cdf_cdm", "cdf_idm"


def _view(space: str, xid: str) -> ViewId:
    return ViewId(space, xid, "v1")


def _nodes(
    client: CogniteClient, space: str, xid: str, spaces: list[str] | None = None, flt: Filter | None = None
) -> list[tuple[Node, dict[str, Any]]]:
    """Nodes of one view. spaces=None reads every space, which is how discovery finds the data. `flt` narrows them."""
    v = _view(space, xid)
    out: list[tuple[Node, dict[str, Any]]] = []
    for n in client.data_modeling.instances.list(instance_type="node", sources=v, space=spaces, filter=flt, limit=-1):
        props = n.properties.get(v)
        if props is not None:
            out.append((n, props))
    return out


_MISSING_VIEW = re.compile(r"views? do(?:es)? not exist", re.IGNORECASE)


def _optional_nodes(
    client: CogniteClient,
    space: str,
    xid: str,
    unavailable: list[str],
    spaces: list[str] | None = None,
    flt: Filter | None = None,
) -> list[tuple[Node, dict[str, Any]]]:
    """Nodes of a view the project may not have. Only "the view does not exist" counts as unavailable.

    Any other failure (permissions, server errors, a malformed request) is raised, so a run never reports an optional
    source as absent when it simply could not be read.
    """
    try:
        return _nodes(client, space, xid, spaces, flt)
    except CogniteAPIError as error:
        if error.code == 400 and _MISSING_VIEW.search(str(error.message)):
            unavailable.append(f"{space}.{xid}")
            return []
        raise


def _ref(value: Any) -> Ref | None:
    """Direct relation dict -> (space, externalId) or None."""
    if isinstance(value, dict) and value.get("externalId"):
        return value.get("space"), value["externalId"]
    return None


def _ts(value: Any) -> datetime | None:
    """DMS timestamps arrive as ISO strings (or datetimes); normalise to aware UTC."""
    if value is None:
        return None
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _refs(value: Any) -> list[Ref]:
    return [r for r in (_ref(v) for v in (value or [])) if r]


def _labels(client: CogniteClient, xid: str) -> dict[Ref, str]:
    """(space, externalId) -> display label (name, else code, else externalId) for a classification view."""
    return {
        (n.space, n.external_id): p.get("name") or p.get("code") or n.external_id for n, p in _nodes(client, CDM, xid)
    }


def _only[T: dict[str, Any]](entities: list[T], facility_spaces: dict[str, str]) -> list[T]:
    return [e for e in entities if e["space"] in facility_spaces]


def _load(
    client: CogniteClient, spaces: list[str] | None, kinds: frozenset[str] | None = None, flt: Filter | None = None
) -> dict[str, Any]:
    """The reference nodes, and the entities of `spaces` (every space when None), as plain dicts.

    `kinds` limits which entity lists are read (all when None) and `flt` narrows them to a slice by external id.
    """

    def want(kind: str) -> bool:
        return kinds is None or kind in kinds

    d: dict[str, Any] = {}
    unavailable: list[str] = []
    # reference nodes are found wherever they live; entities reference them by (space, externalId)
    d["asset_types"] = {(n.space, n.external_id): p.get("code") for n, p in _nodes(client, CDM, "CogniteAssetType")}
    d["equipment_types"] = {
        (n.space, n.external_id): p.get("code") for n, p in _nodes(client, CDM, "CogniteEquipmentType")
    }
    d["file_categories"] = {
        (n.space, n.external_id): p.get("code") for n, p in _nodes(client, CDM, "CogniteFileCategory")
    }
    d["class_labels"] = {
        "assetType": _labels(client, "CogniteAssetType"),
        "assetClass": _labels(client, "CogniteAssetClass"),
        "equipmentType": _labels(client, "CogniteEquipmentType"),
        "fileCategory": _labels(client, "CogniteFileCategory"),
    }
    d["assets"] = [
        {
            "space": n.space,
            "id": n.external_id,
            "name": p.get("name"),
            "tags": p.get("tags") or [],
            "parent": _ref(p.get("parent")),
            "type": _ref(p.get("type")),
            "aclass": _ref(p.get("assetClass")),
            "object3d": _ref(p.get("object3D")),
        }
        for n, p in (_nodes(client, CDM, "CogniteAsset", spaces, flt) if want("assets") else [])
    ]
    d["equipment"] = [
        {
            "space": n.space,
            "id": n.external_id,
            "name": p.get("name"),
            "tags": p.get("tags") or [],
            "asset": _ref(p.get("asset")),
            "etype": _ref(p.get("equipmentType")),
            "serial": p.get("serialNumber"),
            "manufacturer": p.get("manufacturer"),
        }
        for n, p in (_nodes(client, CDM, "CogniteEquipment", spaces, flt) if want("equipment") else [])
    ]
    d["timeseries"] = [
        {
            "space": n.space,
            "id": n.external_id,
            "name": p.get("name"),
            "description": p.get("description"),
            "assets": _refs(p.get("assets")),
            "unit": p.get("sourceUnit"),
        }
        for n, p in (_nodes(client, CDM, "CogniteTimeSeries", spaces, flt) if want("timeseries") else [])
    ]
    d["files"] = [
        {
            "space": n.space,
            "id": n.external_id,
            "name": p.get("name"),
            "description": p.get("description"),
            "source_id": p.get("sourceId"),
            "category": _ref(p.get("category")),
            "assets": _refs(p.get("assets")),
        }
        for n, p in (_nodes(client, CDM, "CogniteFile", spaces, flt) if want("files") else [])
    ]
    d["objects3d"] = [
        {
            "space": n.space,
            "id": n.external_id,
            "name": p.get("name"),
            "bbox": all(p.get(k) is not None for k in ("xMin", "xMax", "yMin", "yMax", "zMin", "zMax")),
        }
        for n, p in (_nodes(client, CDM, "Cognite3DObject", spaces, flt) if want("objects3d") else [])
    ]
    d["work_orders"] = [
        {
            "space": n.space,
            "id": n.external_id,
            "name": p.get("name"),
            "main_asset": _ref(p.get("mainAsset")),
            "completed": p.get("endTime") is not None,
        }
        for n, p in (
            _optional_nodes(client, IDM, "CogniteMaintenanceOrder", unavailable, spaces, flt)
            if want("work_orders")
            else []
        )
    ]
    d["notifications"] = [
        {
            "space": n.space,
            "id": n.external_id,
            "name": p.get("name"),
            "asset": _ref(p.get("asset")),
            "work_order": _ref(p.get("maintenanceOrder")),
        }
        for n, p in (
            _optional_nodes(client, IDM, "CogniteNotification", unavailable, spaces, flt)
            if want("notifications")
            else []
        )
    ]
    av = _view(CDM, "CogniteDiagramAnnotation")
    d["annotations"] = []
    edges = (
        client.data_modeling.instances.list(instance_type="edge", sources=av, space=spaces, filter=flt, limit=-1)
        if want("annotations")
        else []
    )
    for e in edges:
        p = e.properties.get(av)
        if p is None:
            continue
        d["annotations"].append(
            {
                "space": e.space,
                "id": e.external_id,
                "file": (e.start_node.space, e.start_node.external_id),
                "asset": (e.end_node.space, e.end_node.external_id),
                "confidence": p.get("confidence"),
                "status": p.get("status"),
                "created": _ts(p.get("sourceCreatedTime")),
            }
        )
    d["unavailable_sources"] = unavailable
    d["entity_counts_by_space"] = count_entities_by_space(d)
    return d


def load_all(client: CogniteClient, cfg: RunConfig) -> LoadedData:
    """Every entity of the project, with the facilities discovered from where they live. One call reads everything."""
    d = _load(client, None)
    # facilities come from where the entities are; everything outside them is dropped from scoring
    d["facility_spaces"] = discover_facility_spaces(d["entity_counts_by_space"], cfg)
    for key in ENTITY_KEYS:
        d[key] = _only(d[key], d["facility_spaces"])
    return d  # type: ignore[return-value]


def load_batch(
    client: CogniteClient, space: str, facility: str, kinds: frozenset[str], flt: Filter | None = None
) -> LoadedData:
    """The entities of some kinds in one space, optionally only those that match `flt`: what one batch counts.

    The kinds not asked for are empty lists. What the batch's entities link to outside it is answered separately
    (cq.lookups).
    """
    d = _load(client, [space], kinds, flt)
    d["facility_spaces"] = {space: facility}
    return d  # type: ignore[return-value]


def load_shard(client: CogniteClient, facility_spaces: dict[str, str]) -> LoadedData:
    """The entities of the given facility spaces only: what one shard of a run counts. The facilities were found by the
    plan, so nothing is discovered here. An entity that links to one outside these spaces has no such entity loaded."""
    d = _load(client, list(facility_spaces))
    d["facility_spaces"] = dict(facility_spaces)
    return d  # type: ignore[return-value]


# (kind, view space, view, optional, edge): the views whose instances count towards a space being a facility
_COUNTED_VIEWS = (
    ("assets", CDM, "CogniteAsset", False, False),
    ("equipment", CDM, "CogniteEquipment", False, False),
    ("timeseries", CDM, "CogniteTimeSeries", False, False),
    ("files", CDM, "CogniteFile", False, False),
    ("objects3d", CDM, "Cognite3DObject", False, False),
    ("work_orders", IDM, "CogniteMaintenanceOrder", True, False),
    ("notifications", IDM, "CogniteNotification", True, False),
    ("annotations", CDM, "CogniteDiagramAnnotation", False, True),
)
# kind -> (view space, view, edge): where a kind's instances live
KIND_VIEWS = {kind: (space, xid, edge) for kind, space, xid, _optional, edge in _COUNTED_VIEWS}


def _counts_by_space(client: CogniteClient, view: ViewId, *, edge: bool = False) -> dict[str, int]:
    """Instances of a view per instance space, counted by the data modeling service without reading any instance."""
    result = client.data_modeling.instances.aggregate(
        view, Count("externalId"), group_by="space", instance_type="edge" if edge else "node", limit=1000
    )
    return {str(item.group["space"]): int(item.aggregates[0].value) for item in result}


def count_kinds_by_space_remote(client: CogniteClient) -> tuple[dict[str, dict[str, int]], list[str]]:
    """-> ({kind: {space: instances}}, optional views the project does not have).

    The figures `load_all` derives from reading everything (`count_entities_by_space`), kind by kind, at the cost of a few
    aggregate calls, so a run can be planned without loading the project.
    """
    by_kind: dict[str, dict[str, int]] = {}
    unavailable: list[str] = []
    for kind, space, xid, optional, edge in _COUNTED_VIEWS:
        try:
            by_kind[kind] = _counts_by_space(client, _view(space, xid), edge=edge)
        except CogniteAPIError as error:
            if optional and error.code == 400 and _MISSING_VIEW.search(str(error.message)):
                unavailable.append(f"{space}.{xid}")
                by_kind[kind] = {}
                continue
            raise
    return by_kind, unavailable


def leaf_tools(client: CogniteClient, view: ViewId, space: str, *, edge: bool = False) -> tuple[CountLeaves, SampleId]:
    """How `cq.batching.split_leaves` looks at one kind of one space: counting a leaf, and naming one instance of it."""
    instance_type = "edge" if edge else "node"

    def count_one(leaf: Leaf) -> int:
        result = client.data_modeling.instances.aggregate(
            view, Count("externalId"), filter=leaf_filter(leaf, edge), space=[space], instance_type=instance_type
        )
        return int(result.value or 0)

    def sample_id(leaf: Leaf) -> str | None:
        found = client.data_modeling.instances.list(
            instance_type=instance_type, sources=view, space=[space], filter=leaf_filter(leaf, edge), limit=1
        )
        return found[0].external_id if found else None

    return counter(count_one), sample_id


def validate_extra_links(client: CogniteClient, links: list[ExtraLink]) -> dict[tuple[str, str], ViewId]:
    """(model, view) -> the view, for every view the extra links name.

    Each model is looked up once with its views inlined, and every link is checked against it first: the view must be in
    the model and the property must be a direct relation (or a list of them). A wrong name fails the run before any
    instance is read, and the message names what was found.
    """
    view_ids: dict[tuple[str, str], ViewId] = {}
    for model in dict.fromkeys(link["model"] for link in links):
        space, xid, version = model.split(":")
        found = client.data_modeling.data_models.retrieve((space, xid, version), inline_views=True)
        if not found:
            raise ValueError(f"The data model {model} was not found in this project")
        views = {view.external_id: view for view in found[0].views or [] if not isinstance(view, ViewId)}
        for link in (link for link in links if link["model"] == model):
            view = views.get(link["view"])
            if view is None:
                raise ValueError(
                    f"The view {link['view']} is not in {model}. Its views are: {', '.join(sorted(views)) or 'none'}"
                )
            definition = view.properties.get(link["property"])
            name = f"{link['view']}.{link['property']}"
            if definition is None:
                raise ValueError(f"The property {name} does not exist in {model}")
            if not (isinstance(definition, MappedProperty) and isinstance(definition.type, DirectRelation)):
                raise ValueError(f"{name} is not a direct relation, so it is not a link")
            view_ids[(model, link["view"])] = view.as_id()
    return view_ids


def count_extra_link_instances(
    client: CogniteClient, view_ids: dict[tuple[str, str], ViewId]
) -> dict[tuple[str, str], dict[str, int]]:
    """(model, view) -> {space: instances}, without reading the instances."""
    return {key: _counts_by_space(client, view_id) for key, view_id in view_ids.items()}


def load_extra_links(
    client: CogniteClient, links: list[ExtraLink], spaces: list[str] | None = None, flt: Filter | None = None
) -> ViewNodes:
    """The nodes of every view the extra links name, from `spaces` (every space when None), narrowed by `flt`.

    Every space is readable, not only the Core facilities, because the models in the config can hold their instances in
    spaces that have no Core entity (see extra_links.extra_facility_spaces, which decides which become facilities).
    The links are validated first (`validate_extra_links`).
    """
    view_ids = validate_extra_links(client, links)
    return {
        key: [
            (node.space, node.external_id, dict(props))
            for node in client.data_modeling.instances.list(
                instance_type="node", sources=view_id, space=spaces, filter=flt, limit=-1
            )
            if (props := node.properties.get(view_id)) is not None
        ]
        for key, view_id in view_ids.items()
    }
