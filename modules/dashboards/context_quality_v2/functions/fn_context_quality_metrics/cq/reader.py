"""Reads the source views from the data model into plain dicts."""

import re
from datetime import UTC, datetime
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import DirectRelation, MappedProperty, Node, ViewId
from cognite.client.exceptions import CogniteAPIError

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
    client: CogniteClient, space: str, xid: str, spaces: list[str] | None = None
) -> list[tuple[Node, dict[str, Any]]]:
    """Nodes of one view. spaces=None reads every space, which is how discovery finds the data."""
    v = _view(space, xid)
    out: list[tuple[Node, dict[str, Any]]] = []
    for n in client.data_modeling.instances.list(instance_type="node", sources=v, space=spaces, limit=-1):
        props = n.properties.get(v)
        if props is not None:
            out.append((n, props))
    return out


_MISSING_VIEW = re.compile(r"views? do(?:es)? not exist", re.IGNORECASE)


def _optional_nodes(
    client: CogniteClient, space: str, xid: str, unavailable: list[str]
) -> list[tuple[Node, dict[str, Any]]]:
    """Nodes of a view the project may not have. Only "the view does not exist" counts as unavailable.

    Any other failure (permissions, server errors, a malformed request) is raised, so a run never reports an optional
    source as absent when it simply could not be read.
    """
    try:
        return _nodes(client, space, xid)
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


def load_all(client: CogniteClient, cfg: RunConfig) -> LoadedData:
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
        for n, p in _nodes(client, CDM, "CogniteAsset")
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
        for n, p in _nodes(client, CDM, "CogniteEquipment")
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
        for n, p in _nodes(client, CDM, "CogniteTimeSeries")
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
        for n, p in _nodes(client, CDM, "CogniteFile")
    ]
    d["objects3d"] = [
        {
            "space": n.space,
            "id": n.external_id,
            "name": p.get("name"),
            "bbox": all(p.get(k) is not None for k in ("xMin", "xMax", "yMin", "yMax", "zMin", "zMax")),
        }
        for n, p in _nodes(client, CDM, "Cognite3DObject")
    ]
    d["work_orders"] = [
        {
            "space": n.space,
            "id": n.external_id,
            "name": p.get("name"),
            "main_asset": _ref(p.get("mainAsset")),
            "completed": p.get("endTime") is not None,
        }
        for n, p in _optional_nodes(client, IDM, "CogniteMaintenanceOrder", unavailable)
    ]
    d["notifications"] = [
        {
            "space": n.space,
            "id": n.external_id,
            "name": p.get("name"),
            "asset": _ref(p.get("asset")),
            "work_order": _ref(p.get("maintenanceOrder")),
        }
        for n, p in _optional_nodes(client, IDM, "CogniteNotification", unavailable)
    ]
    av = _view(CDM, "CogniteDiagramAnnotation")
    d["annotations"] = []
    for e in client.data_modeling.instances.list(instance_type="edge", sources=av, limit=-1):
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
    # facilities come from where the entities are; everything outside them is dropped from scoring
    d["unavailable_sources"] = unavailable
    d["entity_counts_by_space"] = count_entities_by_space(d)
    d["facility_spaces"] = discover_facility_spaces(d["entity_counts_by_space"], cfg)
    for key in ENTITY_KEYS:
        d[key] = _only(d[key], d["facility_spaces"])
    return d  # type: ignore[return-value]


def load_extra_links(client: CogniteClient, links: list[ExtraLink]) -> ViewNodes:
    """The nodes of every view the extra links name, read from every space.

    Every space is read, not only the Core facilities, because the models in the config can hold their instances in
    spaces that have no Core entity (see extra_links.extra_facility_spaces, which decides which become facilities).

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
    return {
        key: [
            (node.space, dict(props))
            for node in client.data_modeling.instances.list(instance_type="node", sources=view_id, space=None, limit=-1)
            if (props := node.properties.get(view_id)) is not None
        ]
        for key, view_id in view_ids.items()
    }
