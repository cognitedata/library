"""CDF browse helpers for the Discovery object tree and operator API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Tuple

RAW_VALUE_PREVIEW_LEN = 512

# Cognite Core Data Model (CDM) — built into CDF; spaces/models list APIs may omit it.
NATIVE_CDF_CDM_SPACE = "cdf_cdm"
NATIVE_CDF_CDM_DATA_MODEL_EXTERNAL_ID = "CogniteCore"
NATIVE_CDF_CDM_DATA_MODEL_VERSION = "v1"

CLASSIC_RESOURCE_BRANCHES: Tuple[Tuple[str, str], ...] = (
    ("assets", "Assets"),
    ("timeseries", "Time Series"),
    ("files", "Files"),
    ("events", "Events"),
    ("sequences", "Sequences"),
    ("data_sets", "Data Sets"),
    ("relationships", "Relationships"),
    ("labels", "Labels"),
)

SYSTEM_DM_SPACE_PREFIX = "cdf_"


def _iso(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.isoformat()
    return str(ts)


def _truncate(value: Any, max_len: int = RAW_VALUE_PREVIEW_LEN) -> Any:
    if isinstance(value, str) and len(value) > max_len:
        return value[:max_len] + "…"
    if isinstance(value, dict):
        return {k: _truncate(v, max_len) for k, v in list(value.items())[:40]}
    if isinstance(value, list):
        return [_truncate(v, max_len) for v in value[:20]]
    return value


def connection_info(client: Any) -> Dict[str, str]:
    from local_runner.client import auth_mode_from_env

    cfg = client.config
    return {
        "project": str(cfg.project or ""),
        "base_url": str(cfg.base_url or ""),
        "auth_mode": auth_mode_from_env(),
    }


# —— Data modeling ——


def _data_model_row_key(row: Mapping[str, str]) -> Tuple[str, str, str]:
    return (row["space"], row["external_id"], row["version"])


def native_cdf_cdm_data_model_row(client: Any) -> Dict[str, str]:
    """Built-in Cognite Core Data Model row (retrieve or canonical fallback)."""
    fallback: Dict[str, str] = {
        "space": NATIVE_CDF_CDM_SPACE,
        "external_id": NATIVE_CDF_CDM_DATA_MODEL_EXTERNAL_ID,
        "version": NATIVE_CDF_CDM_DATA_MODEL_VERSION,
        "name": "Cognite Core Data Model",
    }
    try:
        from cognite.client import data_modeling as dm

        model = client.data_modeling.data_models.retrieve(
            dm.DataModelId(
                NATIVE_CDF_CDM_SPACE,
                NATIVE_CDF_CDM_DATA_MODEL_EXTERNAL_ID,
                NATIVE_CDF_CDM_DATA_MODEL_VERSION,
            )
        )
        return {
            "space": model.space,
            "external_id": model.external_id,
            "version": model.version,
            "name": (model.name or fallback["name"]).strip(),
        }
    except Exception:
        return dict(fallback)


def _ensure_native_cdf_cdm_space(names: List[str]) -> List[str]:
    if NATIVE_CDF_CDM_SPACE not in names:
        names = [NATIVE_CDF_CDM_SPACE, *names]
    names.sort()
    return names


def _ensure_native_cdf_cdm_data_model(
    rows: List[Dict[str, str]],
    *,
    space_filter: Optional[str],
    client: Any,
) -> List[Dict[str, str]]:
    if space_filter and space_filter != NATIVE_CDF_CDM_SPACE:
        return rows
    native = native_cdf_cdm_data_model_row(client)
    key = _data_model_row_key(native)
    if any(_data_model_row_key(r) == key for r in rows):
        return rows
    out = [native, *rows]
    out.sort(key=lambda r: (r["space"], r["external_id"], r["version"]))
    return out


def dm_list_spaces(client: Any, *, limit: int = 2000, include_global: bool = False) -> List[str]:
    seen: set[str] = set()
    names: List[str] = []

    def _collect(*, with_global: bool) -> None:
        for s in client.data_modeling.spaces.list(limit=limit, include_global=with_global):
            sid = getattr(s, "space", None) or str(s)
            if sid not in seen:
                seen.add(sid)
                names.append(sid)

    try:
        _collect(with_global=include_global)
    except TypeError:
        _collect(with_global=False)
    except Exception:
        if include_global:
            _collect(with_global=False)
        else:
            raise
    return _ensure_native_cdf_cdm_space(names)


def _view_to_row(view: Any) -> Dict[str, str]:
    from cognite.client.data_classes.data_modeling.ids import ViewId

    if isinstance(view, ViewId):
        return {
            "space": view.space,
            "external_id": view.external_id,
            "version": view.version,
            "name": "",
        }
    return {
        "space": view.space,
        "external_id": view.external_id,
        "version": view.version,
        "name": (getattr(view, "name", None) or "").strip(),
    }


def dm_retrieve_data_model(
    client: Any,
    *,
    space: str,
    external_id: str,
    version: str,
    inline_views: bool = True,
) -> Any:
    from cognite.client import data_modeling as dm

    dm_id = dm.DataModelId(space.strip(), external_id.strip(), version.strip())
    batch = client.data_modeling.data_models.retrieve(dm_id, inline_views=inline_views)
    models = list(batch) if batch is not None else []
    if not models:
        raise ValueError(f"Data model not found: {space}/{external_id}/{version}")
    return models[0]


def _view_graph_key(space: str, external_id: str, version: str) -> str:
    return f"{space}|{external_id}|{version}"


def _view_key_tuple(view: Any) -> Tuple[str, str, str]:
    return (str(view.space), str(view.external_id), str(view.version))


def _collect_views_for_model(
    client: Any,
    *,
    space: str,
    external_id: str,
    version: str,
) -> List[Any]:
    from cognite.client.data_classes.data_modeling.ids import ViewId
    from cognite.client.data_classes.data_modeling.views import View

    model = dm_retrieve_data_model(
        client, space=space, external_id=external_id, version=version, inline_views=True
    )
    views: List[Any] = []
    pending: List[ViewId] = []
    for item in model.views or []:
        if isinstance(item, View):
            views.append(item)
        elif isinstance(item, ViewId):
            pending.append(item)
        else:
            views.append(item)
    if pending:
        views.extend(client.data_modeling.views.retrieve(pending))
    return views


def _edges_from_view_properties(
    view: Any,
    *,
    model_keys: set[Tuple[str, str, str]],
) -> List[Dict[str, Any]]:
    from cognite.client.data_classes.data_modeling.views import (
        ConnectionDefinition,
        EdgeConnection,
        MappedProperty,
        ReverseDirectRelation,
    )

    edges: List[Dict[str, Any]] = []
    from_key = _view_key_tuple(view)
    if from_key not in model_keys:
        return edges

    for prop_name, prop in (getattr(view, "properties", None) or {}).items():
        label = str(prop_name)
        target_key: Optional[Tuple[str, str, str]] = None
        edge_from = from_key
        edge_to: Optional[Tuple[str, str, str]] = None
        kind = "property"

        if isinstance(prop, MappedProperty):
            src = getattr(prop, "source", None)
            if src is not None:
                target_key = _view_key_tuple(src)
                kind = "direct_relation"
        elif isinstance(prop, ConnectionDefinition):
            src = getattr(prop, "source", None)
            if src is not None:
                target_key = _view_key_tuple(src)
            if isinstance(prop, EdgeConnection):
                kind = "edge_connection"
                if getattr(prop, "direction", "outwards") == "inwards":
                    edge_from = target_key or from_key
                    edge_to = from_key
                    target_key = None
            elif isinstance(prop, ReverseDirectRelation):
                kind = "reverse_direct_relation"
                edge_from = target_key or from_key
                edge_to = from_key
                target_key = None

        if target_key is not None and target_key in model_keys:
            edge_from = from_key
            edge_to = target_key

        if edge_to is None and target_key is not None and target_key in model_keys:
            edge_to = target_key

        if edge_to is None or edge_from == edge_to:
            continue

        fs, fe, fv = edge_from
        ts, te, tv = edge_to
        edges.append(
            {
                "id": f"{_view_graph_key(*edge_from)}->{_view_graph_key(*edge_to)}:{label}",
                "from": {"space": fs, "external_id": fe, "version": fv},
                "to": {"space": ts, "external_id": te, "version": tv},
                "label": label,
                "kind": kind,
            }
        )
    return edges


def dm_data_model_graph(
    client: Any,
    *,
    space: str,
    external_id: str,
    version: str,
) -> Dict[str, Any]:
    space_s = space.strip()
    ext_s = external_id.strip()
    ver_s = version.strip()

    model = dm_retrieve_data_model(
        client, space=space_s, external_id=ext_s, version=ver_s, inline_views=True
    )
    views = _collect_views_for_model(
        client, space=space_s, external_id=ext_s, version=ver_s
    )
    model_keys = {_view_key_tuple(v) for v in views}

    view_nodes: List[Dict[str, Any]] = []
    all_edges: List[Dict[str, Any]] = []
    seen_edge_ids: set[str] = set()

    for v in views:
        sk, ek, vk = _view_key_tuple(v)
        props = getattr(v, "properties", None) or {}
        view_nodes.append(
            {
                "id": _view_graph_key(sk, ek, vk),
                "space": sk,
                "external_id": ek,
                "version": vk,
                "name": (getattr(v, "name", None) or "").strip(),
                "property_count": len(props),
            }
        )
        for edge in _edges_from_view_properties(v, model_keys=model_keys):
            eid = edge["id"]
            if eid in seen_edge_ids:
                continue
            seen_edge_ids.add(eid)
            all_edges.append(edge)

    view_nodes.sort(key=lambda n: (n["external_id"], n["version"]))
    dm_name = (getattr(model, "name", None) or "").strip()

    return {
        "data_model": {
            "space": space_s,
            "external_id": ext_s,
            "version": ver_s,
            "name": dm_name,
        },
        "views": view_nodes,
        "edges": all_edges,
    }


def dm_list_views_for_data_model(
    client: Any,
    *,
    space: str,
    external_id: str,
    version: str,
) -> List[Dict[str, str]]:
    rows = [
        _view_to_row(v)
        for v in _collect_views_for_model(
            client, space=space, external_id=external_id, version=version
        )
    ]
    rows.sort(key=lambda r: (r["external_id"], r["version"]))
    return rows


def dm_list_all_data_models(client: Any, *, limit: int = 2000) -> List[Dict[str, str]]:
    return dm_list_data_models(client, space=None, limit=limit, include_global=True)


def dm_list_data_models(
    client: Any,
    *,
    space: Optional[str] = None,
    limit: int = 2000,
    include_global: bool = False,
) -> List[Dict[str, str]]:
    """List data models in one space, or all spaces when ``space`` is omitted."""
    rows: List[Dict[str, str]] = []
    space_f = space.strip() if space and space.strip() else None
    cap = max(1, min(limit, 2000))
    want_global = include_global or space_f == NATIVE_CDF_CDM_SPACE

    def _append_from_iter(dm_iter: Any) -> None:
        for dm in dm_iter:
            rows.append(
                {
                    "space": dm.space,
                    "external_id": dm.external_id,
                    "version": dm.version,
                    "name": (dm.name or "").strip(),
                }
            )
            if len(rows) >= cap:
                return

    def _list_once(*, space_arg: Optional[str], global_arg: bool) -> None:
        kwargs: Dict[str, Any] = {"limit": cap}
        if space_arg is not None:
            kwargs["space"] = space_arg
        try:
            _append_from_iter(
                client.data_modeling.data_models.list(**kwargs, include_global=global_arg)
            )
        except TypeError:
            try:
                _append_from_iter(client.data_modeling.data_models.list(**kwargs))
            except TypeError:
                _append_from_iter(client.data_modeling.data_models.list(limit=cap))

    try:
        _list_once(space_arg=space_f, global_arg=want_global)
    except Exception:
        if want_global and space_f is None:
            rows.clear()
            _list_once(space_arg=None, global_arg=False)

    # Some projects return nothing for space=None even with include_global; scan spaces.
    if not rows and space_f is None:
        try:
            for sp in dm_list_spaces(client, limit=cap, include_global=include_global):
                if len(rows) >= cap:
                    break
                try:
                    _list_once(space_arg=sp, global_arg=False)
                except Exception:
                    continue
        except Exception:
            pass

    rows = _ensure_native_cdf_cdm_data_model(rows, space_filter=space_f, client=client)
    if len(rows) > cap:
        rows = rows[:cap]
    return rows


# —— RAW ——


def is_system_dm_space(space: str) -> bool:
    return (space or "").strip().startswith(SYSTEM_DM_SPACE_PREFIX)


def fusion_partition_spaces(
    client: Any, *, limit: int = 2000, include_global: bool = True
) -> Tuple[List[str], List[str]]:
    """Return (system_spaces, user_spaces) sorted."""
    names = dm_list_spaces(client, limit=limit, include_global=include_global)
    system = [s for s in names if is_system_dm_space(s)]
    user = [s for s in names if not is_system_dm_space(s)]
    return system, user


def _dm_view_row(v: Any) -> Dict[str, str]:
    name = (getattr(v, "name", None) or "").strip()
    ext = str(v.external_id)
    ver = str(v.version)
    space = str(v.space)
    return {
        "space": space,
        "external_id": ext,
        "version": ver,
        "name": name,
        "label": _discovery_resource_label(name=name or None, external_id=ext, id_val=ver),
    }


def _dm_container_row(c: Any) -> Dict[str, str]:
    name = (getattr(c, "name", None) or "").strip()
    ext = str(c.external_id)
    space = str(c.space)
    return {
        "space": space,
        "external_id": ext,
        "name": name,
        "label": _discovery_resource_label(name=name or None, external_id=ext),
    }


def fusion_list_views_in_space(
    client: Any,
    space: str,
    *,
    limit: int = 2000,
    include_global: bool = False,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    cap = max(1, min(limit, 2000))
    space_s = space.strip()
    try:
        view_iter = client.data_modeling.views(
            chunk_size=min(250, cap),
            space=space_s,
            include_global=include_global,
        )
    except TypeError:
        view_iter = client.data_modeling.views(chunk_size=min(250, cap), space=space_s)
    for view_list in view_iter:
        for v in view_list:
            rows.append(_dm_view_row(v))
            if len(rows) >= cap:
                break
        if len(rows) >= cap:
            break
    rows.sort(key=lambda r: str(r.get("label") or "").lower())
    return rows


def fusion_list_containers_in_space(
    client: Any,
    space: str,
    *,
    limit: int = 2000,
    include_global: bool = False,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    cap = max(1, min(limit, 2000))
    space_s = space.strip()
    try:
        container_iter = client.data_modeling.containers(
            chunk_size=min(250, cap),
            space=space_s,
            include_global=include_global,
        )
    except TypeError:
        container_iter = client.data_modeling.containers(
            chunk_size=min(250, cap), space=space_s
        )
    for container_list in container_iter:
        for c in container_list:
            rows.append(_dm_container_row(c))
            if len(rows) >= cap:
                break
        if len(rows) >= cap:
            break
    rows.sort(key=lambda r: str(r.get("label") or "").lower())
    return rows


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if hasattr(value, "dump"):
        try:
            return _json_safe(value.dump())
        except Exception:
            pass
    if hasattr(value, "as_dict"):
        try:
            return _json_safe(value.as_dict())
        except Exception:
            pass
    if hasattr(value, "model_dump"):
        try:
            return _json_safe(value.model_dump())
        except Exception:
            pass
    return str(value)


def _dm_property_type_to_dict(prop_type: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for attr in ("type", "list", "nullable", "autoIncrement", "auto_increment"):
        if hasattr(prop_type, attr):
            val = getattr(prop_type, attr)
            if val is not None:
                key = "autoIncrement" if attr == "auto_increment" else attr
                out[key] = _json_safe(val)
    if not out:
        out["type"] = str(prop_type)
    return out


def dm_container_to_dict(container: Any) -> Dict[str, Any]:
    props: Dict[str, Any] = {}
    raw_props = getattr(container, "properties", None) or {}
    if isinstance(raw_props, dict):
        for name, prop in raw_props.items():
            props[str(name)] = _dm_property_type_to_dict(prop)

    indexes: List[Any] = []
    raw_indexes = getattr(container, "indexes", None) or {}
    if isinstance(raw_indexes, dict):
        for name, idx in raw_indexes.items():
            entry: Dict[str, Any] = {"name": str(name)}
            for attr in ("properties", "cursorable", "indexType", "index_type"):
                if hasattr(idx, attr):
                    key = "indexType" if attr == "index_type" else attr
                    entry[key] = _json_safe(getattr(idx, attr))
            indexes.append(entry)

    constraints: List[Any] = []
    raw_constraints = getattr(container, "constraints", None) or {}
    if isinstance(raw_constraints, dict):
        for name, con in raw_constraints.items():
            entry = {"name": str(name)}
            for attr in ("constraintType", "constraint_type", "require", "properties"):
                if hasattr(con, attr):
                    key = "constraintType" if attr == "constraint_type" else attr
                    entry[key] = _json_safe(getattr(con, attr))
            constraints.append(entry)

    out: Dict[str, Any] = {
        "space": str(container.space),
        "external_id": str(container.external_id),
        "name": getattr(container, "name", None),
        "description": getattr(container, "description", None),
        "usedFor": _json_safe(getattr(container, "usedFor", None) or getattr(container, "used_for", None)),
        "properties": props,
        "indexes": indexes,
        "constraints": constraints,
    }
    for ts in ("createdTime", "created_time", "lastUpdatedTime", "last_updated_time"):
        if hasattr(container, ts):
            val = getattr(container, ts)
            if val is not None:
                key = "createdTime" if "created" in ts else "lastUpdatedTime"
                out[key] = _json_safe(val)
    return out


def dm_retrieve_container(client: Any, *, space: str, external_id: str) -> Dict[str, Any]:
    from cognite.client.data_classes.data_modeling import ContainerId

    cid = ContainerId(space=space.strip(), external_id=external_id.strip())
    containers = client.data_modeling.containers.retrieve(cid)
    if not containers:
        raise ValueError(f"Container not found: {space}/{external_id}")
    container = containers[0] if isinstance(containers, list) else containers
    return dm_container_to_dict(container)


def _dm_instance_ref_to_dict(ref: Any) -> Dict[str, Any]:
    if ref is None:
        return {}
    out: Dict[str, Any] = {}
    space = getattr(ref, "space", None)
    if space is not None:
        out["space"] = str(space)
    ext = getattr(ref, "external_id", None)
    if ext is None:
        ext = getattr(ref, "externalId", None)
    if ext is not None:
        out["external_id"] = str(ext)
    return out


def dm_node_to_dict(node: Any) -> Dict[str, Any]:
    props: Dict[str, Any] = {}
    raw_props = getattr(node, "properties", None) or {}
    if isinstance(raw_props, dict):
        for view_key, bag in raw_props.items():
            key = str(view_key)
            if hasattr(bag, "dump"):
                props[key] = _json_safe(bag.dump())
            elif isinstance(bag, dict):
                props[key] = _json_safe(bag)
            else:
                props[key] = _json_safe(bag)

    sources: List[Any] = []
    raw_sources = getattr(node, "sources", None) or []
    if isinstance(raw_sources, (list, tuple)):
        for src in raw_sources:
            entry: Dict[str, Any] = {}
            for attr in ("source", "properties"):
                if hasattr(src, attr):
                    entry[attr] = _json_safe(getattr(src, attr))
            if entry:
                sources.append(entry)

    out: Dict[str, Any] = {
        "space": str(node.space),
        "external_id": str(node.external_id),
        "version": _json_safe(getattr(node, "version", None)),
        "properties": props,
        "sources": sources,
    }
    for ts in ("created_time", "last_updated_time"):
        if hasattr(node, ts):
            out[ts] = _json_safe(getattr(node, ts))
    return out


def dm_edge_to_dict(edge: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "space": str(edge.space),
        "external_id": str(edge.external_id),
        "type": _json_safe(getattr(edge, "type", None) or getattr(edge, "edge_type", None)),
        "start_node": _dm_instance_ref_to_dict(getattr(edge, "start_node", None) or getattr(edge, "startNode", None)),
        "end_node": _dm_instance_ref_to_dict(getattr(edge, "end_node", None) or getattr(edge, "endNode", None)),
        "properties": _json_safe(getattr(edge, "properties", None) or {}),
    }
    for ts in ("created_time", "last_updated_time"):
        if hasattr(edge, ts):
            out[ts] = _json_safe(getattr(edge, ts))
    return out


def dm_retrieve_node(client: Any, *, space: str, external_id: str) -> Dict[str, Any]:
    from cognite.client.data_classes.data_modeling import NodeId

    nid = NodeId(space=space.strip(), external_id=external_id.strip())
    nodes = client.data_modeling.instances.retrieve_nodes(nid)
    if not nodes:
        raise ValueError(f"Node not found: {space}/{external_id}")
    node = nodes[0] if isinstance(nodes, list) else nodes
    return dm_node_to_dict(node)


def dm_retrieve_edge(client: Any, *, space: str, external_id: str) -> Dict[str, Any]:
    from cognite.client.data_classes.data_modeling import EdgeId

    eid = EdgeId(space=space.strip(), external_id=external_id.strip())
    edges = client.data_modeling.instances.retrieve_edges(eid)
    if not edges:
        raise ValueError(f"Edge not found: {space}/{external_id}")
    edge = edges[0] if isinstance(edges, list) else edges
    return dm_edge_to_dict(edge)


def fusion_view_key_lookup(
    views: List[Dict[str, str]],
) -> Dict[Tuple[str, str, str], Dict[str, str]]:
    return {
        (v["space"], v["external_id"], v["version"]): v
        for v in views
    }


def fusion_view_by_container_lookup(
    views: List[Dict[str, str]],
) -> Dict[Tuple[str, str], Dict[str, str]]:
    """Map container (space, external_id) to a queryable view (prefer highest version)."""
    best: Dict[Tuple[str, str], Dict[str, str]] = {}
    for v in views:
        key = (v["space"], v["external_id"])
        cur = best.get(key)
        if cur is None or str(v["version"]) > str(cur["version"]):
            best[key] = v
    return best


def fusion_open_target_for_container(
    container: Mapping[str, str],
    views_by_container_key: Dict[Tuple[str, str], Dict[str, str]],
) -> Optional[Dict[str, str]]:
    """When a view shares space and externalId with the container, instances are queryable."""
    key = (container["space"], container["external_id"])
    view = views_by_container_key.get(key)
    if view is None:
        return None
    return {
        "type": "dm_instances",
        "view_space": view["space"],
        "view_external_id": view["external_id"],
        "view_version": view["version"],
    }


def raw_list_databases(client: Any, *, limit: int = 500) -> List[str]:
    lim = max(1, min(limit, 1000))
    dbs = client.raw.databases.list(limit=lim)
    return sorted(dbs.as_names() if hasattr(dbs, "as_names") else [str(d) for d in dbs])


def raw_list_tables(client: Any, *, database: str, limit: int = 1000) -> List[Dict[str, Any]]:
    lim = max(1, min(limit, 1000))
    tables = client.raw.tables.list(database, limit=lim)
    rows: List[Dict[str, Any]] = []
    for t in tables:
        rows.append(
            {
                "name": getattr(t, "name", None) or str(t),
                "row_count": getattr(t, "row_count", None),
            }
        )
    rows.sort(key=lambda r: r["name"])
    return rows


# —— Transformations, workflows, pipelines, governance ——


def _discovery_resource_label(
    *,
    name: Any = None,
    external_id: Any = None,
    id_val: Any = None,
) -> str:
    n = (str(name).strip() if name is not None else "") or ""
    if n:
        return n
    ext = (str(external_id).strip() if external_id is not None else "") or ""
    if ext:
        return ext
    if id_val is not None:
        return str(id_val)
    return "—"


def list_transformations(client: Any, *, limit: int = 500) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for t in client.transformations.list(limit=limit):
        ext = getattr(t, "external_id", None)
        name = getattr(t, "name", None)
        tid = getattr(t, "id", None)
        rows.append(
            {
                "id": tid,
                "external_id": ext,
                "name": name,
                "label": _discovery_resource_label(name=name, external_id=ext, id_val=tid),
                "created_time": _iso(getattr(t, "created_time", None)),
                "data_set_id": getattr(t, "data_set_id", None),
            }
        )
    rows.sort(key=lambda r: str(r.get("label") or "").lower())
    return rows


def _serialize_transformation_definition(t: Any) -> Dict[str, Any]:
    if hasattr(t, "dump"):
        dumped = t.dump(camel_case=False)
        if isinstance(dumped, dict):
            return _truncate(dumped)
    return {
        "id": getattr(t, "id", None),
        "external_id": getattr(t, "external_id", None),
        "name": getattr(t, "name", None),
    }


def get_transformation_detail(client: Any, *, transformation_id: int) -> Dict[str, Any]:
    if transformation_id <= 0:
        raise ValueError("transformation_id must be a positive integer")
    t = client.transformations.retrieve(id=int(transformation_id))
    query = getattr(t, "query", None) or ""
    destination = getattr(t, "destination", None)
    schedule = getattr(t, "schedule", None)
    return {
        "id": getattr(t, "id", None),
        "external_id": getattr(t, "external_id", None),
        "name": getattr(t, "name", None),
        "query": query if isinstance(query, str) else str(query),
        "created_time": _iso(getattr(t, "created_time", None)),
        "last_updated_time": _iso(getattr(t, "last_updated_time", None)),
        "data_set_id": getattr(t, "data_set_id", None),
        "is_public": getattr(t, "is_public", None),
        "has_destination": destination is not None,
        "conflict_mode": getattr(t, "conflict_mode", None),
        "destination": _truncate(destination.dump(camel_case=False))
        if destination is not None and hasattr(destination, "dump")
        else _truncate(destination),
        "schedule": _truncate(schedule.dump(camel_case=False))
        if schedule is not None and hasattr(schedule, "dump")
        else _truncate(schedule),
        "definition": _serialize_transformation_definition(t),
    }


def list_functions(client: Any, *, limit: int = 500) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for f in client.functions.list(limit=limit):
        fid = getattr(f, "id", None)
        ext = getattr(f, "external_id", None)
        name = getattr(f, "name", None)
        rows.append(
            {
                "id": fid,
                "external_id": ext,
                "name": name,
                "label": _discovery_resource_label(name=name, external_id=ext, id_val=fid),
                "status": getattr(f, "status", None),
                "file_id": getattr(f, "file_id", None),
                "owner": getattr(f, "owner", None),
                "created_time": _iso(getattr(f, "created_time", None)),
            }
        )
    rows.sort(key=lambda r: str(r.get("label") or "").lower())
    return rows


def _serialize_function_definition(f: Any) -> Dict[str, Any]:
    if hasattr(f, "dump"):
        dumped = f.dump(camel_case=False)
        if isinstance(dumped, dict):
            return _truncate(dumped)
    return {
        "id": getattr(f, "id", None),
        "external_id": getattr(f, "external_id", None),
        "name": getattr(f, "name", None),
    }


def get_function_detail(client: Any, *, function_id: str) -> Dict[str, Any]:
    fid = (function_id or "").strip()
    if not fid:
        raise ValueError("function_id is required")
    if fid.isdigit():
        f = client.functions.retrieve(id=int(fid))
    else:
        f = client.functions.retrieve(external_id=fid)
    return {
        "id": getattr(f, "id", None),
        "external_id": getattr(f, "external_id", None),
        "name": getattr(f, "name", None),
        "description": getattr(f, "description", None),
        "status": getattr(f, "status", None),
        "file_id": getattr(f, "file_id", None),
        "owner": getattr(f, "owner", None),
        "created_time": _iso(getattr(f, "created_time", None)),
        "definition": _serialize_function_definition(f),
    }


def _workflow_version_sort_key(version: str) -> Tuple[int, Any]:
    ver = (version or "").strip()
    if ver.lower().startswith("v") and ver[1:].isdigit():
        return (0, int(ver[1:]))
    if ver.isdigit():
        return (0, int(ver))
    return (1, ver)


def _resolve_workflow_version(
    client: Any,
    workflow_external_id: str,
    version: Optional[str],
) -> Any:
    from cognite.client.data_classes import WorkflowVersionId

    ext = workflow_external_id.strip()
    ver_s = (version or "").strip()
    if ver_s:
        wv = client.workflows.versions.retrieve(WorkflowVersionId(ext, ver_s))
        if wv is None:
            raise ValueError(f"Workflow version not found: {ext}/{ver_s}")
        return wv

    versions = list(client.workflows.versions.list([ext], limit=500))
    if not versions:
        raise ValueError(f"No versions found for workflow {ext!r}")
    return max(versions, key=lambda v: _workflow_version_sort_key(v.version))


def _serialize_task_parameters(params: Any) -> Dict[str, Any]:
    if params is None:
        return {}
    if hasattr(params, "dump"):
        dumped = params.dump(camel_case=False)
        if isinstance(dumped, dict):
            return _truncate(dumped)
    return {}


def _workflow_task_graph_node(task: Any) -> Dict[str, Any]:
    ext = str(task.external_id)
    name = (getattr(task, "name", None) or "").strip()
    return {
        "id": ext,
        "external_id": ext,
        "name": name,
        "type": str(getattr(task, "type", "") or ""),
        "label": name or ext,
        "description": (getattr(task, "description", None) or "").strip(),
        "retries": getattr(task, "retries", None),
        "timeout": getattr(task, "timeout", None),
        "on_failure": getattr(task, "on_failure", None),
        "parameters": _serialize_task_parameters(getattr(task, "parameters", None)),
    }


def workflow_graph(
    client: Any,
    *,
    workflow_external_id: str,
    version: Optional[str] = None,
) -> Dict[str, Any]:
    """Serializable task DAG for a workflow version (dependsOn → edges)."""
    wv = _resolve_workflow_version(client, workflow_external_id, version)
    wdef = wv.workflow_definition
    tasks = list(getattr(wdef, "tasks", None) or [])
    task_ids = {str(t.external_id) for t in tasks}

    nodes = [_workflow_task_graph_node(t) for t in tasks]
    edges: List[Dict[str, Any]] = []
    seen_edge_ids: set[str] = set()

    for t in tasks:
        target = str(t.external_id)
        for dep in getattr(t, "depends_on", None) or []:
            dep_s = str(dep)
            if dep_s not in task_ids:
                continue
            eid = f"{dep_s}->{target}"
            if eid in seen_edge_ids:
                continue
            seen_edge_ids.add(eid)
            edges.append({"id": eid, "from": dep_s, "to": target, "label": ""})

    nodes.sort(key=lambda n: n["external_id"])
    desc = (getattr(wdef, "description", None) or "").strip()

    return {
        "workflow": {
            "external_id": wv.workflow_external_id,
            "version": wv.version,
            "description": desc,
            "name": wv.workflow_external_id,
            "task_count": len(nodes),
        },
        "tasks": nodes,
        "edges": edges,
    }


def list_workflows(client: Any, *, limit: int = 500) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for w in client.workflows.list(limit=limit):
        ext = getattr(w, "external_id", None)
        name = getattr(w, "name", None)
        rows.append(
            {
                "external_id": ext,
                "name": name,
                "label": _discovery_resource_label(name=name, external_id=ext),
                "created_time": _iso(getattr(w, "created_time", None)),
                "data_set_id": getattr(w, "data_set_id", None),
            }
        )
    rows.sort(key=lambda r: str(r.get("label") or "").lower())
    return rows


def list_extraction_pipelines(client: Any, *, limit: int = 500) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for p in client.extraction_pipelines.list(limit=limit):
        ext = getattr(p, "external_id", None)
        name = getattr(p, "name", None)
        rows.append(
            {
                "external_id": ext,
                "name": name,
                "label": _discovery_resource_label(name=name, external_id=ext),
                "created_time": _iso(getattr(p, "created_time", None)),
                "data_set_id": getattr(p, "data_set_id", None),
            }
        )
    rows.sort(key=lambda r: str(r.get("label") or "").lower())
    return rows


def _security_groups_api(client: Any) -> Any:
    """SDK 7.x exposes groups on ``client.iam.groups``; older paths use ``client.groups``."""
    api = getattr(client, "groups", None)
    if api is not None:
        return api
    iam = getattr(client, "iam", None)
    if iam is not None:
        api = getattr(iam, "groups", None)
        if api is not None:
            return api
    raise AttributeError(
        "CogniteClient has no security groups API (expected client.groups or client.iam.groups)"
    )


def list_governance_spaces(
    client: Any, *, limit: int = 2000, include_global: bool = True
) -> List[Dict[str, str]]:
    names = dm_list_spaces(client, limit=limit, include_global=include_global)
    return [{"space": s, "label": s} for s in names]


def get_governance_space_detail(client: Any, *, space: str) -> Dict[str, Any]:
    """Best-effort space metadata for Discovery tree detail tab."""
    out: Dict[str, Any] = {"space": space}
    try:
        spaces_api = getattr(client, "data_modeling", None)
        if spaces_api is not None and hasattr(spaces_api, "spaces"):
            for s in spaces_api.spaces.list(limit=1000):
                sid = getattr(s, "space", None) or getattr(s, "external_id", None)
                if str(sid) == space:
                    out["name"] = getattr(s, "name", None)
                    out["description"] = getattr(s, "description", None)
                    out["is_global"] = getattr(s, "is_global", None)
                    break
    except Exception as e:
        out["lookup_error"] = str(e)
    return out


def get_governance_group_detail(client: Any, *, group_id: int) -> Dict[str, Any]:
    g = _security_groups_api(client).retrieve(id=group_id)
    caps = []
    for c in getattr(g, "capabilities", None) or []:
        if hasattr(c, "dump"):
            caps.append(c.dump(camel_case=False))
        elif isinstance(c, dict):
            caps.append(c)
        else:
            caps.append({"raw": str(c)})
    return {
        "id": getattr(g, "id", group_id),
        "name": getattr(g, "name", None),
        "source_id": getattr(g, "source_id", None),
        "member_count": len(getattr(g, "member_ids", None) or []),
        "capabilities": caps,
    }


def list_security_groups(client: Any, *, limit: int = 2000) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    lim = max(1, min(limit, 10_000))
    groups_api = _security_groups_api(client)
    try:
        groups = groups_api.list(all=True)
    except TypeError:
        groups = groups_api.list(limit=lim)
    for g in groups[:lim]:
        gid = getattr(g, "id", None)
        name = getattr(g, "name", None)
        rows.append(
            {
                "id": gid,
                "name": name,
                "source_id": getattr(g, "source_id", None),
                "label": _discovery_resource_label(name=name, id_val=gid),
                "member_count": len(getattr(g, "member_ids", None) or []),
            }
        )
    rows.sort(key=lambda r: str(r.get("label") or "").lower())
    return rows


# —— SQL preview ——


def _serialize_preview_schema_col(col: Any) -> Dict[str, Any]:
    if hasattr(col, "dump"):
        dumped = col.dump(camel_case=False)
        if isinstance(dumped, dict):
            return dumped
    col_type = getattr(col, "type", None)
    if hasattr(col_type, "dump"):
        type_val: Any = col_type.dump(camel_case=False)
    elif col_type is not None:
        type_val = str(col_type)
    else:
        type_val = None
    return {
        "name": getattr(col, "name", None),
        "sql_type": getattr(col, "sql_type", None),
        "nullable": getattr(col, "nullable", None),
        "type": type_val,
    }


def run_sql_preview(
    client: Any,
    *,
    query: str,
    limit: int = 100,
    source_limit: Optional[int] = None,
    convert_to_string: bool = True,
    infer_schema_limit: Optional[int] = None,
    timeout: Optional[int] = None,
) -> Dict[str, Any]:
    q = query.strip()
    if not q:
        raise ValueError("query is required")

    kwargs: Dict[str, Any] = {
        "query": q,
        "convert_to_string": convert_to_string,
        "limit": limit,
        "source_limit": source_limit,
    }
    if infer_schema_limit is not None:
        kwargs["infer_schema_limit"] = infer_schema_limit
    if timeout is not None:
        kwargs["timeout"] = timeout

    preview = client.transformations.preview(**kwargs)
    schema_cols: List[Dict[str, Any]] = []
    if preview.schema:
        for col in preview.schema:
            schema_cols.append(_serialize_preview_schema_col(col))

    items = preview.results or []
    columns: List[str] = []
    if items:
        columns = list(items[0].keys())
    elif schema_cols:
        columns = [str(c["name"]) for c in schema_cols if c.get("name")]

    return {
        "columns": columns,
        "items": [_truncate(row) for row in items],
        "schema": schema_cols,
        "row_count": len(items),
    }
