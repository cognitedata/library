"""CDF browse helpers for workflow palette data tree (copied from cdf_explorer cdf_browse.py)."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

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


def _data_model_row_key(row: Mapping[str, str]) -> Tuple[str, str, str]:
    return (row["space"], row["external_id"], row["version"])


def native_cdf_cdm_data_model_row(client: Any) -> Dict[str, str]:
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


def dm_list_data_models(
    client: Any,
    *,
    space: Optional[str] = None,
    limit: int = 2000,
    include_global: bool = False,
) -> List[Dict[str, str]]:
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


def dm_list_all_data_models(client: Any, *, limit: int = 2000) -> List[Dict[str, str]]:
    return dm_list_data_models(client, space=None, limit=limit, include_global=True)


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
