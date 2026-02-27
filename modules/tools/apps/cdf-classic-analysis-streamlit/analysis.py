"""
Classic CDF model analysis: metadata field distribution.
Port of the TypeScript analysis module for use with the Python Cognite SDK.
"""
from __future__ import annotations

import re
from typing import Any, Optional

CDF_FILTER_MAX_LEN = 64
DOCUMENTS_FILTER_MAX_LEN = 512
LIST_LIMIT = 1000
RAW_DBS_LIST_CAP = 50
RAW_TABLES_PER_DB_LIMIT = 1000
VALUE_TOO_LONG_PREVIEW_LEN = 1024

ResourceType = str  # "assets" | "timeseries" | "events" | "sequences" | "files"


def _project_path(project: str, resource: str) -> str:
    from urllib.parse import quote
    return f"/api/v1/projects/{quote(project, safe='')}/{resource}/aggregate"


def _project_resource_path(project: str, segment: str) -> str:
    from urllib.parse import quote
    return f"/api/v1/projects/{quote(project, safe='')}/{segment}"


def _project_list_path(project: str, segment: str) -> str:
    from urllib.parse import quote
    return f"/api/v1/projects/{quote(project, safe='')}/{segment}"


def _parse_list_response(res: Any) -> list:
    if res is None or not isinstance(res, dict):
        return []
    items = res.get("items")
    if isinstance(items, list):
        return items
    data = res.get("data")
    if data is not None and isinstance(data, dict):
        if isinstance(data.get("items"), list):
            return data["items"]
        inner = data.get("data")
        if isinstance(inner, dict) and isinstance(inner.get("items"), list):
            return inner["items"]
    for key in ("value", "result", "data"):
        v = res.get(key)
        if isinstance(v, list):
            return v
        if isinstance(v, dict) and isinstance(v.get("items"), list):
            return v["items"]
    if isinstance(res, list):
        return res
    return []


def _parse_count_response(res: Any) -> int:
    if res is None or not isinstance(res, dict):
        return 0
    # Top-level count (some API wrappers)
    for key in ("count", "totalCount", "total_count", "value"):
        v = res.get(key)
        if isinstance(v, (int, float)):
            return int(v)
    # Nested items[0].count (standard CDF aggregate response)
    items = res.get("items")
    if not isinstance(items, list) and res.get("data") is not None:
        d = res["data"]
        if isinstance(d, dict):
            items = d.get("items") or (d.get("data") or {}).get("items") if isinstance(d.get("data"), dict) else None
    if isinstance(items, list) and len(items) > 0:
        first = items[0]
        if isinstance(first, dict):
            count = first.get("count")
            if isinstance(count, (int, float)):
                return int(count)
        elif isinstance(first, (int, float)):
            return int(first)
    return 0


def _data_set_filter_aggregate(data_set_ids: Optional[list[dict]]) -> dict:
    """Match Dune app: CDF aggregate endpoints use advancedFilter with property [\"dataSetId\"] (not filter.dataSetIds)."""
    if not data_set_ids:
        return {}
    id_objs = [x for x in data_set_ids if isinstance(x.get("id"), (int, float))]
    numeric_ids = [int(x["id"]) for x in id_objs if abs(int(x["id"])) != float("inf") and int(x["id"]) >= 1]
    if not numeric_ids:
        return {}
    if len(numeric_ids) == 1:
        return {"advancedFilter": {"equals": {"property": ["dataSetId"], "value": numeric_ids[0]}}}
    return {"advancedFilter": {"in": {"property": ["dataSetId"], "values": numeric_ids}}}


def _documents_data_set_filter(data_set_ids: Optional[list[dict]]) -> dict:
    if not data_set_ids:
        return {}
    numeric_ids = [x["id"] for x in data_set_ids if isinstance(x.get("id"), (int, float))]
    numeric_ids = [int(x) for x in numeric_ids if abs(x) != float("inf") and x >= 1]
    if not numeric_ids:
        return {}
    in_clause = {"in": {"property": ["sourceFile", "datasetId"], "values": numeric_ids}}
    return {"filter": {"and": [{"and": [{"or": [in_clause]}]}]}}


def _files_data_set_filter(data_set_ids: Optional[list[dict]]) -> dict:
    """Files API uses filter only (no advancedFilter). filter.dataSetIds for dataset scope."""
    if not data_set_ids:
        return {}
    id_objs = [x for x in data_set_ids if isinstance(x.get("id"), (int, float))]
    numeric_ids = [int(x["id"]) for x in id_objs if abs(int(x["id"])) != float("inf") and int(x["id"]) >= 1]
    if not numeric_ids:
        return {}
    return {"filter": {"dataSetIds": [{"id": i} for i in numeric_ids]}}


def _aggregate_resource(resource_type: str) -> str:
    """For UI 'files' use Documents API (documents/aggregate); it supports aggregate/uniqueValues."""
    return "documents" if resource_type == "files" else resource_type


def _get_property_path(filter_key: str, resource_type: str) -> list:
    if resource_type == "events":
        return ["type"]
    return ["metadata", filter_key]


def _get_timeseries_property_path(filter_key: str) -> list:
    n = filter_key.strip().lower().replace(" ", " ")
    if n in ("is step", "isstep"):
        return ["isStep"]
    if n in ("is string", "isstring"):
        return ["isString"]
    if n in ("unit", "units"):
        return ["unit"]
    return ["metadata", filter_key.strip()]


def _get_documents_property_path(filter_key: str) -> list:
    n = filter_key.strip().lower()
    if n == "type":
        return ["type"]
    if n == "labels":
        return ["labels"]
    if n == "author":
        return ["author"]
    if n == "source":
        return ["sourceFile", "source"]
    return ["sourceFile", "metadata", filter_key.strip()]


def _get_files_property_path(filter_key: str) -> list:
    """Property path for /files/aggregate (Files API)."""
    n = filter_key.strip().lower()
    if n == "type":
        return ["type"]
    return ["metadata", filter_key.strip()]


def _advanced_filter_equals(property_path: list, value: str | bool) -> dict:
    return {"advancedFilter": {"equals": {"property": property_path, "value": value}}}


def _filter_equals(property_path: list, value: str | bool) -> dict:
    return {"filter": {"equals": {"property": property_path, "value": value}}}


def _value_too_long_meta_part(value: str) -> str:
    preview = value[:VALUE_TOO_LONG_PREVIEW_LEN]
    suffix = "..." if len(value) > VALUE_TOO_LONG_PREVIEW_LEN else ""
    return f"Metadata keys: (value too long to list; first {VALUE_TOO_LONG_PREVIEW_LEN} chars: {preview}{suffix})\n\n"


def _item_value(item: dict) -> Optional[tuple[str, int]]:
    raw = item.get("value")
    if raw is None and isinstance(item.get("values"), list) and item["values"]:
        raw = item["values"][0]
    if raw is None:
        return None
    if isinstance(raw, str):
        value = raw
    elif isinstance(raw, (bool, int, float)):
        value = str(raw)
    else:
        value = str(raw)
    count = item.get("count", 0) or 0
    return (value, int(count))


def _unique_properties_keys(items: list) -> list[str]:
    out = []
    for p in items:
        if not isinstance(p, dict):
            continue
        prop = p.get("property")
        if not prop and isinstance(p.get("values"), list) and p["values"] and isinstance(p["values"][0], dict):
            prop = p["values"][0].get("property")
        if isinstance(prop, list) and len(prop) > 0:
            out.append(prop[-1])
    return out


def _unique_properties_keys_documents(items: list) -> list[str]:
    out = []
    for p in items:
        if not isinstance(p, dict):
            continue
        first = (p.get("values") or [None])[0] if p.get("values") else None
        if isinstance(first, str):
            out.append(first)
            continue
        prop = p.get("property") or (first.get("property") if isinstance(first, dict) else None)
        if isinstance(prop, list) and len(prop) > 0:
            out.append(prop[-1])
    return out


def _unwrap_maybe_coro(x: Any) -> Any:
    """If x is a coroutine/awaitable or JS Promise (thenable), run it and return the result.
    In Pyodide, wrap coroutines in a fresh async wrapper so the same coroutine is only awaited once
    (avoids RuntimeError: cannot reuse already awaited coroutine).
    """
    import asyncio
    for _ in range(32):  # avoid infinite loop on nested promises
        # Treat JS Promise / thenable (e.g. from CDF client in Pyodide) as awaitable
        is_thenable = hasattr(x, "then") and callable(getattr(x, "then", None))
        if is_thenable and not asyncio.iscoroutine(x):
            async def _await_thenable(thenable: Any) -> Any:
                return await thenable
            x = _await_thenable(x)
        is_awaitable = asyncio.iscoroutine(x) or (
            hasattr(x, "__await__") and callable(getattr(x, "__await__", None))
        )
        if not is_awaitable:
            return x
        # Wrap so the same awaitable is only awaited once (avoids "cannot reuse already awaited coroutine" in Pyodide)
        async def _run_once(awaitable: Any) -> Any:
            return await awaitable
        runner = _run_once(x)
        out = None
        try:
            from pyodide.ffi import run_sync as pyodide_run_sync
        except Exception:
            pyodide_run_sync = None
        if pyodide_run_sync is not None:
            try:
                out = pyodide_run_sync(runner)
            except Exception:
                pass
        if out is None:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    raise RuntimeError("Loop already running")
                out = loop.run_until_complete(runner)
            except (RuntimeError, AttributeError, OSError):
                try:
                    out = asyncio.run(runner)
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        out = loop.run_until_complete(runner)
                    finally:
                        try:
                            loop.close()
                        except Exception:
                            pass
        if out is None:
            return x
        # Wait for Future/Task if not yet done (Pyodide run_sync can return an incomplete Future)
        if isinstance(out, asyncio.Task):
            if not out.done():
                try:
                    loop = asyncio.get_event_loop()
                    if not loop.is_running():
                        loop.run_until_complete(out)
                except Exception:
                    try:
                        asyncio.run(out)
                    except Exception:
                        pass
            try:
                out = out.result()
            except Exception as e:
                if "Result is not set" in str(e) or "not set" in str(e).lower():
                    try:
                        # concurrent.futures.Future supports timeout; asyncio.Future does not
                        out = out.result(timeout=60)
                    except TypeError:
                        out = out.result()
                    except Exception:
                        raise e
                else:
                    raise
        elif getattr(out, "done", None) and callable(out.done) and getattr(out, "result", None):
            try:
                out = out.result()
            except Exception as e:
                if "Result is not set" in str(e) or "not set" in str(e).lower():
                    try:
                        out = out.result(timeout=60)
                    except TypeError:
                        out = out.result()
                    except Exception:
                        raise e
                else:
                    raise
        x = out
    return x


class ClientAdapter:
    """Thin adapter over CogniteClient for POST/GET returning parsed JSON."""

    def __init__(self, client: Any, project: str):
        self._client = client
        self._project = project

    def post(self, path: str, body: dict) -> dict:
        resp = self._client.post(path, json=body)
        resp = _unwrap_maybe_coro(resp)
        if hasattr(resp, "json"):
            data = resp.json()
            data = _unwrap_maybe_coro(data)
            return data if isinstance(data, dict) else {}
        return resp if isinstance(resp, dict) else {}

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        resp = self._client.get(path, params=params or {})
        resp = _unwrap_maybe_coro(resp)
        if hasattr(resp, "json"):
            data = resp.json()
            data = _unwrap_maybe_coro(data)
            return data if isinstance(data, dict) else {}
        return resp if isinstance(resp, dict) else {}


def get_total_count(
    client: ClientAdapter,
    project: str,
    resource_type: str,
    data_set_ids: Optional[list[dict]],
) -> int:
    """Files use Documents API (aggregate + filter). Other resources use advancedFilter + aggregate."""
    path = _project_path(project, _aggregate_resource(resource_type))
    if resource_type == "files":
        body = {"aggregate": "count", **_documents_data_set_filter(data_set_ids)}
    else:
        body = {"aggregate": "count", **_data_set_filter_aggregate(data_set_ids)}
    res = client.post(path, body)
    res = _unwrap_maybe_coro(res)
    count = _parse_count_response(res)
    count = _unwrap_maybe_coro(count)
    return int(count) if isinstance(count, (int, float)) else 0


def get_aggregate_count_no_filter(client: ClientAdapter, project: str, resource_type: str) -> int:
    path = _project_path(project, _aggregate_resource(resource_type))
    res = client.post(path, {"aggregate": "count"})
    return _parse_count_response(res)


def _get_list_count_post_only(client: ClientAdapter, project: str, list_segment: str, body: Optional[dict] = None) -> int:
    path = _project_list_path(project, list_segment)
    payload = {"limit": LIST_LIMIT, **(body or {})}
    res = client.post(path, payload)
    return len(_parse_list_response(res))


def _get_transformations_count(client: ClientAdapter, project: str) -> int:
    path = _project_resource_path(project, "transformations/")
    res = client.get(path, {"limit": LIST_LIMIT, "includePublic": True})
    return len(_parse_list_response(res))


def _get_workflows_count(client: ClientAdapter, project: str) -> int:
    path = _project_resource_path(project, "workflows/")
    res = client.get(path, {"limit": LIST_LIMIT})
    return len(_parse_list_response(res))


def get_global_extended_counts(client: ClientAdapter, project: str) -> dict:
    out = {"transformations": 0, "functions": 0, "workflows": 0, "rawTables": 0}
    try:
        out["transformations"] = _get_transformations_count(client, project)
    except Exception:
        pass
    try:
        out["functions"] = _get_list_count_post_only(client, project, "functions/list")
    except Exception:
        pass
    try:
        out["workflows"] = _get_workflows_count(client, project)
    except Exception:
        pass
    try:
        dbs_path = _project_resource_path(project, "raw/dbs")
        dbs_res = client.get(dbs_path, {"limit": RAW_DBS_LIST_CAP})
        dbs = _parse_list_response(dbs_res)
        from urllib.parse import quote
        total = 0
        for db in dbs:
            name = db.get("name") if isinstance(db, dict) else None
            if not name:
                continue
            tables_path = _project_resource_path(project, f"raw/dbs/{quote(str(name))}/tables")
            tables_res = client.get(tables_path, {"limit": RAW_TABLES_PER_DB_LIMIT})
            total += len(_parse_list_response(tables_res))
        out["rawTables"] = total
    except Exception:
        pass
    return out


def get_datasets_list(client: ClientAdapter, project: str, limit: int = 500) -> list:
    """
    List datasets via REST API. Returns a list of objects with .id, .name, .external_id.
    .id is always the CDF internal numeric dataset id (used for filtering); never external_id.
    """
    from types import SimpleNamespace
    path = _project_list_path(project, "datasets/list")
    res = client.post(path, {"limit": limit})
    items = _parse_list_response(res)
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        # Use only the internal numeric dataset id (CDF "id" field). Never use externalId for .id.
        raw_id = item.get("id")
        if raw_id is None:
            continue
        try:
            internal_id = int(raw_id) if isinstance(raw_id, (int, float)) else None
        except (TypeError, ValueError):
            internal_id = None
        if internal_id is None or internal_id < 1:
            continue
        out.append(SimpleNamespace(
            id=internal_id,
            name=item.get("name"),
            external_id=item.get("externalId"),
        ))
    return out


def _count_body_for_dataset(data_set_id: dict) -> dict:
    numeric_id = data_set_id.get("id") if isinstance(data_set_id.get("id"), (int, float)) else None
    if numeric_id is not None:
        ids = [int(numeric_id)]
        in_clause = {"in": {"property": ["dataSetId"], "values": ids}}
        return {"advancedFilter": {"and": [{"and": [{"or": [in_clause]}]}]}}
    return {"aggregate": "count"}


def _count_body_for_dataset_documents(data_set_id: dict) -> dict:
    numeric_id = data_set_id.get("id") if isinstance(data_set_id.get("id"), (int, float)) else None
    if numeric_id is None:
        return {"aggregate": "count"}
    in_clause = {"in": {"property": ["sourceFile", "datasetId"], "values": [int(numeric_id)]}}
    return {"filter": {"and": [{"and": [{"or": [in_clause]}]}]}, "aggregate": "count"}


def _count_body_for_dataset_files(data_set_id: dict) -> dict:
    """Files API: filter only (no aggregate key). Endpoint returns count in items[0].count."""
    numeric_id = data_set_id.get("id") if isinstance(data_set_id.get("id"), (int, float)) else None
    if numeric_id is None:
        return {}
    return {"filter": {"dataSetIds": [{"id": int(numeric_id)}]}}


def get_dataset_resource_counts(
    client: ClientAdapter,
    project: str,
    data_set_id: dict,
) -> dict:
    """Per-dataset counts for the dataset table. Files use Documents API (documents/aggregate)."""
    paths = {
        "assets": _project_path(project, "assets"),
        "timeseries": _project_path(project, "timeseries"),
        "events": _project_path(project, "events"),
        "sequences": _project_path(project, "sequences"),
        "files": _project_path(project, "documents"),
    }
    count_body = _count_body_for_dataset(data_set_id)  # advancedFilter for assets/timeseries/events/sequences
    count_body_files = _count_body_for_dataset_documents(data_set_id)  # Documents API for files column

    def _safe_dataset_count(path_key: str, body: dict) -> int:
        try:
            return _parse_count_response(client.post(paths[path_key], body))
        except Exception:
            return 0

    res = {
        "assets": _safe_dataset_count("assets", count_body),
        "timeseries": _safe_dataset_count("timeseries", count_body),
        "events": _safe_dataset_count("events", count_body),
        "sequences": _safe_dataset_count("sequences", count_body),
        "files": _safe_dataset_count("files", count_body_files),
    }
    return res


def run_asset_analysis(
    client: ClientAdapter,
    filter_key: str,
    project: str,
    data_set_ids: Optional[list[dict]] = None,
) -> dict:
    path = _project_path(project, "assets")
    results = []
    filter_part = _data_set_filter_aggregate(data_set_ids)
    try:
        uv = client.post(path, {
            "aggregate": "uniqueValues",
            "properties": [{"property": _get_property_path(filter_key, "assets")}],
            **filter_part,
        })
        items = uv.get("items") or []
        for item in items:
            parsed = _item_value(item)
            if not parsed:
                continue
            value, count = parsed
            if len(value) > CDF_FILTER_MAX_LEN:
                keys = []
            else:
                up = client.post(path, {
                    "aggregate": "uniqueProperties",
                    "path": ["metadata"],
                    **_advanced_filter_equals(["metadata", filter_key], value),
                    **filter_part,
                })
                keys = _unique_properties_keys(up.get("items") or [])
            count_str = f"Count: {count}\n" if count else ""
            label = filter_key.title()
            meta_part = f"Metadata keys: [{', '.join(keys)}]\n\n" if keys else _value_too_long_meta_part(value)
            results.append({
                "count": count,
                "text": f"{label}: {value}\n{count_str}{meta_part}",
                "filterKeyPart": f"{label}: {value}\n",
                "countPart": count_str,
                "metadataKeysPart": meta_part,
            })
        results.sort(key=lambda r: r["count"], reverse=True)
        return {"resourceType": "assets", "filterKey": filter_key, "rows": results}
    except Exception as e:
        return {"resourceType": "assets", "filterKey": filter_key, "rows": [], "error": str(e)}


def run_timeseries_analysis(
    client: ClientAdapter,
    filter_key: str,
    project: str,
    data_set_ids: Optional[list[dict]] = None,
) -> dict:
    path = _project_path(project, "timeseries")
    prop_path = _get_timeseries_property_path(filter_key)
    filter_part = _data_set_filter_aggregate(data_set_ids)
    results = []
    try:
        uv = client.post(path, {
            "aggregate": "uniqueValues",
            "properties": [{"property": prop_path}],
            **filter_part,
        })
        items = uv.get("items") or []
        for item in items:
            parsed = _item_value(item)
            if not parsed:
                continue
            value, count = parsed
            if len(value) > CDF_FILTER_MAX_LEN:
                keys = []
            else:
                filter_val = value
                if prop_path[0] in ("isStep", "isString") and value in ("true", "false"):
                    filter_val = value == "true"
                up = client.post(path, {
                    "aggregate": "uniqueProperties",
                    "path": ["metadata"],
                    **_advanced_filter_equals(prop_path, filter_val),
                    **filter_part,
                })
                keys = _unique_properties_keys(up.get("items") or [])
            count_str = f"Count: {count}\n" if count else ""
            label = filter_key.title()
            meta_part = f"Metadata keys: [{', '.join(keys)}]\n\n" if keys else _value_too_long_meta_part(value)
            results.append({
                "count": count,
                "text": f"{label}: {value}\n{count_str}{meta_part}",
                "filterKeyPart": f"{label}: {value}\n",
                "countPart": count_str,
                "metadataKeysPart": meta_part,
            })
        results.sort(key=lambda r: r["count"], reverse=True)
        return {"resourceType": "timeseries", "filterKey": filter_key, "rows": results}
    except Exception as e:
        return {"resourceType": "timeseries", "filterKey": filter_key, "rows": [], "error": str(e)}


def run_events_analysis(
    client: ClientAdapter,
    filter_key: str,
    project: str,
    data_set_ids: Optional[list[dict]] = None,
) -> dict:
    path = _project_path(project, "events")
    filter_part = _data_set_filter_aggregate(data_set_ids)
    results = []
    try:
        if filter_key == "metadata":
            all_meta = client.post(path, {"aggregate": "uniqueProperties", "path": ["metadata"], **filter_part})
            meta_fields = sorted(set(_unique_properties_keys(all_meta.get("items") or [])))
            for meta_field in meta_fields:
                try:
                    sv = client.post(path, {
                        "aggregate": "uniqueValues",
                        "properties": [{"property": ["metadata", meta_field]}],
                        **filter_part,
                    })
                    vals = sv.get("items") or []
                    sample_value = None
                    for v in vals:
                        x = v.get("value") or (v.get("values") or [None])[0]
                        if x is not None and len(str(x)) <= CDF_FILTER_MAX_LEN:
                            sample_value = str(x)
                            break
                except Exception:
                    sample_value = None
                if sample_value is None:
                    results.append({
                        "count": 0,
                        "text": f"Metadata field: {meta_field}\n (skipped - no filterable value)\n\n",
                        "filterKeyPart": f"Metadata field: {meta_field}\n",
                        "countPart": "",
                        "metadataKeysPart": " (skipped - no filterable value)\n\n",
                    })
                    continue
                try:
                    cr = client.post(path, {
                        "aggregate": "count",
                        **_advanced_filter_equals(["metadata", meta_field], sample_value),
                        **filter_part,
                    })
                    count = (cr.get("items") or [{}])[0].get("count")
                except Exception:
                    count = None
                up = client.post(path, {
                    "aggregate": "uniqueProperties",
                    "path": ["metadata"],
                    **_advanced_filter_equals(["metadata", meta_field], sample_value),
                    **filter_part,
                })
                keys = _unique_properties_keys(up.get("items") or [])
                count_str = f"Count: {count}\n" if count is not None else ""
                meta_part = f"Metadata keys: [{', '.join(keys)}]\n\n"
                results.append({
                    "count": count or 0,
                    "text": f"Metadata field: {meta_field}\n{count_str}{meta_part}",
                    "filterKeyPart": f"Metadata field: {meta_field}\n",
                    "countPart": count_str,
                    "metadataKeysPart": meta_part,
                })
            results.sort(key=lambda r: r["count"], reverse=True)
            return {"resourceType": "events", "filterKey": filter_key, "rows": results}
        event_prop = ["type"] if filter_key == "type" else ["metadata", filter_key]
        uv = client.post(path, {
            "aggregate": "uniqueValues",
            "properties": [{"property": event_prop}],
            **filter_part,
        })
        items = uv.get("items") or []
        for item in items:
            parsed = _item_value(item)
            if not parsed:
                continue
            value, count = parsed
            if len(value) > CDF_FILTER_MAX_LEN:
                keys = []
            else:
                up = client.post(path, {
                    "aggregate": "uniqueProperties",
                    "path": ["metadata"],
                    **_advanced_filter_equals(event_prop, value),
                    **filter_part,
                })
                keys = _unique_properties_keys(up.get("items") or [])
            count_str = f"Count: {count}\n" if count else ""
            label = filter_key.title()
            meta_part = f"Metadata keys: [{', '.join(keys)}]\n\n" if keys else _value_too_long_meta_part(value)
            results.append({
                "count": count,
                "text": f"{label}: {value}\n{count_str}{meta_part}",
                "filterKeyPart": f"{label}: {value}\n",
                "countPart": count_str,
                "metadataKeysPart": meta_part,
            })
        results.sort(key=lambda r: r["count"], reverse=True)
        return {"resourceType": "events", "filterKey": filter_key, "rows": results}
    except Exception as e:
        return {"resourceType": "events", "filterKey": filter_key, "rows": [], "error": str(e)}


def run_sequences_analysis(
    client: ClientAdapter,
    filter_key: str,
    project: str,
    data_set_ids: Optional[list[dict]] = None,
) -> dict:
    path = _project_path(project, "sequences")
    filter_part = _data_set_filter_aggregate(data_set_ids)
    results = []
    try:
        uv = client.post(path, {
            "aggregate": "uniqueValues",
            "properties": [{"property": ["metadata", filter_key]}],
            **filter_part,
        })
        items = uv.get("items") or []
        for item in items:
            parsed = _item_value(item)
            if not parsed:
                continue
            value, count = parsed
            if len(value) > CDF_FILTER_MAX_LEN:
                keys = []
            else:
                up = client.post(path, {
                    "aggregate": "uniqueProperties",
                    "path": ["metadata"],
                    **_advanced_filter_equals(["metadata", filter_key], value),
                    **filter_part,
                })
                keys = _unique_properties_keys(up.get("items") or [])
            count_str = f"Count: {count}\n" if count else ""
            label = filter_key.title()
            meta_part = f"Metadata keys: [{', '.join(keys)}]\n\n" if keys else _value_too_long_meta_part(value)
            results.append({
                "count": count,
                "text": f"{label}: {value}\n{count_str}{meta_part}",
                "filterKeyPart": f"{label}: {value}\n",
                "countPart": count_str,
                "metadataKeysPart": meta_part,
            })
        results.sort(key=lambda r: r["count"], reverse=True)
        return {"resourceType": "sequences", "filterKey": filter_key, "rows": results}
    except Exception as e:
        return {"resourceType": "sequences", "filterKey": filter_key, "rows": [], "error": str(e)}


def run_files_analysis(
    client: ClientAdapter,
    filter_key: str,
    project: str,
    data_set_ids: Optional[list[dict]] = None,
) -> dict:
    """Run analysis for Files using Documents API (documents/aggregate) with filter + aggregate, like Dune app."""
    path = _project_path(project, "documents")
    prop_path = _get_documents_property_path(filter_key)
    filter_part = _documents_data_set_filter(data_set_ids)
    is_labels = len(prop_path) == 1 and prop_path[0] == "labels"
    max_len = DOCUMENTS_FILTER_MAX_LEN
    results = []
    try:
        uv = client.post(path, {
            "aggregate": "uniqueValues",
            "properties": [{"property": prop_path}],
            "limit": 1000,
            **filter_part,
        })
        items = uv.get("items") or []
        for item in items:
            parsed = _item_value(item)
            if not parsed:
                continue
            value, count = parsed
            skipped_long = False
            if is_labels:
                keys = []
            elif len(value) > max_len:
                keys = []
                skipped_long = True
            else:
                eq_filter = _filter_equals(prop_path, value).get("filter") or {}
                dataset_filter = (filter_part.get("filter") or {}) if filter_part else {}
                merged = {"and": [dataset_filter, eq_filter]} if dataset_filter else eq_filter
                up = client.post(path, {
                    "aggregate": "uniqueProperties",
                    "properties": [{"property": ["sourceFile", "metadata"]}],
                    "limit": 1000,
                    "filter": merged,
                })
                keys = _unique_properties_keys_documents(up.get("items") or [])
            count_str = f"Count: {count}\n" if count else ""
            label = filter_key.title()
            if keys:
                meta_part = f"Metadata keys: [{', '.join(keys)}]\n\n"
            elif is_labels:
                meta_part = "Metadata keys: (not available for labels)\n\n"
            elif skipped_long:
                meta_part = _value_too_long_meta_part(value)
            else:
                meta_part = "Metadata keys: (none returned)\n\n"
            results.append({
                "count": count,
                "text": f"{label}: {value}\n{count_str}{meta_part}",
                "filterKeyPart": f"{label}: {value}\n",
                "countPart": count_str,
                "metadataKeysPart": meta_part,
            })
        results.sort(key=lambda r: r["count"], reverse=True)
        return {"resourceType": "files", "filterKey": filter_key, "rows": results}
    except Exception as e:
        return {"resourceType": "files", "filterKey": filter_key, "rows": [], "error": str(e)}


def run_analysis(
    client: ClientAdapter,
    resource_type: str,
    filter_key: str,
    project: str,
    data_set_ids: Optional[list[dict]] = None,
) -> dict:
    runners = {
        "assets": run_asset_analysis,
        "timeseries": run_timeseries_analysis,
        "events": run_events_analysis,
        "sequences": run_sequences_analysis,
        "files": run_files_analysis,
    }
    fn = runners.get(resource_type)
    if fn:
        out = fn(client, filter_key, project, data_set_ids)
        out = _unwrap_maybe_coro(out)
        if not isinstance(out, dict) and getattr(out, "result", None) and callable(getattr(out, "result")):
            try:
                out = out.result()
            except Exception:
                pass
        return out if isinstance(out, dict) else {"resourceType": resource_type, "filterKey": filter_key, "rows": [], "error": "Invalid response"}
    return {"resourceType": resource_type, "filterKey": filter_key, "rows": [], "error": f"Unknown resource type: {resource_type}"}


def get_metadata_keys_list(
    client: ClientAdapter,
    resource_type: str,
    project: str,
    data_set_ids: Optional[list[dict]] = None,
) -> list[dict]:
    """Files use Documents API (uniqueProperties on sourceFile.metadata). Other resources use aggregate uniqueProperties."""
    if resource_type == "files":
        filter_part = _documents_data_set_filter(data_set_ids)
        path = _project_path(project, "documents")
        res = client.post(path, {
            "aggregate": "uniqueProperties",
            "properties": [{"property": ["sourceFile", "metadata"]}],
            "limit": 1000,
            **filter_part,
        })
        res = _unwrap_maybe_coro(res)
        res = res if isinstance(res, dict) else {}
        if res.get("error"):
            raise ValueError(str(res.get("error")))
        items = res.get("items") or []
        from_agg = []
        for item in items:
            if not isinstance(item, dict):
                continue
            first = (item.get("values") or [None])[0] if item.get("values") else None
            if isinstance(first, str):
                key = first
            elif isinstance(item.get("property"), list) and item["property"]:
                key = item["property"][-1]
            elif isinstance(first, dict) and isinstance(first.get("property"), list) and first.get("property"):
                key = first["property"][-1]
            else:
                key = None
            if key:
                from_agg.append({"key": key, "count": item.get("count") or 0})
        from_agg.sort(key=lambda x: x["count"], reverse=True)
        total = get_total_count(client, project, "files", data_set_ids)
        return [
            {"key": "type", "count": total},
            {"key": "labels", "count": total},
            {"key": "author", "count": total},
            {"key": "source", "count": total},
        ] + from_agg
    filter_part = _data_set_filter_aggregate(data_set_ids)
    path = _project_path(project, resource_type)
    body = {"aggregate": "uniqueProperties", "path": ["metadata"], **filter_part}
    res = client.post(path, body)
    res = _unwrap_maybe_coro(res)
    res = res if isinstance(res, dict) else {}
    if res.get("error"):
        raise ValueError(str(res.get("error")))
    items = _parse_list_response(res)
    out = []
    for item in items:
        prop = item.get("property")
        if not prop and item.get("values") and isinstance(item["values"][0], dict):
            prop = item["values"][0].get("property")
        key = prop[-1] if isinstance(prop, list) and prop else None
        if key:
            out.append({"key": key, "count": item.get("count") or 0})
    out.sort(key=lambda x: x["count"], reverse=True)
    if resource_type == "timeseries":
        total = get_total_count(client, project, "timeseries", data_set_ids)
        return [
            {"key": "is step", "count": total},
            {"key": "is string", "count": total},
            {"key": "unit", "count": total},
        ] + out
    return out
