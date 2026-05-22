"""Discovery query previews for the operator UI (view / RAW / classic)."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping

from cognite.client.data_classes.data_modeling.ids import ViewId

from .discovery_query_shared import _first_nonempty
from .incremental_scope import (
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RUN_ID_COLUMN,
    iter_raw_table_rows_chunked,
    list_all_instances,
    node_instance_id_str,
    node_last_updated_time_ms,
    raw_row_columns,
)
from .query_enumeration import list_all_classic_resources, resolve_page_size
from .source_view_filter_build import build_source_view_query_filter
from .sql_preview import _truncate

_PREVIEW_ROW_CAP = 1000


def _preview_grid(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    columns: List[str] = []
    if items:
        columns = list(items[0].keys())
    return {
        "columns": columns,
        "items": items,
        "row_count": len(items),
    }


def _flatten_mapping(prefix: str, value: Mapping[str, Any], out: Dict[str, Any]) -> None:
    for k, v in value.items():
        key = f"{prefix}.{k}" if prefix else str(k)
        if isinstance(v, Mapping):
            _flatten_mapping(key, v, out)
        else:
            out[key] = _truncate(v)


def run_view_query_preview(
    client: Any,
    config: Mapping[str, Any],
    *,
    limit: int = 100,
) -> Dict[str, Any]:
    """List DM view instances (no cohort RAW write)."""
    view_space = _first_nonempty(config.get("view_space"), "cdf_cdm")
    view_external_id = _first_nonempty(config.get("view_external_id"))
    view_version = _first_nonempty(config.get("view_version"), "v1")
    if not view_external_id:
        raise ValueError("view_external_id is required")

    include_properties = config.get("include_properties") or []
    if not isinstance(include_properties, list):
        include_properties = []

    lim = max(1, min(int(limit or 100), _PREVIEW_ROW_CAP))
    view_id = ViewId(space=view_space, external_id=view_external_id, version=view_version)
    scope_view = {
        "view_space": view_space,
        "view_external_id": view_external_id,
        "view_version": view_version,
        "instance_space": config.get("instance_space"),
        "filters": config.get("filters") or [],
    }
    base_filter = build_source_view_query_filter(view_id, scope_view.get("filters") or [])

    instance_space = _first_nonempty(config.get("instance_space"))
    _ins = str(instance_space or "").strip()
    list_space_arg = None if (not _ins or _ins.lower() == "all_spaces") else _ins

    items: List[Dict[str, Any]] = []
    for inst in list_all_instances(
        client,
        instance_type="node",
        space=list_space_arg,
        sources=[view_id],
        filter=base_filter,
        limit_per_page=resolve_page_size(config, default=min(1000, lim)),
    ):
        ext_id = _first_nonempty(getattr(inst, "external_id", None))
        if not ext_id:
            continue
        nid = node_instance_id_str(inst)
        dumped = inst.dump() if hasattr(inst, "dump") else {}
        props: Dict[str, Any] = {}
        if isinstance(dumped, dict):
            view_props = (
                dumped.get("properties", {})
                .get(view_space, {})
                .get(f"{view_external_id}/{view_version}", {})
                or {}
            )
            if isinstance(view_props, dict):
                props = dict(view_props)
        if include_properties:
            props = {
                str(name).strip(): props[str(name).strip()]
                for name in include_properties
                if str(name).strip() in props
            }
        row: Dict[str, Any] = {
            "external_id": ext_id,
            "node_instance_id": nid,
            "space": getattr(inst, "space", None) or "",
            "last_updated_time_ms": node_last_updated_time_ms(inst),
        }
        _flatten_mapping("property", props, row)
        items.append(_truncate(row))
        if len(items) >= lim:
            break

    return _preview_grid(items)


def _classic_external_id(item: Any) -> str:
    for attr in ("external_id", "id"):
        val = getattr(item, attr, None)
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def _classic_dump(item: Any) -> Dict[str, Any]:
    if hasattr(item, "dump"):
        d = item.dump()
        return dict(d) if isinstance(d, dict) else {"value": d}
    if hasattr(item, "as_write_dict"):
        d = item.as_write_dict()
        return dict(d) if isinstance(d, dict) else {"value": d}
    return {"repr": repr(item)}


def run_classic_query_preview(
    client: Any,
    config: Mapping[str, Any],
    *,
    limit: int = 100,
) -> Dict[str, Any]:
    """List classic resources (assets/files/events/time series)."""
    resource_type = _first_nonempty(config.get("resource_type"), config.get("classic_resource_type"), "assets")
    preview_lim = max(1, min(int(limit or 100), _PREVIEW_ROW_CAP))

    items: List[Dict[str, Any]] = []
    for item in list_all_classic_resources(client, resource_type, limit=preview_lim):
        ext_id = _classic_external_id(item)
        if not ext_id:
            continue
        row: Dict[str, Any] = {"external_id": ext_id, "resource_type": resource_type}
        dumped = _classic_dump(item)
        _flatten_mapping("field", dumped, row)
        items.append(_truncate(row))
        if len(items) >= preview_lim:
            break

    return _preview_grid(items)


def run_raw_query_preview(
    client: Any,
    config: Mapping[str, Any],
    *,
    limit: int = 100,
) -> Dict[str, Any]:
    """Read entity rows from a RAW table (no cohort write)."""
    source_db = _first_nonempty(
        config.get("source_raw_db"),
        config.get("raw_db"),
    )
    source_table = _first_nonempty(
        config.get("source_raw_table"),
        config.get("source_raw_table_key"),
        config.get("raw_table"),
        config.get("raw_table_key"),
    )
    if not source_db or not source_table:
        raise ValueError("raw_db and raw_table_key (or source_raw_*) are required")

    read_limit = int(config.get("read_limit") or config.get("limit") or limit or 100)
    read_limit = max(1, min(read_limit, _PREVIEW_ROW_CAP))
    wanted_run = _first_nonempty(config.get("source_run_id"))

    items: List[Dict[str, Any]] = []
    n_read = 0
    for row in iter_raw_table_rows_chunked(client, source_db, source_table):
        cols = raw_row_columns(row)
        if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
            continue
        if wanted_run and str(cols.get(RUN_ID_COLUMN) or "") != wanted_run:
            continue
        n_read += 1
        if n_read > read_limit:
            break
        flat: Dict[str, Any] = {"raw_key": getattr(row, "key", None) or ""}
        for k, v in cols.items():
            if isinstance(v, (dict, list)):
                flat[k] = json.dumps(v, default=str)[:_PREVIEW_ROW_CAP]
            else:
                flat[k] = v
        items.append(_truncate(flat))

    return _preview_grid(items)
