"""Classic resources query: assets/files/events/time series to cohort."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, MutableMapping

from cdf_fn_common.discovery_query_shared import (
    build_entity_cohort_row,
    resolve_query_sink,
    resolve_run_id,
    resolve_task_config,
    _flush_rows,
)
from cdf_fn_common.raw_upload import RawRowsUploadQueue
from cdf_fn_common.task_runtime import merge_compiled_task_into_data

from .base import AbstractDiscoveryQueryHandler


def _classic_list(
    client: Any,
    resource_type: str,
    *,
    limit: int,
) -> Iterable[Any]:
    rt = resource_type.strip().lower()
    lim = max(1, min(limit or 1000, 1000))
    if rt in ("asset", "assets"):
        return client.assets.list(limit=lim)
    if rt in ("file", "files"):
        return client.files.list(limit=lim)
    if rt in ("event", "events"):
        return client.events.list(limit=lim)
    if rt in ("timeseries", "time_series", "time-series"):
        return client.time_series.list(limit=lim)
    raise ValueError(f"Unsupported classic resource_type: {resource_type!r}")


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


class ClassicQueryHandler(AbstractDiscoveryQueryHandler):
    @classmethod
    def run(
        cls,
        fn_external_id: str,
        data: MutableMapping[str, Any],
        client: Any,
        log: Any,
    ) -> Dict[str, Any]:
        merge_compiled_task_into_data(data)
        cfg = resolve_task_config(data)
        resource_type = cls.first_nonempty(cfg.get("resource_type"), cfg.get("classic_resource_type"), "assets")
        limit = int(cfg.get("limit") or cfg.get("batch_size") or 1000)
        entity_type = cls.first_nonempty(cfg.get("entity_type"), resource_type.rstrip("s"))
        scope_key = cls.first_nonempty(cfg.get("scope_key"), f"classic:{resource_type.lower()}")
        run_id = resolve_run_id(data)
        task_id = cls.first_nonempty(data.get("task_id"), fn_external_id)
        raw_db, raw_table = resolve_query_sink(data)

        queue = RawRowsUploadQueue(client)
        pending: List[Dict[str, Any]] = []
        n_written = 0

        for item in _classic_list(client, resource_type, limit=limit):
            ext_id = _classic_external_id(item)
            if not ext_id:
                continue
            props = _classic_dump(item)
            nid = ext_id
            pending.append(
                build_entity_cohort_row(
                    run_id=run_id,
                    scope_key=scope_key,
                    task_id=task_id,
                    query_source="classic",
                    node_instance_id=nid,
                    external_id=ext_id,
                    entity_type=entity_type,
                    view_space="",
                    view_external_id=resource_type,
                    view_version="",
                    properties=props,
                )
            )
            n_written += 1
            if len(pending) >= 500:
                _flush_rows(queue, raw_db, raw_table, pending)

        _flush_rows(queue, raw_db, raw_table, pending)

        if log and hasattr(log, "info"):
            log.info("%s listed %s classic %s resource(s)", fn_external_id, n_written, resource_type)

        summary = {
            "function_external_id": fn_external_id,
            "task_id": task_id,
            "query_source": "classic",
            "instances_written": n_written,
            "run_id": run_id,
            "scope_key": scope_key,
            "resource_type": resource_type,
            "raw_db": raw_db,
            "raw_table": raw_table,
        }
        data["run_id"] = run_id
        return summary
