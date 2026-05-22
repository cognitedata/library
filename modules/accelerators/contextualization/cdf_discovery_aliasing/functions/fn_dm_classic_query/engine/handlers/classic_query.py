"""Classic resources query: assets/files/events/time series to cohort."""

from __future__ import annotations

from typing import Any, Dict, List, MutableMapping

from cdf_fn_common.cohort_storage import canvas_node_id_for_task, require_run_id
from cdf_fn_common.discovery_query_shared import (
    build_entity_cohort_row,
    resolve_query_sink,
    resolve_task_config,
    _flush_rows,
)
from cdf_fn_common.query_enumeration import (
    QueryEnumerationStats,
    enumeration_summary,
    list_all_classic_resources,
    mark_truncated,
    resolve_classic_list_limit,
    resolve_read_limit,
)
from cdf_fn_common.raw_upload import RawRowsUploadQueue
from cdf_fn_common.task_runtime import merge_compiled_task_into_data

from .base import AbstractDiscoveryQueryHandler


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
        list_limit = resolve_classic_list_limit(cfg)
        read_cap = resolve_read_limit(cfg)
        entity_type = cls.first_nonempty(cfg.get("entity_type"), resource_type.rstrip("s"))
        scope_key = cls.first_nonempty(cfg.get("scope_key"), f"classic:{resource_type.lower()}")
        run_id = require_run_id(data)
        data["run_id"] = run_id
        task_id = cls.first_nonempty(data.get("task_id"), fn_external_id)
        canvas_node_id = canvas_node_id_for_task(data, task_id)
        raw_db, raw_table = resolve_query_sink(data)

        queue = RawRowsUploadQueue(client)
        pending: List[Dict[str, Any]] = []
        n_written = 0
        enum_stats = QueryEnumerationStats()

        for item in list_all_classic_resources(client, resource_type, limit=list_limit):
            enum_stats.rows_read += 1
            ext_id = _classic_external_id(item)
            if not ext_id:
                continue
            props = _classic_dump(item)
            pending.append(
                build_entity_cohort_row(
                    run_id=run_id,
                    scope_key=scope_key,
                    canvas_node_id=canvas_node_id,
                    query_source="classic",
                    node_instance_id=ext_id,
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
                _flush_rows(queue, raw_db, raw_table, pending, client=client)

        _flush_rows(queue, raw_db, raw_table, pending, client=client)
        enum_stats.rows_written = n_written
        enum_stats.pages = 1
        enum_stats.list_complete = True
        if read_cap > 0 and enum_stats.rows_read >= read_cap:
            mark_truncated(enum_stats, reason="read_limit")
            if log and hasattr(log, "warning"):
                log.warning(
                    "%s classic query may be truncated at read_limit=%s (rows_read=%s)",
                    fn_external_id,
                    read_cap,
                    enum_stats.rows_read,
                )

        if log and hasattr(log, "info"):
            log.info(
                "%s listed %s classic %s resource(s)",
                fn_external_id,
                n_written,
                resource_type,
            )

        summary = enumeration_summary(
            enum_stats,
            extra={
                "function_external_id": fn_external_id,
                "task_id": task_id,
                "query_source": "classic",
                "instances_written": n_written,
                "run_id": run_id,
                "scope_key": scope_key,
                "resource_type": resource_type,
                "raw_db": raw_db,
                "raw_table": raw_table,
                "classic_list_limit": list_limit,
            },
        )
        data["run_id"] = run_id
        return summary
