"""SQL query: CDF transformations preview → discovery cohort RAW rows."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Mapping, MutableMapping

from cdf_fn_common.cohort_storage import canvas_node_id_for_task, require_run_id
from cdf_fn_common.discovery_query_shared import (
    build_entity_cohort_row,
    resolve_query_sink,
    resolve_task_config,
    _flush_rows,
)
from cdf_fn_common.query_enumeration import (
    QueryEnumerationStats,
    SQL_PREVIEW_MAX_ROWS,
    enumeration_summary,
    mark_truncated,
    resolve_sql_row_limit,
)
from cdf_fn_common.raw_upload import RawRowsUploadQueue
from cdf_fn_common.sql_preview import resolve_sql_row_external_id, run_sql_preview
from cdf_fn_common.task_runtime import merge_compiled_task_into_data

from .base import AbstractDiscoveryQueryHandler


def _scope_key_from_query(query: str, configured: str) -> str:
    if configured.strip():
        return configured.strip()
    digest = hashlib.sha256(query.strip().encode("utf-8")).hexdigest()[:16]
    return f"sql:{digest}"


class SqlQueryHandler(AbstractDiscoveryQueryHandler):
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
        query = cls.first_nonempty(cfg.get("sql_query"), cfg.get("query"))
        if not query:
            raise ValueError("query_sql requires config.sql_query")

        sql_limit = resolve_sql_row_limit(cfg)
        raw_limit = cfg.get("limit")
        if raw_limit is None:
            raw_limit = cfg.get("batch_size")
        try:
            explicit_int = int(raw_limit) if raw_limit is not None else 0
        except (TypeError, ValueError):
            explicit_int = 0
        source_limit = None
        convert_to_string = bool(cfg.get("convert_to_string", True))
        timeout_raw = cfg.get("timeout")
        timeout = int(timeout_raw) if timeout_raw is not None and str(timeout_raw).strip() else None
        if timeout is not None:
            timeout = max(1, min(timeout, 240))

        entity_type = cls.first_nonempty(cfg.get("entity_type"), "sql")
        scope_key = _scope_key_from_query(query, cls.first_nonempty(cfg.get("scope_key")))
        external_id_column = cls.first_nonempty(cfg.get("external_id_column"))

        run_id = require_run_id(data)
        data["run_id"] = run_id
        task_id = cls.first_nonempty(data.get("task_id"), fn_external_id)
        canvas_node_id = canvas_node_id_for_task(data, task_id)
        raw_db, raw_table = resolve_query_sink(data)

        preview = run_sql_preview(
            client,
            query=query,
            limit=sql_limit,
            source_limit=source_limit,
            convert_to_string=convert_to_string,
            timeout=timeout,
        )
        items = preview.get("items") or []
        if not isinstance(items, list):
            items = []

        enum_stats = QueryEnumerationStats(rows_read=len(items), pages=1, list_complete=True)
        if explicit_int > 0 and len(items) >= explicit_int:
            mark_truncated(enum_stats, reason="limit")
        elif explicit_int <= 0 and len(items) >= SQL_PREVIEW_MAX_ROWS:
            mark_truncated(enum_stats, reason="sql_preview_max")

        queue = RawRowsUploadQueue(client)
        pending: List[Dict[str, Any]] = []
        n_written = 0
        skipped = 0

        for i, row in enumerate(items):
            if not isinstance(row, dict):
                skipped += 1
                continue
            props = dict(row)
            ext_id = resolve_sql_row_external_id(props, external_id_column)
            if not ext_id:
                ext_id = f"row_{i}"
            nid = ext_id
            pending.append(
                build_entity_cohort_row(
                    run_id=run_id,
                    scope_key=scope_key,
                    canvas_node_id=canvas_node_id,
                    query_source="sql",
                    node_instance_id=nid,
                    external_id=ext_id,
                    entity_type=entity_type,
                    view_space="",
                    view_external_id="sql",
                    view_version="",
                    properties=props,
                )
            )
            n_written += 1
            if len(pending) >= 500:
                _flush_rows(queue, raw_db, raw_table, pending, client=client)

        _flush_rows(queue, raw_db, raw_table, pending, client=client)

        if log and hasattr(log, "info"):
            log.info(
                "%s sql_query rows=%s written=%s skipped=%s scope_key=%s",
                fn_external_id,
                len(items),
                n_written,
                skipped,
                scope_key,
            )

        enum_stats.rows_written = n_written
        summary = enumeration_summary(
            enum_stats,
            extra={
                "function_external_id": fn_external_id,
                "task_id": task_id,
                "query_source": "sql",
                "instances_written": n_written,
                "rows_skipped": skipped,
                "run_id": run_id,
                "scope_key": scope_key,
                "raw_db": raw_db,
                "raw_table": raw_table,
                "row_count": preview.get("row_count"),
                "sql_limit": sql_limit,
            },
        )
        data["run_id"] = run_id
        return summary
