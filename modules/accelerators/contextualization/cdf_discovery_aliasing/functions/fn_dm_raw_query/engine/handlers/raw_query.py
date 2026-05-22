"""RAW query: copy predecessor cohort rows."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, MutableMapping

from cdf_fn_common.cohort_storage import canvas_node_id_for_task, require_run_id
from cdf_fn_common.discovery_query_shared import (
    ENTITY_TYPE_COLUMN,
    EXTERNAL_ID_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    PROPERTIES_JSON_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RUN_ID_COLUMN,
    SCOPE_KEY_COLUMN,
    VIEW_EXTERNAL_ID_COLUMN,
    VIEW_SPACE_COLUMN,
    VIEW_VERSION_COLUMN,
    build_entity_cohort_row,
    merge_confidence_column_into_properties,
    resolve_query_sink,
    resolve_task_config,
    _flush_rows,
)
from cdf_fn_common.incremental_scope import iter_raw_table_rows_chunked
from cdf_fn_common.query_enumeration import (
    QueryEnumerationStats,
    enumeration_summary,
    mark_truncated,
    resolve_read_limit,
)
from cdf_fn_common.raw_upload import RawRowsUploadQueue
from cdf_fn_common.task_runtime import merge_compiled_task_into_data

from .base import AbstractDiscoveryQueryHandler


class RawQueryHandler(AbstractDiscoveryQueryHandler):
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
        source_db = cls.first_nonempty(
            cfg.get("source_raw_db"),
            cfg.get("raw_db"),
            resolve_query_sink(data)[0],
        )
        source_table = cls.first_nonempty(
            cfg.get("source_raw_table"),
            cfg.get("source_raw_table_key"),
            cfg.get("raw_table"),
            cfg.get("raw_table_key"),
        )
        if not source_db or not source_table:
            raise ValueError("config.source_raw_db and source_raw_table (or raw_db/raw_table) are required")

        run_id = require_run_id(data)
        data["run_id"] = run_id
        task_id = cls.first_nonempty(data.get("task_id"), fn_external_id)
        canvas_node_id = canvas_node_id_for_task(data, task_id)
        sink_db, sink_table = resolve_query_sink(data)
        wanted_run = cls.first_nonempty(cfg.get("source_run_id"))
        read_limit = resolve_read_limit(cfg)

        queue = RawRowsUploadQueue(client)
        pending: List[Dict[str, Any]] = []
        n_read = 0
        n_written = 0
        enum_stats = QueryEnumerationStats()

        for row in iter_raw_table_rows_chunked(client, source_db, source_table):
            cols = dict(getattr(row, "columns", None) or {})
            if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
                continue
            if wanted_run and str(cols.get(RUN_ID_COLUMN) or "") != wanted_run:
                continue
            n_read += 1
            if read_limit > 0 and n_read > read_limit:
                mark_truncated(enum_stats, reason="read_limit")
                if log and hasattr(log, "warning"):
                    log.warning(
                        "%s RAW query truncated at read_limit=%s",
                        fn_external_id,
                        read_limit,
                    )
                break

            nid = cls.first_nonempty(cols.get(NODE_INSTANCE_ID_COLUMN), getattr(row, "key", None))
            ext_id = cls.first_nonempty(cols.get(EXTERNAL_ID_COLUMN))
            scope_key = cls.first_nonempty(cols.get(SCOPE_KEY_COLUMN), "raw_source")
            props_raw = cols.get(PROPERTIES_JSON_COLUMN)
            props: Dict[str, Any]
            if isinstance(props_raw, str) and props_raw.strip():
                try:
                    props = json.loads(props_raw)
                except json.JSONDecodeError:
                    props = {"raw_columns": cols}
            else:
                props = {"raw_columns": cols}

            merge_confidence_column_into_properties(cols, props)

            pending.append(
                build_entity_cohort_row(
                    run_id=run_id,
                    scope_key=scope_key,
                    canvas_node_id=canvas_node_id,
                    query_source="raw",
                    node_instance_id=str(nid or ext_id or n_read),
                    external_id=ext_id or str(nid or n_read),
                    entity_type=cls.first_nonempty(cols.get(ENTITY_TYPE_COLUMN), "raw"),
                    view_space=cls.first_nonempty(cols.get(VIEW_SPACE_COLUMN)),
                    view_external_id=cls.first_nonempty(cols.get(VIEW_EXTERNAL_ID_COLUMN), source_table),
                    view_version=cls.first_nonempty(cols.get(VIEW_VERSION_COLUMN)),
                    properties=props,
                )
            )
            n_written += 1
            if len(pending) >= 500:
                _flush_rows(queue, sink_db, sink_table, pending, client=client)

        _flush_rows(queue, sink_db, sink_table, pending, client=client)

        if log and hasattr(log, "info"):
            log.info(
                "%s copied %s row(s) from %s/%s to %s/%s",
                fn_external_id,
                n_written,
                source_db,
                source_table,
                sink_db,
                sink_table,
            )

        enum_stats.rows_read = n_read
        enum_stats.rows_written = n_written
        enum_stats.list_complete = not enum_stats.rows_truncated
        summary = enumeration_summary(
            enum_stats,
            extra={
                "function_external_id": fn_external_id,
                "task_id": task_id,
                "query_source": "raw",
                "instances_written": n_written,
                "run_id": run_id,
                "source_raw_db": source_db,
                "source_raw_table": source_table,
                "raw_db": sink_db,
                "raw_table": sink_table,
                "read_limit": read_limit,
            },
        )
        data["run_id"] = run_id
        return summary
