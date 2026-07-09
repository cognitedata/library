"""Persist canvas preview node snapshots to stable RAW tables (excluded from cohort cleanup)."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterator, List, Mapping, MutableMapping, Optional, Tuple

try:
    from cognite.client.exceptions import CogniteNotFoundError
except ImportError:  # pragma: no cover
    CogniteNotFoundError = Exception  # type: ignore[misc, assignment]

from cdf_fn_common.etl_cohort_storage import node_cohort_table_name
from cdf_fn_common.etl_discovery_query_shared import _flush_rows, create_table_if_not_exists
from cdf_fn_common.etl_incremental_scope import (
    RECORD_KIND_COLUMN,
    RUN_ID_COLUMN,
    iter_raw_table_rows_chunked,
    raw_row_columns,
)
from cdf_fn_common.etl_raw_upload import RawRowsUploadQueue

logger = logging.getLogger(__name__)

PREVIEW_NODE_ID_COLUMN = "PREVIEW_NODE_ID"
SOURCE_CANVAS_NODE_ID_COLUMN = "SOURCE_CANVAS_NODE_ID"
DEFAULT_PREVIEW_RAW_DB = "etl_staging"
DEFAULT_PREVIEW_RAW_TABLE_KEY = "etl_preview"


def resolve_preview_sink(
    configuration: Mapping[str, Any],
    preview_config: Optional[Mapping[str, Any]] = None,
) -> Tuple[str, str]:
    """Return ``(raw_db, preview_table)`` from pipeline configuration parameters."""
    root = configuration if isinstance(configuration, dict) else {}
    cfg_root = root.get("configuration") if isinstance(root.get("configuration"), dict) else root
    params = cfg_root.get("parameters") if isinstance(cfg_root.get("parameters"), dict) else cfg_root
    if not isinstance(params, dict):
        params = {}
    node_cfg = preview_config if isinstance(preview_config, dict) else {}
    raw_db = str(params.get("raw_db") or DEFAULT_PREVIEW_RAW_DB).strip() or DEFAULT_PREVIEW_RAW_DB
    table = (
        str(node_cfg.get("preview_raw_table_key") or params.get("preview_raw_table_key") or DEFAULT_PREVIEW_RAW_TABLE_KEY).strip()
        or DEFAULT_PREVIEW_RAW_TABLE_KEY
    )
    return raw_db, table


def preview_row_key(run_id: str, preview_node_id: str, source_row_key: str) -> str:
    base = str(source_row_key or "row").strip() or "row"
    return f"preview:{run_id}:{preview_node_id}:{base}"


def _cohort_base_table(configuration: Mapping[str, Any]) -> str:
    root = configuration if isinstance(configuration, dict) else {}
    cfg_root = root.get("configuration") if isinstance(root.get("configuration"), dict) else root
    params = cfg_root.get("parameters") if isinstance(cfg_root.get("parameters"), dict) else cfg_root
    if not isinstance(params, dict):
        params = {}
    return str(params.get("raw_table_key") or params.get("raw_table") or "cohort").strip() or "cohort"


def _iter_rows_from_memory(
    shared_data: Mapping[str, Any],
    source_canvas_node_id: str,
) -> Iterator[Tuple[Dict[str, Any], Dict[str, Any], str]]:
    from cdf_fn_common.etl_common import iter_predecessor_rows_for_task

    for cols, props in iter_predecessor_rows_for_task(shared_data, source_canvas_node_id):
        row_key = str(cols.get("RAW_ROW_KEY") or cols.get("raw_row_key") or "").strip()
        yield dict(cols), dict(props), row_key


def _iter_rows_from_cohort_raw(
    client: Any,
    *,
    configuration: Mapping[str, Any],
    run_id: str,
    source_canvas_node_id: str,
    record_kind: Optional[str],
) -> Iterator[Tuple[Dict[str, Any], Dict[str, Any], str]]:
    from cdf_fn_common.etl_discovery_query_shared import (
        PROPERTIES_JSON_COLUMN,
        merge_confidence_column_into_properties,
    )

    raw_db, _preview_table = resolve_preview_sink(configuration)
    base = _cohort_base_table(configuration)
    cohort_table = node_cohort_table_name(base, run_id, source_canvas_node_id)
    for raw_row in iter_raw_table_rows_chunked(client, raw_db, cohort_table):
        cols = raw_row_columns(raw_row)
        if record_kind:
            if str(cols.get(RECORD_KIND_COLUMN) or "").strip() != record_kind:
                continue
        props: Dict[str, Any] = {}
        raw_json = cols.get(PROPERTIES_JSON_COLUMN)
        if raw_json:
            try:
                parsed = json.loads(str(raw_json))
                if isinstance(parsed, dict):
                    props = parsed
            except json.JSONDecodeError:
                props = {}
        merge_confidence_column_into_properties(cols, props)
        row_key = str(getattr(raw_row, "key", None) or cols.get("RAW_ROW_KEY") or "").strip()
        yield cols, props, row_key


def build_preview_snapshot_row(
    *,
    run_id: str,
    preview_node_id: str,
    source_canvas_node_id: str,
    source_cols: Mapping[str, Any],
    source_props: Mapping[str, Any],
    source_row_key: str,
) -> Dict[str, Any]:
    from cdf_fn_common.etl_discovery_query_shared import PROPERTIES_JSON_COLUMN

    cols = dict(source_cols)
    cols[RUN_ID_COLUMN] = run_id
    cols[PREVIEW_NODE_ID_COLUMN] = preview_node_id
    cols[SOURCE_CANVAS_NODE_ID_COLUMN] = source_canvas_node_id
    if PROPERTIES_JSON_COLUMN not in cols and source_props:
        cols[PROPERTIES_JSON_COLUMN] = json.dumps(dict(source_props), default=str, sort_keys=True)
    key = preview_row_key(run_id, preview_node_id, source_row_key or str(cols.get("RAW_ROW_KEY") or ""))
    return {"key": key, "columns": cols}


def snapshot_predecessor_to_preview(
    client: Any,
    shared_data: MutableMapping[str, Any],
    *,
    run_id: str,
    preview_node_id: str,
    source_canvas_node_id: str,
    preview_config: Optional[Mapping[str, Any]] = None,
    record_kind: Optional[str] = "entity",
    row_cap: int = 10_000,
    log: Any = None,
) -> Dict[str, Any]:
    """Copy predecessor cohort rows into the stable preview RAW table."""
    if client is None:
        raise ValueError("preview snapshot requires a CDF client")
    configuration = shared_data.get("configuration")
    if not isinstance(configuration, dict):
        configuration = {}
    raw_db, preview_table = resolve_preview_sink(configuration, preview_config)

    marker = f"{run_id}:{raw_db}/{preview_table}"
    truncated_markers = shared_data.get("_preview_tables_truncated")
    seen_markers: set[str] = (
        {str(v).strip() for v in truncated_markers if str(v).strip()}
        if isinstance(truncated_markers, (list, tuple, set))
        else set()
    )
    if marker not in seen_markers:
        try:
            client.raw.tables.delete(raw_db, preview_table)
        except CogniteNotFoundError:
            pass
        except Exception as ex:  # pragma: no cover
            raise RuntimeError(f"Failed to truncate preview RAW table {raw_db}/{preview_table}: {ex}") from ex
        create_table_if_not_exists(client, raw_db, preview_table, log)
        seen_markers.add(marker)
        shared_data["_preview_tables_truncated"] = sorted(seen_markers)

    cap = max(1, int(row_cap or 10_000))
    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    n_written = 0

    def consume(iterator: Iterator[Tuple[Dict[str, Any], Dict[str, Any], str]]) -> None:
        nonlocal n_written
        for cols, props, row_key in iterator:
            if n_written >= cap:
                break
            pending.append(
                build_preview_snapshot_row(
                    run_id=run_id,
                    preview_node_id=preview_node_id,
                    source_canvas_node_id=source_canvas_node_id,
                    source_cols=cols,
                    source_props=props,
                    source_row_key=row_key,
                )
            )
            n_written += 1
            if len(pending) >= 500:
                _flush_rows(queue, raw_db, preview_table, pending, client=client)

    memory_rows = list(_iter_rows_from_memory(shared_data, source_canvas_node_id))
    if memory_rows:

        def mem_iter():
            for item in memory_rows:
                yield item

        consume(mem_iter())
    else:
        consume(
            _iter_rows_from_cohort_raw(
                client,
                configuration=configuration,
                run_id=run_id,
                source_canvas_node_id=source_canvas_node_id,
                record_kind=record_kind,
            )
        )

    _flush_rows(queue, raw_db, preview_table, pending, client=client)
    if log and hasattr(log, "info"):
        log.info(
            "preview snapshot node=%s source=%s wrote=%s sink=%s/%s",
            preview_node_id,
            source_canvas_node_id,
            n_written,
            raw_db,
            preview_table,
        )
    return {
        "preview_node_id": preview_node_id,
        "source_canvas_node_id": source_canvas_node_id,
        "rows_written": n_written,
        "raw_db": raw_db,
        "raw_table": preview_table,
    }
