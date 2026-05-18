"""Discovery pipeline query handlers: dispatch and re-exports for tests and tooling."""

from __future__ import annotations

from typing import Any, Dict, MutableMapping

from .discovery_query_shared import (
    DEFAULT_RAW_DB,
    DEFAULT_RAW_TABLE,
    ENTITY_TYPE_COLUMN,
    EXTERNAL_ID_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    PROPERTIES_JSON_COLUMN,
    QUERY_SOURCE_COLUMN,
    QUERY_TASK_ID_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RUN_ID_COLUMN,
    SCOPE_KEY_COLUMN,
    VIEW_EXTERNAL_ID_COLUMN,
    VIEW_SPACE_COLUMN,
    VIEW_VERSION_COLUMN,
    build_entity_cohort_row,
    resolve_query_sink,
    new_pipeline_run_id,
    resolve_run_id,
    resolve_task_config,
    _as_dict,
    _first_nonempty,
    _flush_rows,
    _utc_run_id,
)


def discovery_handle_view_query(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    from fn_dm_view_query.engine.orchestration import discovery_handle_view_query as _impl

    return _impl(fn_external_id, data, client, log)


def discovery_handle_raw_query(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    from fn_dm_raw_query.engine.orchestration import discovery_handle_raw_query as _impl

    return _impl(fn_external_id, data, client, log)


def discovery_handle_classic_query(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    from fn_dm_classic_query.engine.orchestration import discovery_handle_classic_query as _impl

    return _impl(fn_external_id, data, client, log)


def discovery_query_handle_cdf(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    """Dispatch to the appropriate discovery query implementation."""
    if fn_external_id == "fn_dm_view_query":
        return discovery_handle_view_query(fn_external_id, data, client, log)
    if fn_external_id == "fn_dm_raw_query":
        return discovery_handle_raw_query(fn_external_id, data, client, log)
    if fn_external_id == "fn_dm_classic_query":
        return discovery_handle_classic_query(fn_external_id, data, client, log)
    raise ValueError(f"Not a discovery query function: {fn_external_id}")
