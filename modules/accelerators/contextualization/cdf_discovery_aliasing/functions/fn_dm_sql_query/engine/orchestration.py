"""SQL query engine: Cognite function entry for transformations preview → RAW sink."""

from __future__ import annotations

from typing import Any, Dict, MutableMapping

from fn_dm_sql_query.engine.handlers.sql_query import SqlQueryHandler


def discovery_handle_sql_query(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    return SqlQueryHandler.run(fn_external_id, data, client, log)
