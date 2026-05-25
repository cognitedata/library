"""Local pipeline entry for fn_etl_view_query."""

from __future__ import annotations

from typing import Any, Dict, MutableMapping

from fn_etl_view_query.handler import etl_handle_view_query


def query_view(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    return etl_handle_view_query(fn_external_id, data, client, log)
