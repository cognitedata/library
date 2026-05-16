"""View query engine: Cognite function entry for cohort DM list → RAW sink."""

from __future__ import annotations

from typing import Any, Dict, MutableMapping

from fn_dm_view_query.engine.handlers.view_query import ViewQueryHandler


def discovery_handle_view_query(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    return ViewQueryHandler.run(fn_external_id, data, client, log)
