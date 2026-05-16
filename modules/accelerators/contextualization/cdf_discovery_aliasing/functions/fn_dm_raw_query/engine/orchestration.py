"""RAW query engine: Cognite function entry for cohort RAW reads → RAW sink."""

from __future__ import annotations

from typing import Any, Dict, MutableMapping

from fn_dm_raw_query.engine.handlers.raw_query import RawQueryHandler


def discovery_handle_raw_query(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    return RawQueryHandler.run(fn_external_id, data, client, log)
