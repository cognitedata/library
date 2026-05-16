"""Classic query engine: Cognite function entry for classic API list → RAW sink."""

from __future__ import annotations

from typing import Any, Dict, MutableMapping

from fn_dm_classic_query.engine.handlers.classic_query import ClassicQueryHandler


def discovery_handle_classic_query(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    return ClassicQueryHandler.run(fn_external_id, data, client, log)
