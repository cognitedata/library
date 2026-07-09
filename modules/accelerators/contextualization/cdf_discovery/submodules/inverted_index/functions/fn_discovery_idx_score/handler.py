"""CDF handler: contextualization score for a file."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.fn_runtime import (  # noqa: E402
    require_client,
    resolve_handler_payload,
    storage_adapter_for,
)
from inverted_index.scoring import calculate_contextualization_score  # noqa: E402


def handle(data: dict[str, Any] | None = None, client: Any = None) -> dict[str, Any]:
    resolved = resolve_handler_payload(data)
    payload = resolved["payload"]
    overrides = resolved["overrides"]
    client = require_client(client)
    file_external_id = str(
        payload.get("file_external_id") or payload.get("file_id") or ""
    ).strip()
    if not file_external_id:
        return {"error": "file_external_id is required"}
    return calculate_contextualization_score(
        client,
        file_external_id=file_external_id,
        file_space=str(payload.get("file_space", "cdf_cdm")),
        match_scope_key=payload.get("match_scope_key"),
        storage_adapter=storage_adapter_for(client, overrides["storage_config"]),
    )
