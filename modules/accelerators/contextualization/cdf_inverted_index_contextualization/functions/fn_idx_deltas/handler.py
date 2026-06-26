"""CDF handler: detection mode deltas for a file."""

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
from inverted_index.scoring import (  # noqa: E402
    get_pattern_not_in_standard_delta,
    get_standard_not_in_pattern_delta,
)


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
    file_space = str(payload.get("file_space", "cdf_cdm"))
    match_scope_key = payload.get("match_scope_key")
    adapter = storage_adapter_for(client, overrides["storage_config"])
    delta_mode = str(payload.get("delta_mode", "both"))
    result: dict[str, Any] = {}
    if delta_mode in ("both", "pattern_not_standard"):
        result["missing_tags"] = get_pattern_not_in_standard_delta(
            client,
            file_external_id,
            file_space=file_space,
            match_scope_key=match_scope_key,
            storage_adapter=adapter,
        )
    if delta_mode in ("both", "standard_not_pattern"):
        result["pattern_feedback"] = get_standard_not_in_pattern_delta(
            client,
            file_external_id,
            file_space=file_space,
            storage_adapter=adapter,
        )
    return result
