"""CDF handler: virtual CogniteAsset tag creation from inverted index (UC4)."""

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
from inverted_index.virtual_tags import run_virtual_tag_creation  # noqa: E402


def handle(data: dict[str, Any] | None = None, client: Any = None) -> dict[str, Any]:
    resolved = resolve_handler_payload(data)
    payload = resolved["payload"]
    overrides = resolved["overrides"]
    client = require_client(client)
    dry_run = overrides["dry_run"]
    vtc = dict(overrides.get("virtual_tag_creation_config") or {})
    if payload.get("enabled") is not None:
        vtc["enabled"] = bool(payload["enabled"])
    elif not vtc.get("enabled"):
        vtc["enabled"] = True

    scope_keys = payload.get("match_scope_keys")
    if payload.get("match_scope_key"):
        scope_keys = [str(payload["match_scope_key"])]
    if isinstance(scope_keys, str):
        scope_keys = [scope_keys]

    storage_adapter = storage_adapter_for(
        client,
        overrides["storage_config"],
        dry_run=dry_run,
    )
    try:
        return run_virtual_tag_creation(
            client,
            virtual_tag_config=vtc,
            scope_config=overrides["scope_config"],
            storage_config=overrides["storage_config"],
            storage_adapter=storage_adapter,
            all_scopes=bool(payload.get("all_scopes", False)),
            match_scope_keys=list(scope_keys) if scope_keys else None,
            dry_run=dry_run,
            limit=int(payload.get("limit", 0)),
            term_selection_mode=payload.get("term_selection_mode"),
            progress_interval=int(payload.get("progress_interval", 1000)),
        )
    except ValueError as exc:
        return {"error": str(exc)}
