"""CDF handler: build metadata inverted index."""

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
from inverted_index.build import build_metadata_index  # noqa: E402


def handle(data: dict[str, Any] | None = None, client: Any = None) -> dict[str, Any]:
    resolved = resolve_handler_payload(data)
    overrides = resolved["overrides"]
    client = require_client(client)
    adapter = storage_adapter_for(
        client,
        overrides["storage_config"],
        dry_run=overrides["dry_run"],
    )
    return build_metadata_index(
        client,
        index_field_config=overrides["index_field_config"],
        scope_config=overrides["scope_config"],
        storage_config=overrides["storage_config"],
        filter_updated_after=overrides["filter_updated_after"],
        dry_run=overrides["dry_run"],
        storage_adapter=adapter,
        virtual_tag_creation_config=overrides.get("virtual_tag_creation_config"),
    )
