"""CDF handler: aliases subscription → target-driven contextualization."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.fn_runtime import require_client, resolve_handler_payload  # noqa: E402
from inverted_index.subscription import handle_aliases_subscription_payload  # noqa: E402


def handle(data: dict[str, Any] | None = None, client: Any = None) -> dict[str, Any]:
    resolved = resolve_handler_payload(data)
    payload = resolved["payload"]
    overrides = resolved["overrides"]
    client = require_client(client)
    return handle_aliases_subscription_payload(
        client,
        payload,
        dry_run=overrides["dry_run"],
        runtime_config=resolved["runtime"],
        force=bool(payload.get("force", False)),
    )
