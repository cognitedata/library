"""CDF handler: scheduled watermark incremental metadata + annotation index builds."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.fn_runtime import require_client, resolve_handler_payload  # noqa: E402
from inverted_index.index_build_watermark import run_watermark_incremental_build  # noqa: E402


def handle(data: dict[str, Any] | None = None, client: Any = None) -> dict[str, Any]:
    resolved = resolve_handler_payload(data)
    payload = resolved["payload"]
    overrides = resolved["overrides"]
    client = require_client(client)
    return run_watermark_incremental_build(
        client,
        dry_run=overrides["dry_run"],
        runtime_config=resolved["runtime"],
        force_full_lookback=bool(payload.get("force_full_lookback", False)),
    )
