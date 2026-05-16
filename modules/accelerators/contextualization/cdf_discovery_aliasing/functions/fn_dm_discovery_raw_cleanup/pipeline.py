"""Local runner entry for ``fn_dm_discovery_raw_cleanup``."""

from __future__ import annotations

from typing import Any, Dict

from fn_dm_discovery_raw_cleanup.handler import handle


def discovery_raw_cleanup(client: Any, logger: Any, data: Dict[str, Any], cdf_config: Any) -> None:
    del logger, cdf_config
    out = handle(data, client)
    if isinstance(out, dict):
        if "status" in out:
            data["status"] = out["status"]
        if "message" in out:
            data["message"] = out["message"]
