"""Local runner entry for ``fn_dm_discovery_raw_cleanup``."""

from __future__ import annotations

from typing import Any, Dict

from cdf_fn_common.discovery_handler_result import apply_handler_output

from fn_dm_discovery_raw_cleanup.handler import handle


def discovery_raw_cleanup(client: Any, logger: Any, data: Dict[str, Any], cdf_config: Any) -> None:
    del logger, cdf_config
    out = handle(data, client)
    apply_handler_output(out, data)
