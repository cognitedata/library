"""Local runner entry for ``fn_dm_classic_query`` (delegates to handler)."""

from __future__ import annotations

from typing import Any, Dict

from cdf_fn_common.discovery_handler_result import apply_handler_output

from fn_dm_classic_query.handler import handle


def query_classic(client: Any, logger: Any, data: Dict[str, Any], cdf_config: Any) -> None:
    del logger, cdf_config
    handle(data, client)
