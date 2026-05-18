"""Local runner entry for ``fn_dm_filter``."""

from __future__ import annotations

from typing import Any, Dict

from fn_dm_filter.handler import handle


def instance_filter(client: Any, logger: Any, data: Dict[str, Any], cdf_config: Any) -> None:
    del logger, cdf_config
    handle(data, client)
