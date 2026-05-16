"""Local runner entry for ``fn_dm_validate`` (delegates to handler)."""

from __future__ import annotations

from typing import Any, Dict

from fn_dm_validate.handler import handle


def validate(client: Any, logger: Any, data: Dict[str, Any], cdf_config: Any) -> None:
    del logger, cdf_config
    handle(data, client)
