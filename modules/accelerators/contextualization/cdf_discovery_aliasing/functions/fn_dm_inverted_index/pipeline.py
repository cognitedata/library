"""Local runner entry for ``fn_dm_inverted_index`` (delegates to handler)."""

from __future__ import annotations

from typing import Any, Dict

from fn_dm_inverted_index.handler import handle


def inverted_index(client: Any, logger: Any, data: Dict[str, Any], cdf_config: Any) -> None:
    del logger, cdf_config
    handle(data, client)
