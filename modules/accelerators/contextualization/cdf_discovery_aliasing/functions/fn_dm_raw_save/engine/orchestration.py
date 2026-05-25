"""RAW save engine: Cognite function entry."""

from __future__ import annotations

from typing import Any, Dict

from .handlers.raw_save import RawSaveHandler


def discovery_handle_raw_save(data: Dict[str, Any], client: Any) -> Dict[str, Any]:
    return RawSaveHandler.run(data, client)
