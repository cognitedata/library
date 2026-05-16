"""Classic save engine: Cognite function entry (stub)."""

from __future__ import annotations

from typing import Any, Dict

from .handlers.classic_save import ClassicSaveHandler


def discovery_handle_classic_save(data: Dict[str, Any], client: Any) -> Dict[str, Any]:
    return ClassicSaveHandler.run(data, client)
