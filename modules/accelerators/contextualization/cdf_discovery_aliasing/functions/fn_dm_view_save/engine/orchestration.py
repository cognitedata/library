"""View save engine: Cognite function entry (stub)."""

from __future__ import annotations

from typing import Any, Dict

from .handlers.view_save import ViewSaveHandler


def discovery_handle_view_save(data: Dict[str, Any], client: Any) -> Dict[str, Any]:
    return ViewSaveHandler.run(data, client)
