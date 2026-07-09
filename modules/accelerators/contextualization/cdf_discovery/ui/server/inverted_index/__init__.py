"""Inverted index API routers."""

from __future__ import annotations

from ui.server.inverted_index.config_api import router as config_router
from ui.server.inverted_index.dashboard_api import router as dashboard_router
from ui.server.inverted_index.index_api import router as index_router

__all__ = ["config_router", "dashboard_router", "index_router"]
