"""Registry of build_index handler classes."""

from __future__ import annotations

from typing import Any, Dict, List, Type

from .handlers.base import AbstractBuildIndexHandler
from .handlers.annotation_vertex_index import AnnotationVertexIndexHandler
from .handlers.property_token_index import PropertyTokenIndexHandler

_HANDLERS: tuple[Type[AbstractBuildIndexHandler], ...] = (
    PropertyTokenIndexHandler,
    AnnotationVertexIndexHandler,
)

HANDLER_BY_ID: Dict[str, Type[AbstractBuildIndexHandler]] = {cls.handler_id: cls for cls in _HANDLERS}

DEFAULT_BUILD_INDEX_HANDLER_ID = PropertyTokenIndexHandler.handler_id

BUILD_INDEX_HANDLER_IDS = frozenset(HANDLER_BY_ID)


def build_index_handler_catalog() -> List[Dict[str, Any]]:
    """Return ``handler_id`` and ``description`` for each registered build_index handler."""
    out: List[Dict[str, Any]] = []
    for handler_id in sorted(HANDLER_BY_ID):
        cls = HANDLER_BY_ID[handler_id]
        out.append(
            {
                "handler_id": handler_id,
                "description": str(getattr(cls, "description", "") or "").strip(),
            }
        )
    return out
