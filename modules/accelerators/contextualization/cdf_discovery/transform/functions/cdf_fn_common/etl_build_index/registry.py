"""Registry of build_index handler classes."""

from __future__ import annotations

from typing import Dict, Type

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
