"""Resolve build_index handler config and dispatch collection."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Tuple, Type

from cdf_fn_common.etl_transform.handlers.base import AbstractTransformHandler

from .handlers.base import AbstractBuildIndexHandler
from .registry import BUILD_INDEX_HANDLER_IDS, DEFAULT_BUILD_INDEX_HANDLER_ID, HANDLER_BY_ID


def resolve_build_index_handler_id(cfg: Mapping[str, Any]) -> str:
    handler_id = AbstractTransformHandler.first_nonempty(cfg.get("handler_id"), cfg.get("handler"))
    if not handler_id:
        return DEFAULT_BUILD_INDEX_HANDLER_ID
    if handler_id not in BUILD_INDEX_HANDLER_IDS:
        raise ValueError(
            f"build_index handler_id must be one of {sorted(BUILD_INDEX_HANDLER_IDS)}; got {handler_id!r}"
        )
    return handler_id


def resolve_handler_block(cfg: Mapping[str, Any], handler_id: str) -> Dict[str, Any]:
    block = cfg.get(handler_id)
    if isinstance(block, dict):
        return dict(block)
    return {}


def resolve_build_index_config(cfg: Mapping[str, Any]) -> Tuple[str, Type[AbstractBuildIndexHandler], Dict[str, Any]]:
    handler_id = resolve_build_index_handler_id(cfg)
    handler_cls = HANDLER_BY_ID[handler_id]
    resolved: Dict[str, Any] = {**handler_cls.default_block(), **resolve_handler_block(cfg, handler_id)}
    top_index_kinds = cfg.get("index_kinds")
    if isinstance(top_index_kinds, dict) and top_index_kinds:
        resolved["index_kinds"] = dict(top_index_kinds)
    elif "index_kinds" not in resolved or not isinstance(resolved.get("index_kinds"), dict):
        resolved["index_kinds"] = {}
    return handler_id, handler_cls, resolved
