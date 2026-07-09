"""Registry of v1 transform handler classes."""

from __future__ import annotations

from typing import Any, Dict, List, Type

from .handlers.base import AbstractTransformHandler
from .handlers.change_case import ChangeCaseHandler
from .handlers.coerce_scalar import CoerceScalarHandler
from .handlers.default_if_empty import DefaultIfEmptyHandler
from .handlers.format_datetime import FormatDatetimeHandler
from .handlers.hash_stable import HashStableHandler
from .handlers.heuristic_sampler import HeuristicSamplerHandler
from .handlers.leading_zero_normalize import LeadingZeroNormalizeHandler
from .handlers.mask_string import MaskStringHandler
from .handlers.parse_json_extract import ParseJsonExtractHandler
from .handlers.regex_substitution import RegexSubstitutionHandler
from .handlers.sequential_literal_replace import SequentialLiteralReplaceHandler
from .handlers.split_join import SplitJoinHandler
from .handlers.split_string import SplitStringHandler
from .handlers.static_lookup_map import StaticLookupMapHandler
from .handlers.substitution_variants import SubstitutionVariantsHandler
from .handlers.trim_whitespace import TrimWhitespaceHandler

_HANDLERS: tuple[Type[AbstractTransformHandler], ...] = (
    RegexSubstitutionHandler,
    LeadingZeroNormalizeHandler,
    SequentialLiteralReplaceHandler,
    SubstitutionVariantsHandler,
    TrimWhitespaceHandler,
    ChangeCaseHandler,
    CoerceScalarHandler,
    DefaultIfEmptyHandler,
    SplitStringHandler,
    SplitJoinHandler,
    ParseJsonExtractHandler,
    FormatDatetimeHandler,
    HashStableHandler,
    MaskStringHandler,
    StaticLookupMapHandler,
    HeuristicSamplerHandler,
)

HANDLER_BY_ID: Dict[str, Type[AbstractTransformHandler]] = {cls.handler_id: cls for cls in _HANDLERS}


def transform_handler_catalog() -> List[Dict[str, Any]]:
    """Return ``handler_id`` and ``description`` for each registered transform handler."""
    out: List[Dict[str, Any]] = []
    for handler_id in sorted(HANDLER_BY_ID):
        cls = HANDLER_BY_ID[handler_id]
        out.append(
            {
                "handler_id": handler_id,
                "description": str(getattr(cls, "description", "") or "").strip(),
                "multi_value": bool(getattr(cls, "multi_value", False)),
            }
        )
    return out
