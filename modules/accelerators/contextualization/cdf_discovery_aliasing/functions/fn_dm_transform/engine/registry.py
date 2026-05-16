"""Registry of v1 transform handler classes."""

from __future__ import annotations

from typing import Dict, Type

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
    ParseJsonExtractHandler,
    FormatDatetimeHandler,
    HashStableHandler,
    MaskStringHandler,
    StaticLookupMapHandler,
    HeuristicSamplerHandler,
)

HANDLER_BY_ID: Dict[str, Type[AbstractTransformHandler]] = {cls.handler_id: cls for cls in _HANDLERS}
