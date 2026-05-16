"""V1 transform handler registry — re-exports for Cognite functions and tests."""

from __future__ import annotations

from typing import Any, List, Mapping

from .handlers.base import AbstractTransformHandler, TransformResult, TransformScalar
from .handlers.change_case import ChangeCaseHandler
from .handlers.coerce_scalar import CoerceScalarHandler
from .constants import (
    ASSET_TAG_FROM_NAME_REGEX,
    CORE_TRANSFORM_HANDLERS,
    ELT_TRANSFORM_HANDLERS,
    TRANSFORM_HANDLERS,
    V1_TRANSFORM_HANDLERS,
)
from .handlers.default_if_empty import DefaultIfEmptyHandler
from .field_template import apply_output_template, extract_field_values
from .handlers.format_datetime import FormatDatetimeHandler
from .handlers.hash_stable import HashStableHandler
from .handlers.heuristic_sampler import HeuristicSamplerHandler
from .handlers.leading_zero_normalize import LeadingZeroNormalizeHandler
from .handlers.mask_string import MaskStringHandler
from .handlers.parse_json_extract import ParseJsonExtractHandler
from .pipeline import (
    apply_transform_handler,
    resolve_handler_block,
    resolve_handler_id,
    transform_row_properties,
    validate_transform_config,
    write_output_to_props,
)
from .handlers.regex_substitution import RegexSubstitutionHandler
from .registry import HANDLER_BY_ID
from .handlers.sequential_literal_replace import SequentialLiteralReplaceHandler
from .handlers.split_string import SplitStringHandler
from .handlers.static_lookup_map import StaticLookupMapHandler
from .handlers.substitution_variants import SubstitutionVariantsHandler
from .handlers.trim_whitespace import TrimWhitespaceHandler


def apply_regex_substitution(working: str, block: Mapping[str, Any]) -> str:
    out = RegexSubstitutionHandler.apply(working, block)
    return str(out)


def apply_leading_zero_normalize(working: str, block: Mapping[str, Any]) -> str:
    return str(LeadingZeroNormalizeHandler.apply(working, block))


def apply_sequential_literal_replace(working: str, block: Mapping[str, Any]) -> str:
    return str(SequentialLiteralReplaceHandler.apply(working, block))


def apply_substitution_variants(working: str, block: Mapping[str, Any]) -> List[str]:
    out = SubstitutionVariantsHandler.apply(working, block)
    assert isinstance(out, list)
    return out


def apply_trim_whitespace(working: str, block: Mapping[str, Any]) -> str:
    return str(TrimWhitespaceHandler.apply(working, block))


def apply_change_case(working: str, block: Mapping[str, Any]) -> str:
    return str(ChangeCaseHandler.apply(working, block))


def apply_coerce_scalar(working: str, block: Mapping[str, Any]) -> Any:
    return CoerceScalarHandler.apply(working, block)


def apply_default_if_empty(
    working: str,
    block: Mapping[str, Any],
    *,
    props: Mapping[str, Any] | None = None,
) -> str:
    return str(DefaultIfEmptyHandler.apply(working, block, props=props))


def apply_split_string(working: str, block: Mapping[str, Any]) -> List[str]:
    out = SplitStringHandler.apply(working, block)
    assert isinstance(out, list)
    return out


def apply_parse_json_extract(working: str, block: Mapping[str, Any]) -> str:
    return str(ParseJsonExtractHandler.apply(working, block))


def apply_format_datetime(working: str, block: Mapping[str, Any]) -> str:
    return str(FormatDatetimeHandler.apply(working, block))


def apply_hash_stable(working: str, block: Mapping[str, Any]) -> str:
    return str(HashStableHandler.apply(working, block))


def apply_mask_string(working: str, block: Mapping[str, Any]) -> str:
    return str(MaskStringHandler.apply(working, block))


def apply_static_lookup_map(working: str, block: Mapping[str, Any]) -> str:
    return str(StaticLookupMapHandler.apply(working, block))


def apply_heuristic_sampler(working: str, block: Mapping[str, Any]) -> str:
    return str(HeuristicSamplerHandler.apply(working, block))


__all__ = [
    "ASSET_TAG_FROM_NAME_REGEX",
    "AbstractTransformHandler",
    "CORE_TRANSFORM_HANDLERS",
    "ELT_TRANSFORM_HANDLERS",
    "HANDLER_BY_ID",
    "TRANSFORM_HANDLERS",
    "V1_TRANSFORM_HANDLERS",
    "TransformResult",
    "TransformScalar",
    "apply_change_case",
    "apply_coerce_scalar",
    "apply_default_if_empty",
    "apply_format_datetime",
    "apply_hash_stable",
    "apply_heuristic_sampler",
    "apply_leading_zero_normalize",
    "apply_mask_string",
    "apply_output_template",
    "apply_parse_json_extract",
    "apply_regex_substitution",
    "apply_sequential_literal_replace",
    "apply_split_string",
    "apply_static_lookup_map",
    "apply_substitution_variants",
    "apply_transform_handler",
    "apply_trim_whitespace",
    "extract_field_values",
    "resolve_handler_block",
    "resolve_handler_id",
    "transform_row_properties",
    "validate_transform_config",
    "write_output_to_props",
]
