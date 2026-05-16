"""Transform catalog constants (v1)."""

from __future__ import annotations

CORE_TRANSFORM_HANDLERS = frozenset(
    {
        "regex_substitution",
        "leading_zero_normalize",
        "sequential_literal_replace",
        "substitution_variants",
    }
)

ELT_TRANSFORM_HANDLERS = frozenset(
    {
        "trim_whitespace",
        "change_case",
        "coerce_scalar",
        "default_if_empty",
        "split_string",
        "parse_json_extract",
        "format_datetime",
        "hash_stable",
        "mask_string",
        "static_lookup_map",
        "heuristic_sampler",
    }
)

TRANSFORM_HANDLERS = CORE_TRANSFORM_HANDLERS | ELT_TRANSFORM_HANDLERS
V1_TRANSFORM_HANDLERS = TRANSFORM_HANDLERS

_MULTI_VALUE_HANDLERS = frozenset({"substitution_variants", "split_string"})

_OUTPUT_MODES = frozenset({"overwrite", "append"})
_OUTPUT_MULTI_VALUE = frozenset({"array_json", "explode_rows"})

ASSET_TAG_FROM_NAME_REGEX = (
    r"(?<![\d-])(?:\b|(?<=_))(?:\d{1,8}-?)?[A-Z]{1,8}-?\d{1,10}(?:-\d{1,6})*[A-Z]?\b"
)
