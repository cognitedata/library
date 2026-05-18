"""Row-level cohort filters for the discovery ``filter`` canvas stage."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from .cohort_filter_eval import (
    parse_cohort_filters,
    row_matches_filters,
    validate_cohort_filters_config,
)

# Legacy re-exports for tests and callers that imported row_match constants.
ROW_MATCH_ANY = "any"
ROW_MATCH_ALL = "all"
ROW_MATCH_MODES = frozenset({ROW_MATCH_ANY, ROW_MATCH_ALL})


def parse_row_match_mode(cfg: Mapping[str, Any]) -> str:
    """Deprecated: retained for callers that still read ``row_match`` from config."""
    mode = str(cfg.get("row_match") or ROW_MATCH_ANY).lower()
    if mode not in ROW_MATCH_MODES:
        raise ValueError(f"row_match must be one of {sorted(ROW_MATCH_MODES)}; got {mode!r}")
    return mode


def validate_filter_config(cfg: Mapping[str, Any]) -> None:
    validate_cohort_filters_config(cfg, require_description=True)


def row_passes_filter(
    props: Mapping[str, Any],
    filters: Sequence[Any] | None,
    *,
    row_match: str = ROW_MATCH_ANY,  # noqa: ARG001 — ignored; use filter tree composition
) -> bool:
    """Return whether a cohort entity row should be kept."""
    return row_matches_filters(props, filters)


def apply_row_filter_to_properties(
    props: Mapping[str, Any],
    filters: Sequence[Any] | None,
    *,
    row_match: str = ROW_MATCH_ANY,
) -> bool:
    """Return True when *props* passes the row filter (no in-place mutation)."""
    return row_passes_filter(props, filters, row_match=row_match)
