"""Handler: heuristic_sampler — first re.search match from literals or regex."""

from __future__ import annotations

import re
from typing import Any, List, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult

MAX_HEURISTIC_SAMPLER_SAMPLES = 200
MAX_HEURISTIC_SAMPLER_PATTERN_BYTES = 64 * 1024

_ON_NO_MATCH_ALLOWED = frozenset({"keep_working", "empty", "default"})


def _normalized_samples(block: Mapping[str, Any]) -> List[str]:
    raw = block.get("samples")
    if not isinstance(raw, list):
        return []
    out: List[str] = []
    for item in raw:
        s = str(item).strip()
        if s:
            out.append(s)
    return out


def build_heuristic_sampler_regex(block: Mapping[str, Any]) -> str:
    """Build the alternation (or single user pattern) used for matching."""
    pattern = AbstractTransformHandler.first_nonempty(block.get("pattern"))
    if pattern:
        return pattern
    samples = _normalized_samples(block)
    samples_as_regex = bool(block.get("samples_as_regex"))
    if not samples:
        raise ValueError("heuristic_sampler: set non-empty `pattern` or `samples` with non-blank strings")
    if len(samples) > MAX_HEURISTIC_SAMPLER_SAMPLES:
        raise ValueError(
            f"heuristic_sampler: at most {MAX_HEURISTIC_SAMPLER_SAMPLES} samples allowed; got {len(samples)}"
        )
    ordered = sorted(samples, key=lambda s: (-len(s), s))
    if samples_as_regex:
        return "|".join(ordered)
    return "|".join(re.escape(s) for s in ordered)


def validate_heuristic_sampler_block(block: Mapping[str, Any]) -> None:
    """Validate config; compiles regex to fail fast on invalid patterns."""
    rx = build_heuristic_sampler_regex(block)
    if len(rx.encode("utf-8")) > MAX_HEURISTIC_SAMPLER_PATTERN_BYTES:
        raise ValueError(
            f"heuristic_sampler: combined pattern length exceeds {MAX_HEURISTIC_SAMPLER_PATTERN_BYTES} bytes"
        )
    try:
        re.compile(rx)
    except re.error as e:
        raise ValueError(f"heuristic_sampler: invalid regex: {e}") from e
    on_nm = str(block.get("on_no_match") or "keep_working").strip().lower()
    if on_nm not in _ON_NO_MATCH_ALLOWED:
        raise ValueError(
            f"heuristic_sampler: on_no_match must be one of {sorted(_ON_NO_MATCH_ALLOWED)}; got {on_nm!r}"
        )


class HeuristicSamplerHandler(AbstractTransformHandler):
    handler_id = "heuristic_sampler"
    multi_value = False

    @classmethod
    def apply(
        cls,
        working: str,
        block: Mapping[str, Any],
        *,
        field_values: Optional[Mapping[str, str]] = None,
        props: Optional[Mapping[str, Any]] = None,
    ) -> TransformResult:
        del field_values, props
        rx = build_heuristic_sampler_regex(block)
        m = re.search(rx, working)
        if m:
            return m.group(0)
        on_nm = str(block.get("on_no_match") or "keep_working").strip().lower()
        if on_nm == "empty":
            return ""
        if on_nm == "default":
            return str(block.get("default_value") or "")
        return working
