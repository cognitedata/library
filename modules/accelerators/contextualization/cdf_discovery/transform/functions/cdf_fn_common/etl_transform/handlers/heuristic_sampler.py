"""Handler: heuristic_sampler — regex matches from literals or a custom pattern."""

from __future__ import annotations

import re
from typing import Any, List, Mapping, Optional, Union

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


def parse_max_results(block: Mapping[str, Any]) -> Optional[int]:
    """
    Return match cap for heuristic_sampler.

    - Key absent: ``1`` (first match only; backward compatible).
    - ``null`` / ``0``: unlimited matches.
    - Positive int: at most that many distinct matches (left-to-right order).
    """
    if "max_results" not in block:
        return 1
    raw = block.get("max_results")
    if raw is None:
        return None
    try:
        n = int(raw)
    except (TypeError, ValueError) as e:
        raise ValueError(
            f"heuristic_sampler: max_results must be a non-negative integer, null, or omitted; got {raw!r}"
        ) from e
    if n < 0:
        raise ValueError(f"heuristic_sampler: max_results must be non-negative; got {n}")
    if n == 0:
        return None
    return n


def heuristic_sampler_multi_value(block: Mapping[str, Any]) -> bool:
    """True when the handler emits multiple values (list) for the row pipeline."""
    return parse_max_results(block) != 1


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


def find_heuristic_sampler_matches(
    working: str,
    rx: str,
    *,
    max_results: Optional[int],
) -> List[str]:
    """Return distinct match strings in left-to-right order."""
    if not working:
        return []
    out: List[str] = []
    seen: set[str] = set()
    for m in re.finditer(rx, working):
        s = m.group(0)
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
        if max_results is not None and len(out) >= max_results:
            break
    return out


def _no_match_result(
    working: str,
    block: Mapping[str, Any],
    *,
    multi: bool,
) -> TransformResult:
    on_nm = str(block.get("on_no_match") or "keep_working").strip().lower()
    if on_nm == "empty":
        return [] if multi else ""
    if on_nm == "default":
        value = str(block.get("default_value") or "")
        return [value] if multi else value
    return [working] if multi else working


def apply_heuristic_sampler_core(
    working: str,
    block: Mapping[str, Any],
) -> TransformResult:
    """Run heuristic_sampler; returns a scalar when ``max_results`` is 1, else a list."""
    max_results = parse_max_results(block)
    multi = max_results != 1
    rx = build_heuristic_sampler_regex(block)
    matches = find_heuristic_sampler_matches(working, rx, max_results=max_results)
    if matches:
        if multi:
            return matches
        return matches[0]
    return _no_match_result(working, block, multi=multi)


def validate_heuristic_sampler_block(block: Mapping[str, Any]) -> None:
    """Validate config; compiles regex to fail fast on invalid patterns."""
    parse_max_results(block)
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
        return apply_heuristic_sampler_core(working, block)


def apply_heuristic_sampler(working: str, block: Mapping[str, Any]) -> Union[str, List[str]]:
    """Facade for tests and direct calls; scalar when ``max_results`` defaults to 1."""
    result = apply_heuristic_sampler_core(working, block)
    if isinstance(result, list) and parse_max_results(block) == 1:
        return result[0] if result else ""
    return result
