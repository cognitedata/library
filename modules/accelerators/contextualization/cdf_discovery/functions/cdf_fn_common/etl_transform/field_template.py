"""Field extraction and ``output_template`` substitution (shared across transforms)."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Mapping, Tuple

from .handlers.base import AbstractTransformHandler


def _field_sort_key(raw: Mapping[str, Any]) -> Tuple[int, str]:
    """Lower ``priority`` number = higher precedence; sort so it is applied **last**."""
    p = raw.get("priority")
    try:
        pri = int(p) if p is not None else 1_000_000
    except (TypeError, ValueError):
        pri = 1_000_000
    name = AbstractTransformHandler.first_nonempty(raw.get("field_name"), raw.get("name"))
    return (-pri, name)


def _regex_flags_from_options(opts: Any) -> int:
    flags = 0
    if not isinstance(opts, dict):
        return flags
    if bool(opts.get("ignore_case") or opts.get("ignoreCase")):
        flags |= re.IGNORECASE
    if bool(opts.get("multiline")):
        flags |= re.MULTILINE
    if bool(opts.get("dotall")):
        flags |= re.DOTALL
    if bool(opts.get("unicode")):
        flags |= re.UNICODE
    return flags


def _regex_extract_matches(
    text: str, pattern: str, flags: int, max_matches: int, match_join: str
) -> str:
    if not pattern:
        return text
    if max_matches <= 1:
        m = re.search(pattern, text, flags)
        return m.group(0) if m else ""
    parts: List[str] = []
    for i, m in enumerate(re.finditer(pattern, text, flags)):
        if i >= max_matches:
            break
        parts.append(m.group(0))
    return match_join.join(parts)


def extract_field_values(props: Mapping[str, Any], fields: Iterable[Any]) -> Dict[str, str]:
    """
    Build ``field_name`` → string values for ``output_template`` substitution.

    Each ``fields[]`` row may include ``field_name`` / ``name``, ``regex``, ``regex_options``,
    ``max_matches_per_field``, ``priority`` (lower = applied last / wins).
    """
    rows: List[Dict[str, Any]] = [f for f in fields if isinstance(f, dict)]
    rows.sort(key=_field_sort_key)
    out: Dict[str, str] = {}
    for raw in rows:
        name = AbstractTransformHandler.first_nonempty(raw.get("field_name"), raw.get("name"))
        if not name:
            continue
        val = props.get(name)
        regex = AbstractTransformHandler.first_nonempty(raw.get("regex"))
        if val is None and not regex:
            continue
        if isinstance(val, list):
            val = val[0] if val else ""
        text = "" if val is None else str(val)
        if regex:
            opts = raw.get("regex_options")
            flags = _regex_flags_from_options(opts)
            match_join = ","
            if isinstance(opts, dict) and opts.get("match_join") is not None:
                match_join = str(opts.get("match_join"))
            max_m = raw.get("max_matches_per_field")
            try:
                max_matches = int(max_m) if max_m is not None else 1
            except (TypeError, ValueError):
                max_matches = 1
            if max_matches < 1:
                max_matches = 1
            text = _regex_extract_matches(text, regex, flags, max_matches, match_join)
        out[name] = text
    return out


# Any `{...}` still present after substitution is treated as an unfilled placeholder.
_UNFILLED_PLACEHOLDER_RE = re.compile(r"\{[^}]+\}")
# Strip/collapse these when they border empty segments (leading/trailing or doubled).
_TEMPLATE_SEP_CHARS = "-_. /"


def _cleanup_template_output(text: str) -> str:
    """
    After ``{key}`` → value (including empty string), drop unfilled placeholders and trim
    redundant separator characters left from missing segments (e.g. ``{unit}-{tag}`` with
    no unit → ``-P-1234`` → ``P-1234``).
    """
    s = _UNFILLED_PLACEHOLDER_RE.sub("", text)
    prev: str | None = None
    while prev != s:
        prev = s
        s = s.strip(_TEMPLATE_SEP_CHARS)
        s = re.sub(r"([-_./ ])\1+", r"\1", s)
    return s


def apply_output_template(template: str, field_values: Mapping[str, str]) -> str:
    out = template
    for key, val in field_values.items():
        repl = "" if val is None else str(val)
        out = out.replace("{" + key + "}", repl)
    return _cleanup_template_output(out)
