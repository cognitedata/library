"""Shared delimiter split for split_string and split_join handlers."""

from __future__ import annotations

import re
from typing import Any, List, Mapping, Sequence


def _nonempty_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def compile_delimiters_pattern(delimiters: Sequence[Any]) -> str:
    """Build a regex that splits on any listed delimiter (runs of separators when single-char)."""
    tokens = [_nonempty_str(d) for d in delimiters]
    tokens = [t for t in tokens if t]
    if not tokens:
        raise ValueError("delimiters[] must contain at least one non-empty string")
    if all(len(t) == 1 for t in tokens):
        return "[" + "".join(re.escape(t) for t in tokens) + "]+"
    return "(?:" + "|".join(re.escape(t) for t in tokens) + ")+"


def validate_split_parts_block(block: Mapping[str, Any]) -> None:
    regex = _nonempty_str(block.get("delimiter_regex"))
    if regex:
        try:
            re.compile(regex)
        except re.error as exc:
            raise ValueError(f"delimiter_regex: invalid pattern: {exc}") from exc
    delimiters = block.get("delimiters")
    if delimiters is not None:
        if not isinstance(delimiters, list):
            raise ValueError("delimiters must be a list of strings")
        try:
            compile_delimiters_pattern(delimiters)
        except ValueError as exc:
            raise ValueError(f"delimiters: {exc}") from exc


def split_working_parts(working: str, block: Mapping[str, Any]) -> List[str]:
    """
    Split ``working`` into tokens.

    Precedence: ``delimiter_regex`` → ``delimiters[]`` → literal ``delimiter`` (default ``,``).
    Mixed PI-style tags: ``delimiter_regex: '[-./_:]+'`` (include ``:``; put ``-`` first or escape in the class)
    or ``delimiters: [".", "/", "-", "_", ":"]``.
    """
    max_splits = int(block.get("max_splits") if block.get("max_splits") is not None else -1)
    trim_parts = block.get("trim", True) is not False
    drop_empty = block.get("drop_empty", True) is not False

    regex_pat = _nonempty_str(block.get("delimiter_regex"))
    if regex_pat:
        maxsplit = max_splits if max_splits >= 0 else 0
        parts = re.split(regex_pat, working, maxsplit=maxsplit)
    else:
        delimiters = block.get("delimiters")
        if isinstance(delimiters, list) and any(_nonempty_str(d) for d in delimiters):
            pattern = compile_delimiters_pattern(delimiters)
            maxsplit = max_splits if max_splits >= 0 else 0
            parts = re.split(pattern, working, maxsplit=maxsplit)
        else:
            delimiter = str(block.get("delimiter") if block.get("delimiter") is not None else ",")
            parts = (
                working.split(delimiter, max_splits)
                if max_splits >= 0
                else working.split(delimiter)
            )

    if trim_parts:
        parts = [p.strip() for p in parts]
    if drop_empty:
        parts = [p for p in parts if p]
    return parts
