"""Term normalization aligned with CDF entity-matching tokenization."""

from __future__ import annotations

import re

# Letters (any script) and digit runs — simplified ASCII variant for prototype.
_TOKEN_PATTERN = re.compile(r"[A-Za-z]+|\d+")


def normalize_query_terms(terms: list[str]) -> list[str]:
    """Normalize and dedupe lookup terms for index queries."""
    seen: set[str] = set()
    normalized_terms: list[str] = []
    for term in terms:
        if not term:
            continue
        normalized = normalize_term(str(term))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_terms.append(normalized)
    return normalized_terms


def normalize_term(value: str) -> str:
    """
    Canonical term form for index keys and lookups.

    Rules (prototype):
    - Lowercase letter tokens; concatenate with digit tokens in order.
    - Strip leading/trailing whitespace on input.
    - Empty input → empty string.

    Example: ``P-101A`` → ``p101a``; ``21-PT-1017`` → ``21pt1017``.
    """
    if not value or not str(value).strip():
        return ""
    tokens = _TOKEN_PATTERN.findall(str(value).strip().lower())
    return "".join(tokens)
