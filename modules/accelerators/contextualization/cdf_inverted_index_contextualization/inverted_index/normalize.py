"""Term normalization aligned with CDF entity-matching tokenization."""

from __future__ import annotations

import regex as re

# Letters (any script) and digit runs — aligned with CDF entity matching.
_TOKEN_PATTERN = re.compile(r"\p{L}+|\d+")


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

    Rules:
    - Tokenize with Unicode letters (\\p{L}) and digit runs.
    - casefold letter tokens; concatenate in order.
    - Empty input → empty string.

    Example: ``P-101A`` → ``p101a``; ``ポンプ-101`` → ``ポンプ101``.
    """
    if not value or not str(value).strip():
        return ""
    tokens = _TOKEN_PATTERN.findall(str(value).strip().casefold())
    return "".join(tokens)
