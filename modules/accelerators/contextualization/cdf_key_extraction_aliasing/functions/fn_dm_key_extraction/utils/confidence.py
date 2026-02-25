"""
Shared confidence scoring for key extraction.

Applies consistent rules across extraction handlers:
- Blacklisted (handled in engine): 0.0
- Exact match (source == extracted): 1.0
- Source starts with or ends with extracted: 0.90
- Source contains extracted: 0.80
- Else: token-overlap score (matching tokens / total extracted tokens), capped at 0.70 (below "contains" for parity with other methods)
"""

import re
from typing import List

# First-match wins: order and scores are easy to change.
SUBSTRING_MATCH_SCORES = [
    ("exact", 1.0),
    ("start", 0.90),
    ("end", 0.90),
    ("contains", 0.80),
]


def _tokenize(text: str, case_sensitive: bool = False) -> List[str]:
    """Split text into alphanumeric tokens."""
    if not text:
        return []
    normalized = text if case_sensitive else text.lower()
    tokens = re.findall(r"\w+", normalized)
    return tokens


def _substring_match_kind(src: str, ext: str) -> str:
    """
    Classify how extracted appears in source using a single find().
    Returns one of: "exact", "start", "end", "contains", "none".
    """
    pos = src.find(ext)
    if pos == -1:
        return "none"
    if pos == 0 and len(ext) == len(src):
        return "exact"
    if pos == 0:
        return "start"
    if pos + len(ext) == len(src):
        return "end"
    return "contains"


def compute_confidence(
    source_value: str,
    extracted_value: str,
    case_sensitive: bool = False,
) -> float:
    """
    Compute confidence score for an extracted value relative to its source.

    Rules (first match wins, from SUBSTRING_MATCH_SCORES):
    1. Exact match -> 1.0
    2. Source starts with or ends with extracted -> 0.90
    3. Source contains extracted -> 0.80
    4. Else: token-overlap (matching extracted tokens in source / total extracted tokens), capped at 0.70

    Args:
        source_value: The full source field value (e.g. asset name or description).
        extracted_value: The value extracted by the rule (e.g. tag or key).
        case_sensitive: If False, comparisons are case-insensitive.

    Returns:
        Confidence score in [0.0, 1.0].
    """
    if not source_value or not extracted_value:
        return 0.0

    src = source_value.strip()
    ext = extracted_value.strip()
    if not case_sensitive:
        src = src.lower()
        ext = ext.lower()

    kind = _substring_match_kind(src, ext)
    for match_kind, score in SUBSTRING_MATCH_SCORES:
        if kind == match_kind:
            return score

    # Token-overlap: (extracted tokens found in source) / (total extracted tokens), cap 0.70 (tier 3 parity)
    src_tokens = set(_tokenize(source_value, case_sensitive))
    ext_tokens = _tokenize(extracted_value, case_sensitive)
    if not ext_tokens:
        return 0.0
    matches = sum(1 for t in ext_tokens if t in src_tokens)
    ratio = matches / len(ext_tokens)
    return min(ratio, 0.70)
