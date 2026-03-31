"""
Shared confidence scoring for key extraction.

Handler usage:
- RegexExtractionHandler: uses compute_confidence() for substring/token parity.
- FixedWidthExtractionHandler: uses compute_fixed_width_confidence() for position-based parsing.
- HeuristicExtractionHandler: inline weighted strategies and modifiers (no shared function).
- TokenReassemblyExtractionHandler: uses assembly rule priority for confidence.
- PassthroughExtractionHandler: uses rule min_confidence only (no content-based scoring).

Post-extraction confidence: ``validation.confidence_match_rules`` in the engine
(``_validate_extraction_result``) may set or offset confidence per key before
``min_confidence`` filtering.

Pipeline note: instance retrieval respects ``source_view.batch_size``; per-entity
``extract_keys`` runs serially in ``pipeline.py`` (see loop comment there).
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


def compute_fixed_width_confidence(
    value: str,
    field_type: str,
    required: bool,
    validate_fn=None,
) -> float:
    """
    Confidence for fixed-width field extraction (position-based parsing).

    Base 0.9; +0.05 if value passes field-type validation; +0.05 if required and present.
    Capped at 1.0. Pass validate_fn from fixed_width_utils.validate_field_type to share logic.
    """
    if validate_fn is None:
        from .fixed_width_utils import validate_field_type as validate_fn
    base = 0.9
    if validate_fn(value, field_type):
        base += 0.05
    if required:
        base += 0.05
    return min(base, 1.0)
