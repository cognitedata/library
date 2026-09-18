"""Normalization shared by promote search and cache keys.

Uses the same capture-group semantics as cdf_entity_matching aliases_update:
each pattern yields an alias from its groups joined by "_", and selection chooses
among matches when several patterns hit the same text.
"""

import re
from typing import Literal

from fa_constants import DEFAULT_NORMALIZATION_SUBSTITUTIONS

NormalizeSelection = Literal["all", "longest"]


def extract_forms(
    text: str,
    patterns: list[str],
    selection: NormalizeSelection,
) -> list[str]:
    """Return aliases extracted from text by the configured patterns.

    Args:
        text: Diagram or OCR text to extract from.
        patterns: Regular expressions with at least one capture group each.
        selection: Keep every extracted form, or only the longest.

    Returns:
        Extracted forms in pattern order, or a single longest form. Empty when nothing matches.
    """
    forms: list[str] = []
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        alias = "_".join(group for group in match.groups() if group is not None)
        if alias and alias not in forms:
            forms.append(alias)

    if selection == "longest" and forms:
        return [max(forms, key=len)]
    return forms


def text_variations(
    text: str,
    patterns: list[str],
    selection: NormalizeSelection,
    *,
    convert_to_lowercase: bool,
) -> list[str]:
    """Keep the original, extracted forms, and every enabled hygiene combination."""
    variations = {text, *extract_forms(text, patterns, selection)}
    for pattern, replacement in DEFAULT_NORMALIZATION_SUBSTITUTIONS:
        variations.update(re.sub(pattern, replacement, value) for value in tuple(variations))
    if convert_to_lowercase:
        variations.update(value.lower() for value in tuple(variations))
    return list(variations)


def normalize_text(
    text: str,
    patterns: list[str],
    selection: NormalizeSelection,
    *,
    convert_to_lowercase: bool,
) -> str:
    """Canonical cache-key form: preferred extraction then built-in hygiene."""
    forms = extract_forms(text, patterns, selection)
    value = forms[0] if forms else text
    for pattern, replacement in DEFAULT_NORMALIZATION_SUBSTITUTIONS:
        value = re.sub(pattern, replacement, value)
    return value.lower() if convert_to_lowercase else value
