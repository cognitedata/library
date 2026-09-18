"""Normalization shared by promote search and cache keys.

Uses the same capture-group semantics as cdf_entity_matching aliases_update:
each pattern yields an alias from its groups joined by "_". When several patterns
match, only the longest form is kept (single search candidate lineage).

Texts that match none of the patterns must not be searched (e.g. drawing words like
"REPEATED").

Casing is preserved: DMS alias IN filters are case-sensitive exact matches.
"""

import re

from fa_constants import DEFAULT_NORMALIZATION_SUBSTITUTIONS


def extract_forms(text: str, patterns: list[str]) -> list[str]:
    """Return the longest alias extracted from text by the configured patterns.

    Args:
        text: Diagram or OCR text to extract from.
        patterns: Regular expressions with at least one capture group each.

    Returns:
        A single-element list with the longest extracted form, or [] when nothing matches.
    """
    forms: list[str] = []
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        alias = "_".join(group for group in match.groups() if group is not None)
        if alias and alias not in forms:
            forms.append(alias)

    if not forms:
        return []
    return [max(forms, key=len)]


def text_variations(text: str, patterns: list[str]) -> list[str]:
    """Build search variations from the longest extracted form.

    Returns an empty list when no pattern matches so callers skip entity search.
    When a pattern matches, includes the original text plus extracted/hygiene forms.
    """
    forms = extract_forms(text, patterns)
    if not forms:
        return []

    variations = {text, *forms}
    for pattern, replacement in DEFAULT_NORMALIZATION_SUBSTITUTIONS:
        variations.update(re.sub(pattern, replacement, value) for value in tuple(variations))
    return list(variations)


def normalize_text(text: str, patterns: list[str]) -> str:
    """Canonical cache-key form: longest extraction then built-in hygiene.

    Falls back to the original text when no pattern matches (used only for cache keys).
    Casing is preserved to match case-sensitive alias lookup.
    """
    forms = extract_forms(text, patterns)
    value = forms[0] if forms else text
    for pattern, replacement in DEFAULT_NORMALIZATION_SUBSTITUTIONS:
        value = re.sub(pattern, replacement, value)
    return value
