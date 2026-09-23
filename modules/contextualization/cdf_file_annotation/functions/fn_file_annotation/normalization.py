"""Normalization shared by promote search, auto pattern sample generation, and cache keys.

Uses the same capture-group semantics as cdf_entity_matching aliases_update:
each pattern yields an alias from its groups joined by "_". When several patterns
match, only the longest form is kept (single search candidate lineage).

Callers choose entity vs file pattern lists. When the chosen list is empty, no filtering
is applied (all aliases/text kept). When patterns are set, texts that match none must
not be searched and must not contribute auto-generated structural pattern samples.

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

    When ``patterns`` is empty, no filtering is applied — variations are built from the
    original text (hygiene only). When patterns are set, returns an empty list if none
    match so callers skip entity search. On a match, includes the original text plus
    extracted/hygiene forms.
    """
    if patterns:
        forms = extract_forms(text, patterns)
        if not forms:
            return []
        variations = {text, *forms}
    else:
        variations = {text}

    variations.update(_with_underscore_separators(value) for value in tuple(variations))
    for pattern, replacement in DEFAULT_NORMALIZATION_SUBSTITUTIONS:
        variations.update(re.sub(pattern, replacement, value) for value in tuple(variations))
    return list(variations)


def _with_underscore_separators(value: str) -> str:
    """The tag written with "_" between tokens, the spelling aliases_update stores.

    A pattern whose single capture group holds the whole tag keeps the separators the
    drawing used, so "PH-ME-P-0151-001" would never find the alias "PH_ME_P_0151_001".
    """
    return re.sub(r"[-_.:]+", "_", value)


def normalize_text(text: str, patterns: list[str]) -> str:
    """Canonical cache-key form: longest extraction then built-in hygiene.

    When ``patterns`` is empty, hygiene is applied to the original text only.
    Falls back to the original text when patterns are set but none match.
    Casing is preserved to match case-sensitive alias lookup.
    """
    forms = extract_forms(text, patterns) if patterns else []
    value = forms[0] if forms else text
    for pattern, replacement in DEFAULT_NORMALIZATION_SUBSTITUTIONS:
        value = re.sub(pattern, replacement, value)
    return value
