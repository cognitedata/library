"""Normalization shared by promote search and cache keys."""

import re

from fa_constants import DEFAULT_NORMALIZATION_SUBSTITUTIONS


def text_variations(
    text: str,
    substitutions: list[tuple[str, str]],
    *,
    convert_to_lowercase: bool,
) -> list[str]:
    """Keep the original and generate every enabled normalization combination."""
    project_value = text
    for pattern, replacement in substitutions:
        project_value = re.sub(pattern, replacement, project_value)

    variations = {text, project_value}
    for pattern, replacement in DEFAULT_NORMALIZATION_SUBSTITUTIONS:
        variations.update(re.sub(pattern, replacement, value) for value in tuple(variations))
    if convert_to_lowercase:
        variations.update(value.lower() for value in tuple(variations))
    return list(variations)


def normalize_text(
    text: str,
    substitutions: list[tuple[str, str]],
    *,
    convert_to_lowercase: bool,
) -> str:
    """Apply project substitutions followed by fixed hygiene substitutions."""
    value = text
    for pattern, replacement in [*substitutions, *DEFAULT_NORMALIZATION_SUBSTITUTIONS]:
        value = re.sub(pattern, replacement, value)
    return value.lower() if convert_to_lowercase else value
