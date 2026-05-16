"""Shared helpers for generated YAML on disk."""


def strip_leading_comments_and_blanks(text: str) -> str:
    """Drop initial # comment lines and blank lines (generated file headers)."""
    lines = text.splitlines()
    while lines and (lines[0].startswith("#") or lines[0].strip() == ""):
        lines.pop(0)
    return "\n".join(lines).lstrip()
