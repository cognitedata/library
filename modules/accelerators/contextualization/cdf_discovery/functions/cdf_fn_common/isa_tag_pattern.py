"""ISA tag area/unit prefix shared with cdf_discovery_aliasing ``tag_patterns.yaml``."""

from __future__ import annotations

# Optional area, unit, or train prefix before equipment tag body (0–4 segments).
ISA_TAG_AREA_PREFIX = r"(?:(?:[A-Za-z0-9]{1,12}|\d{1,8})[-_/]){0,4}"


def should_apply_isa_tag_area_prefix(pattern: str) -> bool:
    """True when *pattern* is an ISA tag suffix that may be prefixed in plant IDs."""
    p = pattern.strip()
    if not p or ISA_TAG_AREA_PREFIX in p:
        return False
    if p.startswith("^") or p.startswith("(?s)"):
        return False
    if p.startswith("[") or p.startswith("'[") or p.startswith("'[^"):
        return False
    return r"\b" in p


def with_optional_isa_tag_area_prefix(pattern: str) -> str:
    """Return *pattern* with optional area prefix when applicable."""
    p = pattern.strip()
    if not p or not should_apply_isa_tag_area_prefix(p):
        return p
    return ISA_TAG_AREA_PREFIX + p
