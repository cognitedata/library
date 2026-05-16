"""CDF external id suffix from leaf scope_id (pipeline ids, RAW keys, generated workflow triggers)."""

from __future__ import annotations

import re

_EXTERNAL_ID_SAFE = re.compile(r"[^a-zA-Z0-9_-]+")


def cdf_external_id_suffix(scope_id: str) -> str:
    """Lowercase, CDF-external-id-safe string from ``scope_id`` (``__`` → single ``_``)."""
    s = scope_id.lower().replace("__", "_")
    s = _EXTERNAL_ID_SAFE.sub("_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "scope"


def leaf_level_filename_segment(leaf_level: str) -> str:
    """Filesystem-safe single segment from hierarchy level label (for ``key_extraction_aliasing.<seg>.yaml``)."""
    s = str(leaf_level).strip().lower()
    s = _EXTERNAL_ID_SAFE.sub("_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        raise ValueError("leaf_level must be non-empty after sanitization")
    return s
