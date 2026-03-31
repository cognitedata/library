"""CDF external id suffix from leaf scope_id (must match workflow fusion ``scope_cdf_suffix``)."""

from __future__ import annotations

import re

_EXTERNAL_ID_SAFE = re.compile(r"[^a-zA-Z0-9_-]+")


def cdf_external_id_suffix(scope_id: str) -> str:
    """Lowercase, CDF-external-id-safe string from ``scope_id`` (``__`` → single ``_``)."""
    s = scope_id.lower().replace("__", "_")
    s = _EXTERNAL_ID_SAFE.sub("_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "scope"
