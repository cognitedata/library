"""Handler: hash_stable."""

from __future__ import annotations

import hashlib
from typing import Any, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


class HashStableHandler(AbstractTransformHandler):
    handler_id = "hash_stable"
    description = (
        "Compute a deterministic hash (sha256, sha1, or md5) of the working string with optional salt. "
        "Use for stable surrogate keys and deduplication fingerprints."
    )

    @classmethod
    def apply(
        cls,
        working: str,
        block: Mapping[str, Any],
        *,
        field_values: Optional[Mapping[str, str]] = None,
        props: Optional[Mapping[str, Any]] = None,
    ) -> TransformResult:
        del field_values, props
        algo = cls.first_nonempty(block.get("algorithm"), "sha256").lower()
        salt = str(block.get("salt") or "")
        payload = (salt + working).encode("utf-8")
        if algo == "sha256":
            return hashlib.sha256(payload).hexdigest()
        if algo == "sha1":
            return hashlib.sha1(payload).hexdigest()
        if algo == "md5":
            return hashlib.md5(payload).hexdigest()
        raise ValueError(f"hash_stable: unsupported algorithm {algo!r}")
