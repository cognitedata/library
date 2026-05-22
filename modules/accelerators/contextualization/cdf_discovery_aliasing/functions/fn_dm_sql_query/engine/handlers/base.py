"""Base for discovery SQL query handlers."""

from __future__ import annotations

from typing import Any


class AbstractDiscoveryQueryHandler:
    @staticmethod
    def first_nonempty(*values: Any) -> str:
        for v in values:
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
        return ""
