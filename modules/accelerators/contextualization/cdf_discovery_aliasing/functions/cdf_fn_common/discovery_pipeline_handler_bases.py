"""Abstract bases for discovery query / save engines (class-based handlers, Cognite functions)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, MutableMapping


class AbstractDiscoveryQueryHandler(ABC):
    """Stateless query handler: implement ``run`` as a classmethod."""

    function_external_id: ClassVar[str]

    @classmethod
    def first_nonempty(cls, *values: Any) -> str:
        for v in values:
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
        return ""

    @classmethod
    @abstractmethod
    def run(
        cls,
        fn_external_id: str,
        data: MutableMapping[str, Any],
        client: Any,
        log: Any,
    ) -> Dict[str, Any]:
        """Execute this query type and return a JSON-serializable summary dict."""


class AbstractDiscoverySaveHandler(ABC):
    """Stateless save handler (stubs today): implement ``run`` as a classmethod."""

    function_external_id: ClassVar[str]

    @classmethod
    @abstractmethod
    def run(cls, data: Dict[str, Any], client: Any) -> Dict[str, Any]:
        """Return Cognite function response dict (``status`` / ``message``)."""
