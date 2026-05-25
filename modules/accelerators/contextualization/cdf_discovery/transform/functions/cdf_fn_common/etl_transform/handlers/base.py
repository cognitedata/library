"""Abstract base for discovery v1 transform handlers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, List, Mapping, Optional, Union

TransformScalar = Union[str, int, float, bool, None]
TransformResult = Union[TransformScalar, List[str]]


class AbstractTransformHandler(ABC):
    """Stateless handler: implement ``apply`` as a classmethod."""

    handler_id: ClassVar[str]
    multi_value: ClassVar[bool] = False

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
    def is_blank(cls, value: str) -> bool:
        return not str(value).strip()

    @classmethod
    @abstractmethod
    def apply(
        cls,
        working: str,
        block: Mapping[str, Any],
        *,
        field_values: Optional[Mapping[str, str]] = None,
        props: Optional[Mapping[str, Any]] = None,
    ) -> TransformResult:
        """Transform ``working`` string using handler-specific ``block`` config."""
