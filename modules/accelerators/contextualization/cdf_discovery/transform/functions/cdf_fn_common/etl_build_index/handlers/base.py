"""Abstract base for build_index handlers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, ClassVar, DefaultDict, Dict, List, Mapping, Tuple

from cdf_fn_common.etl_transform.handlers.base import AbstractTransformHandler


class AbstractBuildIndexHandler(ABC):
    handler_id: ClassVar[str]

    @classmethod
    def first_nonempty(cls, *values: Any) -> str:
        return AbstractTransformHandler.first_nonempty(*values)

    @classmethod
    @abstractmethod
    def default_block(cls) -> Dict[str, Any]:
        """Handler-specific defaults (merged with the node ``config`` block)."""

    @classmethod
    @abstractmethod
    def collect_postings(
        cls,
        client: Any,
        data: Mapping[str, Any],
        task_id: str,
        *,
        resolved: Mapping[str, Any],
        run_id: str,
    ) -> Tuple[DefaultDict[Tuple[str, str], List[Dict[str, Any]]], int, int, set[Tuple[str, str]]]:
        """Return ``(pending, rows_read, tokens_indexed, entities_seen)``."""

    @classmethod
    @abstractmethod
    def build_rows(
        cls,
        pending: Mapping[Tuple[str, str], List[Dict[str, Any]]],
        *,
        resolved: Mapping[str, Any],
        run_id: str,
        canvas_node_id: str,
    ) -> List[Dict[str, Any]]:
        """Materialize cohort / RAW index rows from aggregated postings."""
