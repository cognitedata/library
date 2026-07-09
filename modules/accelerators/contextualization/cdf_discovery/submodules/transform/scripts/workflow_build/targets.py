"""Build targets: one scoped workflow per (workflow_id, scope_suffix)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass(frozen=True)
class ScopedWorkflowTarget:
    workflow_id: str
    scope_suffix: str
    scope_id: str
    node_chain: List[Dict[str, Any]]
    segment_ids: List[str]
    source_kind: str  # instance | template
