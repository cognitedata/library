"""Build context passed to each scope artifact builder."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


@dataclass(frozen=True)
class PathStep:
    """One level along root→leaf; ``name`` may contain spaces (display)."""

    level: str
    name: str
    description: str | None
    segment_id: str
    node: Dict[str, Any]


@dataclass(frozen=True)
class ScopeBuildContext:
    module_root: Path
    scope_id: str
    levels: List[str]
    path: List[PathStep]
    dry_run: bool
