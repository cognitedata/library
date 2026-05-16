"""Binding context for one generated artifact row."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class PathStep:
    level: str
    name: str
    description: str | None
    segment_id: str
    node: Dict[str, Any]


@dataclass(frozen=True)
class ScopeBinding:
    scope_id: str
    path: List[PathStep]
    segments: List[str]


@dataclass
class RenderContext:
    scope_id: str
    scope_id_snake: str
    path: List[PathStep]
    segments: List[str]
    flat: Dict[str, Any] = field(default_factory=dict)


def scope_id_to_snake(scope_id: str) -> str:
    """Normalize ``SITE__PLANT`` → ``site_plant`` for stable snake_case segments."""
    s = scope_id.replace("__", "_").replace("-", "_")
    s = re.sub(r"_+", "_", s).strip("_")
    return s.lower()


def filename_stem_from_name(name: str) -> str:
    """Stable filename stem from a Space ``name`` or Group ``name`` (repo-safe, lowercase)."""
    s = name.strip()
    if not s:
        return "unnamed"
    s = re.sub(r"[/\\\0]", "_", s)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("._-")
    return s.lower() if s else "unnamed"


def top_level_scope_folder(segments: List[str]) -> str:
    """Directory name for the hierarchy root segment (Jinja / metadata)."""
    if not segments:
        return "_ungrouped"
    return scope_id_to_snake(segments[0])


def scope_tree_folder_parts(segments: List[str]) -> List[str]:
    """One filesystem directory per scope hierarchy segment under ``spaces/`` or ``auth/``."""
    if not segments:
        return ["_ungrouped"]
    return [scope_id_to_snake(seg) for seg in segments]


def instance_space_external_id(
    scope_id: str, *, prefix: str = "inst_", source_system_id: Optional[str] = None
) -> str:
    """CDF instance space external id: ``inst_{snake(scope)}``, or ``inst_{source}_{scope}`` when given."""
    scope_part = scope_id_to_snake(scope_id)
    if source_system_id:
        src = scope_id_to_snake(str(source_system_id))
        return f"{prefix}{src}_{scope_part}"
    return f"{prefix}{scope_part}"
