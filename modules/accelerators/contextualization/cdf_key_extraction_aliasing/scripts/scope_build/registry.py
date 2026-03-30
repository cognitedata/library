"""Ordered registry of scope artifact builders."""

from __future__ import annotations

from pathlib import Path
from typing import List, Protocol, Sequence

from scope_build.builders import ScopeYamlBuilder
from scope_build.context import ScopeBuildContext


class ScopeArtifactBuilder(Protocol):
    name: str

    def run(self, ctx: ScopeBuildContext) -> None: ...


def default_builders(*, template_path: Path) -> List[ScopeArtifactBuilder]:
    return [ScopeYamlBuilder(template_path)]


def filter_builders(
    builders: Sequence[ScopeArtifactBuilder],
    only: Sequence[str] | None,
) -> List[ScopeArtifactBuilder]:
    """Return a subset of ``builders`` by name (preserves first occurrence order in ``only``)."""
    if not only:
        return list(builders)
    by_name = {b.name: b for b in builders}
    seen: set[str] = set()
    out: List[ScopeArtifactBuilder] = []
    for name in only:
        if name in seen:
            continue
        seen.add(name)
        if name not in by_name:
            known = ", ".join(sorted(by_name))
            raise ValueError(
                f"Unknown builder {name!r}. Known builders: {known}. Use --list-builders."
            )
        out.append(by_name[name])
    return out
