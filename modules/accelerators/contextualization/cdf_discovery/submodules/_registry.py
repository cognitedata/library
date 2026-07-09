"""Discovery submodule registry — CLI, API, and Toolkit roots."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


@dataclass(frozen=True)
class DiscoverySubmodule:
    id: str
    tree_root_id: str
    cli_commands: tuple[str, ...]
    toolkit_roots: tuple[Path, ...]
    enabled: bool = True


def _rel(*parts: str) -> Path:
    return Path(*parts)


DISCOVERY_SUBMODULES: tuple[DiscoverySubmodule, ...] = (
    DiscoverySubmodule(
        id="data",
        tree_root_id="data",
        cli_commands=(),
        toolkit_roots=(),
    ),
    DiscoverySubmodule(
        id="fusion",
        tree_root_id="fusion",
        cli_commands=(),
        toolkit_roots=(),
    ),
    DiscoverySubmodule(
        id="governance",
        tree_root_id="gov",
        cli_commands=("build",),
        toolkit_roots=(
            _rel("submodules/governance"),
            _rel("spaces"),
            _rel("auth"),
        ),
    ),
    DiscoverySubmodule(
        id="extract",
        tree_root_id="extract",
        cli_commands=(),
        toolkit_roots=(),
        enabled=True,
    ),
    DiscoverySubmodule(
        id="transform",
        tree_root_id="transform",
        cli_commands=("transform",),
        toolkit_roots=(
            _rel("submodules/transform/functions"),
            _rel("workflows"),
            _rel("data_sets/ds_discovery_etl.DataSet.yaml"),
        ),
    ),
    DiscoverySubmodule(
        id="inverted_index",
        tree_root_id="index",
        cli_commands=("index",),
        toolkit_roots=(
            _rel("submodules/inverted_index/functions"),
            _rel("submodules/inverted_index/raw"),
            _rel("submodules/inverted_index/data_modeling"),
            _rel("data_sets/ds_inverted_index_all.DataSet.yaml"),
            _rel("workflows"),
        ),
    ),
    DiscoverySubmodule(
        id="monitor",
        tree_root_id="monitor",
        cli_commands=(),
        toolkit_roots=(),
    ),
)


def enabled_submodules() -> Sequence[DiscoverySubmodule]:
    return tuple(s for s in DISCOVERY_SUBMODULES if s.enabled)


def submodule_by_id(submodule_id: str) -> DiscoverySubmodule | None:
    for s in DISCOVERY_SUBMODULES:
        if s.id == submodule_id:
            return s
    return None


def submodule_by_cli_command(command: str) -> DiscoverySubmodule | None:
    for s in enabled_submodules():
        if command in s.cli_commands:
            return s
    return None


def all_toolkit_resource_paths() -> list[Path]:
    from shared.python.paths import module_root

    root = module_root()
    paths: list[Path] = []
    seen: set[Path] = set()
    for sub in enabled_submodules():
        for rel in sub.toolkit_roots:
            p = (root / rel).resolve()
            if p not in seen and (p.exists() or str(rel).endswith(".yaml")):
                seen.add(p)
                paths.append(p)
    return paths


def prepare_sys_path() -> list[str]:
    from shared.python.paths import prepare_sys_path as _prepare

    return _prepare()
