"""Typed loaders for modules/packages.toml, module.toml and the mixpanel lookup, shared by the registry tests."""

import csv
import tomllib
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULES = REPO_ROOT / "modules"
PACKAGES_TOML = MODULES / "packages.toml"
MIXPANEL_LOOKUP = REPO_ROOT / "mixpanel" / "module_lookup.csv"


@dataclass(frozen=True)
class PackageEntry:
    """One pack from packages.toml."""

    id: str
    title: str
    can_cherry_pick: bool
    modules: list[str]


@dataclass(frozen=True)
class ModuleSpec:
    """The [module] table of a module.toml."""

    id: str
    title: str
    package_id: str


def load_packages(path: Path) -> dict[str, PackageEntry]:
    """
    Load the packs of a packages.toml.

    Args:
        path: path to packages.toml

    Returns:
        the packs keyed by their table name (for example "context_quality"), not by their `id`. A pack without
        `canCherryPick` counts as not cherry-pickable.
    """
    packages = tomllib.loads(path.read_text())["packages"]
    return {
        name: PackageEntry(
            id=data["id"],
            title=data["title"],
            can_cherry_pick=data.get("canCherryPick", False),
            modules=list(data["modules"]),
        )
        for name, data in packages.items()
    }


def load_module_spec(path: Path) -> ModuleSpec:
    """
    Load the [module] table of a module.toml.

    Args:
        path: path to module.toml

    Returns:
        the module's id, title and primary package id.
    """
    module = tomllib.loads(path.read_text())["module"]
    return ModuleSpec(id=module["id"], title=module["title"], package_id=module["package_id"])


def load_mixpanel_lookup() -> dict[str, str]:
    """Return the mixpanel lookup as module id -> module name."""
    with MIXPANEL_LOOKUP.open(newline="", encoding="utf-8") as lookup_file:
        return {row["moduleId"]: row["module name"] for row in csv.DictReader(lookup_file)}
