"""Tests for the contents of modules/packages.toml and the module.toml files it points at."""

import csv
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULES = REPO_ROOT / "modules"
PACKAGES = tomllib.loads((MODULES / "packages.toml").read_text())["packages"]
CONTEXT_QUALITY_MODULES = ["dashboards/context_quality_v2", "tools/apps/context_quality_dashboard"]


def test_removed_packs_are_unregistered_but_their_folders_are_kept() -> None:
    registered = {module for package in PACKAGES.values() for module in package["modules"]}

    assert "dashboards" not in PACKAGES
    assert "contextualization/cdf_p_and_id_annotation" not in registered
    assert "dashboards/context_quality" not in registered
    assert (MODULES / "contextualization" / "cdf_p_and_id_annotation" / "module.toml").is_file()
    assert (MODULES / "dashboards" / "context_quality" / "module.toml").is_file()


def test_context_quality_package_is_cherry_pickable_and_holds_the_backend_and_the_dashboard() -> None:
    package = PACKAGES["context_quality"]

    assert package["id"] == "dp:context_quality_v2"
    assert package["canCherryPick"] is True
    assert package["modules"] == CONTEXT_QUALITY_MODULES
    assert "tools/apps/context_quality_dashboard" not in PACKAGES["tools"]["modules"]


def test_context_quality_modules_are_named_in_their_module_toml() -> None:
    backend = tomllib.loads((MODULES / CONTEXT_QUALITY_MODULES[0] / "module.toml").read_text())["module"]
    dashboard = tomllib.loads((MODULES / CONTEXT_QUALITY_MODULES[1] / "module.toml").read_text())["module"]

    assert (backend["title"], backend["id"]) == (
        "Contextualization Quality Dashboard Backend",
        "dp:context_quality_v2:backend",
    )
    assert (dashboard["title"], dashboard["id"]) == (
        "Contextualization Quality Dashboard Flows",
        "dp:context_quality_v2:dashboard",
    )
    assert backend["package_id"] == dashboard["package_id"] == "dp:context_quality_v2"


def test_packs_with_a_single_module_are_not_cherry_pickable() -> None:
    # `tools` is an open-ended collection of accelerators, so it stays cherry-pickable with one tool today.
    offenders = [
        name
        for name, package in PACKAGES.items()
        if len(package["modules"]) == 1 and package["canCherryPick"] and name != "tools"
    ]

    assert offenders == []


def test_every_module_id_has_a_mixpanel_lookup_row() -> None:
    with open(REPO_ROOT / "mixpanel" / "module_lookup.csv", newline="") as lookup_file:
        lookup = {row["moduleId"]: row["module name"] for row in csv.DictReader(lookup_file)}
    missing: list[str] = []
    for module_toml in sorted(MODULES.rglob("module.toml")):
        module = tomllib.loads(module_toml.read_text())["module"]
        if module["id"] not in lookup:
            missing.append(module["id"])

    assert missing == []


def test_mixpanel_lookup_names_the_context_quality_modules_and_keeps_their_previous_ids() -> None:
    with open(REPO_ROOT / "mixpanel" / "module_lookup.csv", newline="") as lookup_file:
        lookup = {row["moduleId"]: row["module name"] for row in csv.DictReader(lookup_file)}

    assert lookup["dp:context_quality_v2:backend"] == "Contextualization Quality Dashboard Backend"
    assert lookup["dp:context_quality_v2:dashboard"] == "Contextualization Quality Dashboard Flows"
    assert lookup["dp:dashboards:context_quality_v2"] == "Contextualization Quality Dashboard Backend"
    assert lookup["dp:tool:context_quality_dashboard"] == "Contextualization Quality Dashboard Flows"
    assert "dp:dashboards:context_quality" in lookup
    assert "dp:contextualization:cdf_p_and_id_annotation" in lookup
