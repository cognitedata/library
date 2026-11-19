"""Tests for the contents of modules/packages.toml and the module.toml files it points at."""

from tests.registry_helpers import MODULES, PACKAGES_TOML, load_mixpanel_lookup, load_module_spec, load_packages

PACKAGES = load_packages(PACKAGES_TOML)
CONTEXT_QUALITY_MODULES = ["dashboards/context_quality", "tools/apps/context_quality_dashboard"]


def test_stale_modules_and_the_dashboards_pack_are_removed() -> None:
    registered = {module for package in PACKAGES.values() for module in package.modules}

    assert "dashboards" not in PACKAGES
    assert "contextualization/cdf_p_and_id_annotation" not in registered
    assert not (MODULES / "contextualization" / "cdf_p_and_id_annotation").exists()
    assert not (MODULES / "dashboards" / "context_quality" / "streamlit").exists()


def test_context_quality_package_is_cherry_pickable_and_holds_the_backend_and_the_dashboard() -> None:
    package = PACKAGES["context_quality"]

    assert package.id == "dp:context_quality"
    assert package.can_cherry_pick is True
    assert package.modules == CONTEXT_QUALITY_MODULES
    assert "tools/apps/context_quality_dashboard" not in PACKAGES["tools"].modules


def test_context_quality_modules_are_named_in_their_module_toml() -> None:
    backend = load_module_spec(MODULES / CONTEXT_QUALITY_MODULES[0] / "module.toml")
    dashboard = load_module_spec(MODULES / CONTEXT_QUALITY_MODULES[1] / "module.toml")

    assert (backend.title, backend.id) == (
        "Contextualization Quality Dashboard Backend",
        "dp:context_quality:backend",
    )
    assert (dashboard.title, dashboard.id) == (
        "Contextualization Quality Dashboard App",
        "dp:context_quality:dashboard",
    )
    assert backend.package_id == dashboard.package_id == "dp:context_quality"


def test_packs_with_a_single_module_are_not_cherry_pickable() -> None:
    # `tools` is an open-ended collection of accelerators, so it stays cherry-pickable with one tool today.
    offenders = [
        name
        for name, package in PACKAGES.items()
        if len(package.modules) == 1 and package.can_cherry_pick and name != "tools"
    ]

    assert offenders == []


def test_every_module_id_has_a_mixpanel_lookup_row() -> None:
    lookup = load_mixpanel_lookup()
    module_ids = [load_module_spec(module_toml).id for module_toml in sorted(MODULES.rglob("module.toml"))]

    assert lookup and module_ids, "found no mixpanel rows or no module.toml files, so the check would pass vacuously"
    assert [module_id for module_id in module_ids if module_id not in lookup] == []


def test_mixpanel_lookup_names_the_context_quality_modules_and_drops_the_removed_ones() -> None:
    lookup = load_mixpanel_lookup()

    assert lookup["dp:context_quality:backend"] == "Contextualization Quality Dashboard Backend"
    assert lookup["dp:context_quality:dashboard"] == "Contextualization Quality Dashboard App"
    removed = {"dp:contextualization:cdf_p_and_id_annotation", "dp:dashboards:context_quality"}
    assert removed.isdisjoint(lookup)
