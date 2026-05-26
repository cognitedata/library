"""
Detect the installed data model under modules/data_models/ and source systems under
modules/sourcesystem/, then sync config.<env>.yaml.

Scans <pack-root>/modules/data_models/ for isa_manufacturing_extension or
cfihos_oil_and_gas_extension, and <pack-root>/modules/sourcesystem/ for foundation
modules (pi, sap, opcua, db, files). Updates enabledSources, contextualization,
sourcesystem, ingestion foundation, and data_models in every config.<env>.yaml.

Run this before build_workflow.py.

Usage:
    python configure_datamodel.py [-y] [--check]
"""

from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

import yaml
from _pack_config import (
    REPO_ROOT,
    SOURCE_SYSTEM_DIR_TO_ENABLED_KEY,
    deep_merge,
    detect_data_model_variant,
    detect_enabled_sources,
    find_env_configs,
    get_data_models_dir,
    get_pack_root,
    get_sourcesystem_dir,
    list_installed_source_system_modules,
    load_yaml,
)

# Per-variant overrides for contextualization modules (None → use instance_space).
CONTEXTUALIZATION_VARIABLES: dict[str, dict[str, dict]] = {
    "isa_manufacturing_extension": {
        "cdf_entity_matching": {
            "schemaSpace": "sp_isa_manufacturing",
            "assetInstanceSpace": None,
            "timeseriesInstanceSpace": None,
            "AssetViewExternalId": "ISAAsset",
            "TimeSeriesViewExternalId": "ISATimeSeries",
            "targetViewExternalId": "ISAAsset",
            "entityViewExternalId": "ISATimeSeries",
            "targetViewSearchProperty": "name",
            "targetViewFilterValues": [],
            "entityViewSearchProperty": "name",
            "entityViewFilterValues": [],
            "reservedWordPrefix": "",
        },
        "cdf_file_annotation": {
            "fileSchemaSpace": "sp_isa_manufacturing",
            "fileInstanceSpace": None,
            "fileExternalId": "ISAFile",
            "targetEntitySchemaSpace": "sp_isa_manufacturing",
            "targetEntityInstanceSpace": None,
            "targetEntityExternalId": "ISAAsset",
        },
    },
    "cfihos_oil_and_gas_extension": {
        "cdf_entity_matching": {
            "schemaSpace": "dm_dom_oil_and_gas",
            "assetInstanceSpace": None,
            "timeseriesInstanceSpace": None,
            "AssetViewExternalId": "FunctionalLocation",
            "TimeSeriesViewExternalId": "TimeSeriesData",
            "targetViewExternalId": "FunctionalLocation",
            "entityViewExternalId": "TimeSeriesData",
            "targetViewSearchProperty": "name",
            "targetViewFilterValues": [],
            "entityViewSearchProperty": "name",
            "entityViewFilterValues": [],
            "reservedWordPrefix": "",
        },
        "cdf_file_annotation": {
            "fileSchemaSpace": "dm_dom_oil_and_gas",
            "fileInstanceSpace": None,
            "fileExternalId": "Files",
            "targetEntitySchemaSpace": "dm_dom_oil_and_gas",
            "targetEntityInstanceSpace": None,
            "targetEntityExternalId": "FunctionalLocation",
        },
    },
}

INGESTION_FOUNDATION_VARIABLES: dict[str, dict[str, str]] = {
    "isa_manufacturing_extension": {
        "dataModelVariant": "isa_manufacturing_extension",
        "isaSchemaSpace": "sp_isa_manufacturing",
        "instanceSpace": "sp_isa_instance_space",
    },
    "cfihos_oil_and_gas_extension": {
        "dataModelVariant": "cfihos_oil_and_gas_extension",
        "isaSchemaSpace": "dm_dom_oil_and_gas",
        "instanceSpace": "sp_cfihos_instance_space",
    },
}

DATA_MODELS_MODULE_VARIABLES: dict[str, dict[str, str]] = {
    "isa_manufacturing_extension": {
        "isaSchemaSpace": "sp_isa_manufacturing",
        "isaInstanceSpace": "sp_isa_instance_space",
    },
    "cfihos_oil_and_gas_extension": {
        "isaSchemaSpace": "dm_dom_oil_and_gas",
        "isaInstanceSpace": "sp_cfihos_instance_space",
    },
}

def resolve_contextualization_variables(
    variant: str, instance_space: str
) -> dict[str, dict]:
    templates = CONTEXTUALIZATION_VARIABLES[variant]
    return {
        module: {
            key: (instance_space if value is None else value)
            for key, value in overrides.items()
        }
        for module, overrides in templates.items()
    }


def resolve_sourcesystem_variables(
    instance_space: str, installed_modules: list[str]
) -> dict[str, dict]:
    """instanceSpace for each installed source system module only."""
    return {
        module: {"instanceSpace": instance_space}
        for module in installed_modules
    }


def build_overlay(variant: str, enabled_sources: dict[str, bool]) -> dict:
    ingestion = INGESTION_FOUNDATION_VARIABLES[variant]
    instance_space = ingestion["instanceSpace"]
    installed_modules = list_installed_source_system_modules()

    return {
        "variables": {
            "modules": {
                "common": {
                    "cdf_ingestion_foundation": {
                        **ingestion,
                        "enabledSources": enabled_sources,
                    },
                },
                "contextualization": resolve_contextualization_variables(
                    variant, instance_space
                ),
                "sourcesystem": resolve_sourcesystem_variables(
                    instance_space, installed_modules
                ),
                "data_models": {
                    variant: DATA_MODELS_MODULE_VARIABLES[variant],
                },
            },
        },
    }


def collect_expected(variant: str, enabled_sources: dict[str, bool]) -> dict:
    """Flat map of dotted paths → expected values for --check."""
    overlay = build_overlay(variant, enabled_sources)
    modules = overlay["variables"]["modules"]
    expected: dict[str, object] = {}

    for key, value in modules["common"]["cdf_ingestion_foundation"].items():
        expected[f"common.cdf_ingestion_foundation.{key}"] = value

    for module, overrides in modules["contextualization"].items():
        for key, value in overrides.items():
            expected[f"contextualization.{module}.{key}"] = value

    for module, overrides in modules["sourcesystem"].items():
        for key, value in overrides.items():
            expected[f"sourcesystem.{module}.{key}"] = value

    for key, value in modules["data_models"][variant].items():
        expected[f"data_models.{variant}.{key}"] = value

    return expected


def get_actual_value(config: dict, dotted: str) -> object:
    parts = dotted.split(".")
    node = config.get("variables", {}).get("modules", {})
    for part in parts:
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def check_config_file(
    path: Path, variant: str, enabled_sources: dict[str, bool]
) -> list[str]:
    config = load_yaml(path)
    errors: list[str] = []
    for dotted, expected_value in collect_expected(variant, enabled_sources).items():
        actual = get_actual_value(config, dotted)
        if actual != expected_value:
            errors.append(f"    {dotted}: got {actual!r}, expected {expected_value!r}")
    return errors


def print_summary(
    variant: str,
    data_models_dir: Path,
    sourcesystem_dir: Path,
    pack_root: Path,
    enabled_sources: dict[str, bool],
) -> None:
    overlay = build_overlay(variant, enabled_sources)
    ingestion = overlay["variables"]["modules"]["common"]["cdf_ingestion_foundation"]
    print(f"\n  Detected data model  : {variant}")
    print(f"  Data models path     : {data_models_dir.relative_to(REPO_ROOT)}")
    print(f"  Source systems path  : {sourcesystem_dir.relative_to(REPO_ROOT)}")
    print(f"  Pack root            : {pack_root.relative_to(REPO_ROOT)}")
    print(f"  dataModelVariant     : {ingestion['dataModelVariant']}")
    print(f"  isaSchemaSpace       : {ingestion['isaSchemaSpace']}")
    print(f"  instanceSpace        : {ingestion['instanceSpace']}")
    print(f"  enabledSources       : {enabled_sources}")

    print("\n  Contextualization (variables.modules.contextualization):")
    for module, overrides in overlay["variables"]["modules"]["contextualization"].items():
        print(f"\n    [{module}]")
        for key, value in overrides.items():
            print(f"      {key}: {value!r}")

    print("\n  Source systems (variables.modules.sourcesystem):")
    for module, overrides in overlay["variables"]["modules"]["sourcesystem"].items():
        print(f"    {module}.instanceSpace: {overrides['instanceSpace']!r}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="CI mode: exit 1 if any discovered config file is out of sync",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt and apply immediately",
    )
    args = parser.parse_args()

    pack_root = get_pack_root()
    data_models_dir = get_data_models_dir()
    sourcesystem_dir = get_sourcesystem_dir()
    variant = detect_data_model_variant(data_models_dir)
    enabled_sources = detect_enabled_sources()

    if not any(enabled_sources.values()):
        print(
            f"WARNING: No source system modules found under {sourcesystem_dir}\n"
            f"  Expected subdirectories: {', '.join(SOURCE_SYSTEM_DIR_TO_ENABLED_KEY)}"
        )

    if variant not in CONTEXTUALIZATION_VARIABLES:
        print(f"ERROR: No variable template for variant '{variant}'.")
        sys.exit(1)

    config_files = find_env_configs()
    if not config_files:
        print(
            "WARNING: No config.<env>.yaml files found.\n"
            f"  Searched pack root: {pack_root}"
        )
        sys.exit(0)

    if args.check:
        all_errors: dict[str, list[str]] = {}
        for path in config_files:
            errs = check_config_file(path, variant, enabled_sources)
            if errs:
                all_errors[path.name] = errs

        if all_errors:
            print(
                f"ERROR: Config file(s) out of sync with detected model '{variant}':\n"
            )
            for filename, errs in all_errors.items():
                print(f"  {filename}")
                for e in errs:
                    print(e)
            print(
                "\n  Run: python scripts/configure_datamodel.py -y"
            )
            sys.exit(1)

        print(
            f"OK: All {len(config_files)} config file(s) match '{variant}'."
        )
        return

    print_summary(variant, data_models_dir, sourcesystem_dir, pack_root, enabled_sources)
    print(f"  Config files to update ({len(config_files)}):")
    for p in config_files:
        print(f"    {p.relative_to(REPO_ROOT)}")
    print()

    if not args.yes:
        try:
            answer = (
                input("  Apply these overrides to all config files above? [y/N] ")
                .strip()
                .lower()
            )
        except EOFError:
            answer = "n"
        if answer not in ("y", "yes"):
            print("  Aborted — no changes written.")
            sys.exit(0)

    overlay = build_overlay(variant, enabled_sources)
    for path in config_files:
        existing = load_yaml(path)
        updated = deep_merge(existing, overlay)
        new_yaml = yaml.dump(
            updated, sort_keys=False, allow_unicode=True, default_flow_style=False
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = path.with_name(f"{path.stem}.yaml.bak.{timestamp}")
        shutil.copy2(path, backup_path)

        path.write_text(new_yaml)
        print(f"  Updated : {path.relative_to(REPO_ROOT)}  (backup: {backup_path.name})")

    print(f"\n  Done — {len(config_files)} file(s) updated for variant={variant}")
    print("  Next: python scripts/build_workflow.py")


if __name__ == "__main__":
    main()
