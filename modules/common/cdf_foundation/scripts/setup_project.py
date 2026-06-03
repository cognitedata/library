"""
Project setup for the dp:foundation deployment pack.

Aligned with the project-setup SOP. Creates and synchronises the per-environment
Toolkit config files for the three mandatory environments (SOP Step 1) and keeps
their data-model-driven variables and persona access-group names consistent.

Secrets are NEVER written here. Group `sourceId` values are Entra
ID object IDs, not secrets, and are left for the operator to fill in per
environment. Any credential is referenced via ${ENV_VAR} / Key Vault only.

Idempotent. A timestamped `.bak` of every existing config file is written
before it is modified.

Usage:
    python setup_project.py [-y] [--check] [--variant VARIANT] [--site SITE]
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

import yaml
from _pack_config import (
    CONTEXTUALIZATION_REDUNDANT_AUTH,
    KNOWN_DATA_MODEL_DIRS,
    deep_merge,
    detect_data_model_variant,
    find_env_configs,
    get_contextualization_dir,
    get_data_models_dir,
    get_pack_root,
    list_installed_contextualization_modules,
    list_installed_source_system_modules,
    load_yaml,
)

# Environments managed by this script (SOP Step 1). staging == test.
ENVIRONMENTS: tuple[str, ...] = ("dev", "test", "prod")

# Access-group environment suffix (SOP Step 3b): "dev" covers dev + test.
GROUP_ENV: dict[str, str] = {"dev": "dev", "test": "dev", "prod": "prod"}

# Toolkit environment validation-type per environment.
ENVIRONMENT_VALIDATION_TYPE: dict[str, str] = {"dev": "dev", "test": "dev", "prod": "prod"}

PERSONAS: tuple[str, ...] = ("consumer", "producer", "admin")

# ── Data-model-driven variable maps (per variant) ─────────────────────────────

INGESTION_FOUNDATION_VARIABLES: dict[str, dict[str, str]] = {
    "isa_manufacturing_extension": {
        "dataModelVariant": "isa_manufacturing_extension",
        "schemaSpace": "sp_isa_manufacturing",
        "instanceSpace": "sp_isa_instance_space",
    },
    "cfihos_oil_and_gas_extension": {
        "dataModelVariant": "cfihos_oil_and_gas_extension",
        "schemaSpace": "dm_dom_oil_and_gas",
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

# Per-variant overrides for contextualization modules.
# None values are sentinels meaning "use the variant's instanceSpace at runtime"
# (resolved by resolve_contextualization_variables before writing to config).
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


def group_name(persona: str, site: str, env: str) -> str:
    """SOP Step 3b: <persona>[-<site>]-<env>; env is 'dev' (dev+test) or 'prod'."""
    segments = [persona] + ([site] if site else []) + [GROUP_ENV[env]]
    return "-".join(segments)


def resolve_contextualization_variables(variant: str, instance_space: str) -> dict[str, dict]:
    templates = CONTEXTUALIZATION_VARIABLES[variant]
    return {
        module: {
            key: (instance_space if value is None else value)
            for key, value in overrides.items()
        }
        for module, overrides in templates.items()
    }


def resolve_sourcesystem_variables(instance_space: str, installed_modules: list[str]) -> dict[str, dict]:
    """instanceSpace for each installed source system module only."""
    return {module: {"instanceSpace": instance_space} for module in installed_modules}


def build_foundation_vars(variant: str, env: str, site: str) -> dict:
    """variables.modules.common.cdf_foundation for a given env."""
    ingestion = dict(INGESTION_FOUNDATION_VARIABLES[variant])
    ingestion["site"] = site
    for persona in PERSONAS:
        ingestion[f"{persona}GroupName"] = group_name(persona, site, env)
    return ingestion


def build_overlay(variant: str, env: str, site: str, repo_root: Path | None = None) -> dict:
    """Variable overlay merged into config.<env>.yaml."""
    instance_space = INGESTION_FOUNDATION_VARIABLES[variant]["instanceSpace"]
    installed_modules = list_installed_source_system_modules(repo_root)
    return {
        "variables": {
            "modules": {
                "common": {
                    "cdf_foundation": build_foundation_vars(variant, env, site),
                },
                "contextualization": resolve_contextualization_variables(variant, instance_space),
                "sourcesystem": resolve_sourcesystem_variables(instance_space, installed_modules),
                "data_models": {variant: DATA_MODELS_MODULE_VARIABLES[variant]},
            },
        },
    }


def skeleton_config(env: str) -> dict:
    """Minimal Toolkit config.<env>.yaml created when one does not yet exist."""
    return {
        "environment": {
            "name": env,
            "project": "${CDF_PROJECT}",
            "validation-type": ENVIRONMENT_VALIDATION_TYPE.get(env, "dev"),
            "selected": ["modules"],
        },
        "variables": {"modules": {}},
    }


def config_path(pack_root: Path, env: str, site: str) -> Path:
    name = f"config.{env}.{site}.yaml" if site else f"config.{env}.yaml"
    return pack_root / name


def target_config_paths(pack_root: Path, site: str) -> dict[str, Path]:
    return {env: config_path(pack_root, env, site) for env in ENVIRONMENTS}


def collect_expected(variant: str, env: str, site: str) -> dict[str, object]:
    """Flat map of dotted variable paths → expected values for --check."""
    overlay = build_overlay(variant, env, site)
    modules = overlay["variables"]["modules"]
    expected: dict[str, object] = {}
    for key, value in modules["common"]["cdf_foundation"].items():
        expected[f"common.cdf_foundation.{key}"] = value
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
    node = config.get("variables", {}).get("modules", {})
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def check_config_file(path: Path, variant: str, env: str, site: str) -> list[str]:
    if not path.exists():
        return ["    (file missing — run without --check to create it)"]
    config = load_yaml(path)
    errors: list[str] = []
    for dotted, expected_value in collect_expected(variant, env, site).items():
        actual = get_actual_value(config, dotted)
        if actual != expected_value:
            errors.append(f"    {dotted}: got {actual!r}, expected {expected_value!r}")
    return errors


def resolve_variant(args_variant: str | None, data_models_dir: Path) -> str:
    if args_variant:
        if args_variant not in INGESTION_FOUNDATION_VARIABLES:
            raise SystemExit(
                f"ERROR: Unknown --variant '{args_variant}'.\n"
                f"  Supported: {', '.join(KNOWN_DATA_MODEL_DIRS)}"
            )
        return args_variant
    return detect_data_model_variant(data_models_dir)


def print_summary(variant: str, site: str, pack_root: Path, targets: dict[str, Path]) -> None:
    ingestion = INGESTION_FOUNDATION_VARIABLES[variant]
    print(f"\n  Data model variant   : {variant}")
    print(f"  Pack root            : {pack_root}")
    print(f"  Site segment         : {site or '(none)'}")
    print(f"  schemaSpace          : {ingestion['schemaSpace']}")
    print(f"  instanceSpace        : {ingestion['instanceSpace']}")
    print("\n  Environments / access group names:")
    for env, path in targets.items():
        state = "update" if path.exists() else "create"
        names = ", ".join(group_name(p, site, env) for p in PERSONAS)
        print(f"    {env:<6} {state:<6} {path.name:<26} groups: {names}")
    print()


def write_config(path: Path, env: str, overlay: dict) -> bool:
    """Create or update a config file. Returns True if it changed."""
    if path.exists():
        existing = load_yaml(path)
        updated = deep_merge(existing, overlay)
        if updated == existing:
            return False
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = path.with_suffix(f".{timestamp}.bak")
        shutil.copy2(path, backup)
        print(f"  Updated : {path.name}  (backup: {backup.name})")
    else:
        updated = deep_merge(skeleton_config(env), overlay)
        print(f"  Created : {path.name}")
    path.write_text(
        yaml.dump(updated, sort_keys=False, allow_unicode=True, default_flow_style=False)
    )
    return True


def remove_redundant_auth_files(repo_root: Path | None = None) -> list[Path]:
    """
    Remove auth group files from contextualization modules that are covered by
    cdf_foundation. These files are only redundant when cdf_foundation is present
    in the same deployment pack; in standalone use those modules keep their own auth.
    Returns the list of files actually removed.
    """
    ctx_dir = get_contextualization_dir(repo_root)
    removed: list[Path] = []
    for module_dir in list_installed_contextualization_modules(repo_root):
        for rel_path in CONTEXTUALIZATION_REDUNDANT_AUTH[module_dir]:
            auth_file = ctx_dir / module_dir / rel_path
            if auth_file.exists():
                auth_file.unlink()
                removed.append(auth_file)
                print(f"  Removed : {auth_file.relative_to(ctx_dir.parent.parent)}")
    return removed


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="CI mode: exit 1 if any target config (dev/test/prod) is out of sync",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip the confirmation prompt and apply immediately",
    )
    parser.add_argument(
        "--variant",
        choices=list(KNOWN_DATA_MODEL_DIRS),
        default=None,
        help="Force the data model variant instead of auto-detecting it",
    )
    parser.add_argument(
        "--site",
        default="",
        help="Optional site segment for group names and config.<env>.<site>.yaml",
    )
    args = parser.parse_args()

    site = args.site.strip()
    if site and not re.fullmatch(r"[a-z0-9_-]+", site):
        raise SystemExit(
            f"ERROR: --site '{site}' contains invalid characters.\n"
            "  Only lowercase letters, digits, hyphens, and underscores are allowed."
        )
    pack_root = get_pack_root()
    data_models_dir = get_data_models_dir()
    variant = resolve_variant(args.variant, data_models_dir)
    targets = target_config_paths(pack_root, site)

    if args.check:
        all_errors: dict[str, list[str]] = {}
        for env, path in targets.items():
            errs = check_config_file(path, variant, env, site)
            if errs:
                all_errors[path.name] = errs
        # Flag any other discovered config files that drift on data-model vars.
        target_set = set(targets.values())
        for path in find_env_configs():
            if path in target_set:
                continue
            env_guess = path.name.split(".")[1] if "." in path.name else "dev"
            env_guess = env_guess if env_guess in ENVIRONMENTS else "dev"
            errs = check_config_file(path, variant, env_guess, site)
            if errs:
                all_errors[path.name] = errs

        # Also check for redundant auth files that should have been removed.
        ctx_dir = get_contextualization_dir()
        stale_auth: list[Path] = []
        for module_dir in list_installed_contextualization_modules():
            for rel_path in CONTEXTUALIZATION_REDUNDANT_AUTH[module_dir]:
                auth_file = ctx_dir / module_dir / rel_path
                if auth_file.exists():
                    stale_auth.append(auth_file)

        if all_errors:
            print(f"ERROR: Config file(s) out of sync with variant '{variant}':\n")
            for filename, errs in all_errors.items():
                print(f"  {filename}")
                for e in errs:
                    print(e)
            print("\n  Run: python scripts/setup_project.py -y")
            sys.exit(1)
        if stale_auth:
            print("ERROR: Redundant auth file(s) still present (covered by cdf_foundation):")
            for p in stale_auth:
                print(f"  {p.relative_to(ctx_dir.parent.parent)}")
            print("\n  Run: python scripts/setup_project.py -y")
            sys.exit(1)
        print(f"OK: All config file(s) match variant '{variant}'. No stale auth files.")
        return

    print_summary(variant, site, pack_root, targets)

    if not args.yes:
        try:
            answer = input("  Create/update these config files? [y/N] ").strip().lower()
        except EOFError:
            answer = "n"
        if answer not in ("y", "yes"):
            print("  Aborted — no changes written.")
            sys.exit(0)

    changed = 0
    for env, path in targets.items():
        if write_config(path, env, build_overlay(variant, env, site)):
            changed += 1

    removed = remove_redundant_auth_files()

    print(f"\n  Done — {changed} config file(s) created/updated, {len(removed)} redundant auth file(s) removed.")
    print("  Next: fill in project name, group sourceIds (Entra ID object IDs),")
    print("  and CI/CD secrets (Key Vault / CI secret store — never in the repo).")


if __name__ == "__main__":
    main()
