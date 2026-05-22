"""
Sync data-model variable overrides into config.<env>.yaml for the chosen dataModelVariant (from default.config.yaml), without editing module defaults.
Updates contextualization and source systems modules. Discovers configs under <org-dir>/ or repo root (CDF Toolkit order).

Usage:python configure_datamodel.py [-y] [--check]
Variants: isa_manufacturing_extension | cfihos_oil_and_gas
"""

import argparse
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

import yaml

MODULE_ROOT      = Path(__file__).parent.parent
INGESTION_CONFIG = MODULE_ROOT / "default.config.yaml"
REPO_ROOT        = MODULE_ROOT.parent.parent.parent  # scripts/ -> module/ -> modules/ -> repo/

ACCELERATOR_VARIABLES: dict[str, dict[str, dict]] = {
    "isa_manufacturing_extension": {
        "cdf_entity_matching": {
            "schemaSpace":              "sp_isa_manufacturing",
            "assetInstanceSpace":       None,
            "timeseriesInstanceSpace":  None,
            "AssetViewExternalId":      "ISAAsset",
            "TimeSeriesViewExternalId": "ISATimeSeries",
            "targetViewExternalId":     "ISAAsset",
            "entityViewExternalId":     "ISATimeSeries",
            "targetViewSearchProperty": "name",
            "targetViewFilterValues":   [],
            "entityViewSearchProperty": "name",
            "entityViewFilterValues":   [],
            "reservedWordPrefix":       "",
        },
        "cdf_file_annotation": {
            "fileSchemaSpace":           "sp_isa_manufacturing",
            "fileInstanceSpace":         None,
            "fileExternalId":            "ISAFile",
            "targetEntitySchemaSpace":   "sp_isa_manufacturing",
            "targetEntityInstanceSpace": None,
            "targetEntityExternalId":    "ISAAsset",
        },
    },
    "cfihos_oil_and_gas": {
        "cdf_entity_matching": {
            "schemaSpace":              "dm_dom_oil_and_gas",
            "assetInstanceSpace":       None,
            "timeseriesInstanceSpace":  None,
            "AssetViewExternalId":      "FunctionalLocation",
            "TimeSeriesViewExternalId": "TimeSeriesData",
            "targetViewExternalId":     "FunctionalLocation",
            "entityViewExternalId":     "TimeSeriesData",
            "targetViewSearchProperty": "name",
            "targetViewFilterValues":   [],
            "entityViewSearchProperty": "name",
            "entityViewFilterValues":   [],
            "reservedWordPrefix":       "",
        },
        "cdf_file_annotation": {
            "fileSchemaSpace":           "dm_dom_oil_and_gas",
            "fileInstanceSpace":         None,
            "fileExternalId":            "Files",
            "targetEntitySchemaSpace":   "dm_dom_oil_and_gas",
            "targetEntityInstanceSpace": None,
            "targetEntityExternalId":    "FunctionalLocation",
        },
    },
}


SOURCE_SYSTEM_MODULES = [
    "cdf_pi_foundation",
    "cdf_sap_foundation",
    "cdf_opcua_foundation",
    "cdf_files_foundation",
]


def load_ingestion_config() -> dict:
    return yaml.safe_load(INGESTION_CONFIG.read_text())


def get_org_dir(repo_root: Path) -> str | None:
    """Read organization_dir from cdf.toml if present (mirrors CDF Toolkit lookup)."""
    toml_path = repo_root / "cdf.toml"
    if not toml_path.exists():
        return None
    content = toml_path.read_text()
    m = re.search(r"""organization_dir\s*=\s*["']([^"']+)["']""", content)
    return m.group(1) if m else None


def find_env_configs(repo_root: Path) -> list[Path]:
    """
    Discover all config.<env>.yaml files in the same order the CDF Toolkit uses:
      1. <repo-root>/<org-dir>/config.*.yaml  (if org-dir is set in cdf.toml)
      2. <repo-root>/config.*.yaml
    Backup files (*.bak.*) are excluded.
    """
    found: list[Path] = []
    org_dir = get_org_dir(repo_root)
    search_dirs: list[Path] = []
    if org_dir:
        search_dirs.append(repo_root / org_dir)
    search_dirs.append(repo_root)

    for search_dir in search_dirs:
        for path in sorted(search_dir.glob("config.*.yaml")):
            if ".bak." not in path.name:
                found.append(path)
    return found


def resolve_accelerator_variables(variant: str, instance_space: str) -> dict[str, dict]:
    """Resolve accelerator overrides, substituting instance_space for None values."""
    return {
        module: {
            key: (instance_space if value is None else value)
            for key, value in overrides.items()
        }
        for module, overrides in ACCELERATOR_VARIABLES[variant].items()
    }


def resolve_sourcesystem_variables(instance_space: str) -> dict[str, dict]:
    """Return instanceSpace override for each source system module."""
    return {module: {"instanceSpace": instance_space} for module in SOURCE_SYSTEM_MODULES}


def _deep_merge(base: dict, overlay: dict) -> dict:
    """Recursively merge overlay into base (overlay wins on conflicts)."""
    result = dict(base)
    for key, value in overlay.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def build_updated_config(
    existing: dict,
    accelerator_vars: dict[str, dict],
    sourcesystem_vars: dict[str, dict],
) -> dict:
    """
    Merge all overrides into the existing config using the CDF Toolkit nested path:
        variables.modules.accelerators.contextualization.<module>
        variables.modules.sourcesystem.<module>
    """
    overlay = {
        "variables": {
            "modules": {
                "accelerators": {
                    "contextualization": accelerator_vars,
                },
                "sourcesystem": sourcesystem_vars,
            }
        }
    }
    return _deep_merge(existing, overlay)


def check_config(
    path: Path,
    accelerator_vars: dict[str, dict],
    sourcesystem_vars: dict[str, dict],
    variant: str,
) -> list[str]:
    """Return error strings for any out-of-sync values, or empty list if all OK."""
    config = yaml.safe_load(path.read_text()) or {}
    modules = config.get("variables", {}).get("modules", {})

    ctx = modules.get("accelerators", {}).get("contextualization", {})
    src = modules.get("sourcesystem", {})

    errors: list[str] = []

    for module, expected in accelerator_vars.items():
        actual = ctx.get(module, {})
        for key, expected_value in expected.items():
            if actual.get(key) != expected_value:
                errors.append(
                    f"    accelerators.contextualization.{module}.{key}: "
                    f"got {actual.get(key)!r}, expected {expected_value!r}"
                )

    for module, expected in sourcesystem_vars.items():
        actual = src.get(module, {})
        for key, expected_value in expected.items():
            if actual.get(key) != expected_value:
                errors.append(
                    f"    sourcesystem.{module}.{key}: "
                    f"got {actual.get(key)!r}, expected {expected_value!r}"
                )

    return errors


def print_summary(
    accelerator_vars: dict[str, dict],
    sourcesystem_vars: dict[str, dict],
    variant: str,
) -> None:
    print(f"\n  Data model variant : {variant}")
    print("\n  Accelerator overrides (variables.modules.accelerators.contextualization):")
    for module, overrides in accelerator_vars.items():
        print(f"\n    [{module}]")
        for key, value in overrides.items():
            print(f"      {key}: {value!r}")
    print("\n  Source system overrides (variables.modules.sourcesystem):")
    for module, overrides in sourcesystem_vars.items():
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
        "--yes", "-y",
        action="store_true",
        help="Skip confirmation prompt and apply immediately",
    )
    args = parser.parse_args()

    ingestion_cfg  = load_ingestion_config()
    variant        = ingestion_cfg.get("dataModelVariant", "isa_manufacturing_extension")
    instance_space = ingestion_cfg.get("instanceSpace", "sp_isa_instance_space")

    if variant not in ACCELERATOR_VARIABLES:
        print(
            f"ERROR: Unknown dataModelVariant '{variant}'.\n"
            f"  Supported variants: {list(ACCELERATOR_VARIABLES.keys())}"
        )
        sys.exit(1)

    config_files = find_env_configs(REPO_ROOT)
    if not config_files:
        print(
            "WARNING: No config.<env>.yaml files found under the repository root.\n"
            f"  Searched: {REPO_ROOT}"
        )
        sys.exit(0)

    accelerator_vars   = resolve_accelerator_variables(variant, instance_space)
    sourcesystem_vars  = resolve_sourcesystem_variables(instance_space)

    # ── CI / check mode ────────────────────────────────────────────────────────
    if args.check:
        all_errors: dict[str, list[str]] = {}
        for path in config_files:
            errs = check_config(path, accelerator_vars, sourcesystem_vars, variant)
            if errs:
                all_errors[path.name] = errs

        if all_errors:
            print(
                f"ERROR: The following config files are out of sync with "
                f"dataModelVariant='{variant}':\n"
            )
            for filename, errs in all_errors.items():
                print(f"  {filename}")
                for e in errs:
                    print(e)
            print("\n  Run `python scripts/configure_datamodel.py` to fix.")
            sys.exit(1)

        print(
            f"OK: All {len(config_files)} config file(s) are aligned with "
            f"dataModelVariant='{variant}'."
        )
        return

    # ── Apply mode ─────────────────────────────────────────────────────────────
    print_summary(accelerator_vars, sourcesystem_vars, variant)
    print(f"  Config files found ({len(config_files)}):")
    for p in config_files:
        print(f"    {p.relative_to(REPO_ROOT)}")
    print()

    if not args.yes:
        try:
            answer = input("  Apply these overrides to all config files above? [y/N] ").strip().lower()
        except EOFError:
            answer = "n"
        if answer not in ("y", "yes"):
            print("  Aborted — no changes written.")
            sys.exit(0)

    for path in config_files:
        existing = yaml.safe_load(path.read_text()) or {}
        updated  = build_updated_config(existing, accelerator_vars, sourcesystem_vars)
        new_yaml = yaml.dump(updated, sort_keys=False, allow_unicode=True, default_flow_style=False)

        timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = path.with_name(f"{path.stem}.yaml.bak.{timestamp}")
        shutil.copy2(path, backup_path)

        path.write_text(new_yaml)
        print(f"  Updated : {path.relative_to(REPO_ROOT)}  (backup: {backup_path.name})")

    print(f"\n  Done — {len(config_files)} file(s) updated.  variant={variant}")


if __name__ == "__main__":
    main()
