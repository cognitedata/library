"""
Interactive setup wizard for the dp:foundation deployment pack.

Aligned with the project-setup SOP. Creates and synchronises the per-environment
Toolkit config files for the selected environments and keeps their data-model-driven
variables and persona access-group names consistent.

Secrets are NEVER written to config files.  Group ``sourceId`` values are Entra
ID object IDs stored only in ``.env`` and referenced via ``${ENV_VAR}`` in YAML.

Idempotent: a timestamped ``.bak`` of every existing config file is written
before it is modified.  Existing comments and blank lines are preserved when
updating a file in-place (``_yaml_patch`` helpers).

Usage:
    python setup_project.py [-y] [--check] [--variant VARIANT] [--site SITE]
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from _env_io import parse_env_file, upsert_env  # noqa: F401 (re-exported for tests)
from _pack_config import (
    CONTEXTUALIZATION_REDUNDANT_AUTH,
    KNOWN_DATA_MODEL_DIRS,
    REPO_ROOT,
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
from _prompts import prompt, prompt_choice, prompt_env_var, prompt_yes_no
from _style import ChangeRecord, _banner, _hint, _ok, _section, _show_changes_table, _warn
from _yaml_patch import delete_key as _yaml_delete_key
from _yaml_patch import find_line as _yaml_find_line
from _yaml_patch import insert_key as _yaml_insert_key
from _yaml_patch import set_value as _yaml_set_value

# ── Environment / persona constants ───────────────────────────────────────────

ENVIRONMENTS: tuple[str, ...] = ("dev", "test", "prod")

# Access-group environment suffix: "dev" covers both dev and test environments.
GROUP_ENV: dict[str, str] = {"dev": "dev", "test": "dev", "prod": "prod"}

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
# ``None`` values are sentinels resolved to the variant's ``instanceSpace``
# at runtime by ``resolve_contextualization_variables``.
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

# Keys that are stale in an existing config when cdf_project_foundation is
# present (foundation covers these capabilities; standalone modules added them
# before foundation was installed).
_STALE_CTX_KEYS: tuple[str, ...] = (
    "variables.modules.contextualization.cdf_file_annotation.groupSourceId",
    "variables.modules.contextualization.cdf_entity_matching"
    ".entity_matching_processing_group_source_id",
)

# ── Domain helpers ─────────────────────────────────────────────────────────────

def group_name(persona: str, site: str, env: str) -> str:
    """SOP Step 3b: ``<persona>[-<site>]-<env>``; env is 'dev' (dev+test) or 'prod'."""
    segments = [persona] + ([site] if site else []) + [GROUP_ENV[env]]
    return "-".join(segments)


def resolve_contextualization_variables(
    variant: str,
    instance_space: str,
    installed_ctx: list[str],
) -> dict[str, dict]:
    """Build ctx variable overrides only for modules that are actually installed."""
    templates = CONTEXTUALIZATION_VARIABLES[variant]
    result: dict[str, dict] = {}
    for module, overrides in templates.items():
        if module not in installed_ctx:
            continue
        result[module] = {
            key: (instance_space if value is None else value)
            for key, value in overrides.items()
        }
    return result


def resolve_sourcesystem_variables(
    instance_space: str,
    installed_modules: list[str],
) -> dict[str, dict]:
    return {module: {"instanceSpace": instance_space} for module in installed_modules}


def build_foundation_vars(variant: str, env: str, site: str) -> dict:
    """Variables block for ``variables.modules.common.cdf_project_foundation``."""
    ingestion = dict(INGESTION_FOUNDATION_VARIABLES[variant])
    ingestion["site"] = site
    for persona in PERSONAS:
        ingestion[f"{persona}GroupName"] = group_name(persona, site, env)
        ingestion[f"{persona}SourceId"] = f"${{{persona.upper()}_SOURCE_ID}}"
    return ingestion


def build_overlay(
    variant: str,
    env: str,
    site: str,
    installed_ctx: list[str],
    app_owner: str = "",
    repo_root: Path | None = None,
) -> dict:
    """Full ``variables.modules`` overlay dict to merge into a config file."""
    instance_space = INGESTION_FOUNDATION_VARIABLES[variant]["instanceSpace"]
    installed_ss = list_installed_source_system_modules(repo_root)
    ctx_vars = resolve_contextualization_variables(variant, instance_space, installed_ctx)

    if app_owner and "cdf_file_annotation" in ctx_vars:
        ctx_vars["cdf_file_annotation"]["ApplicationOwner"] = app_owner
    # site doubles as location_name for entity matching (same concept).
    if site and "cdf_entity_matching" in ctx_vars:
        ctx_vars["cdf_entity_matching"]["location_name"] = site

    modules_vars: dict[str, Any] = {
        "common": {
            "cdf_project_foundation": build_foundation_vars(variant, env, site),
        },
    }
    if ctx_vars:
        modules_vars["contextualization"] = ctx_vars
    if installed_ss:
        modules_vars["sourcesystem"] = resolve_sourcesystem_variables(instance_space, installed_ss)
    modules_vars["data_models"] = {variant: DATA_MODELS_MODULE_VARIABLES[variant]}

    return {"variables": {"modules": modules_vars}}


def config_path(pack_root: Path, env: str) -> Path:
    """Config files are always ``config.<env>.yaml`` — site is for group names only."""
    return pack_root / f"config.{env}.yaml"


def target_config_paths(pack_root: Path, envs: tuple[str, ...]) -> dict[str, Path]:
    return {env: config_path(pack_root, env) for env in envs}


def resolve_variant(args_variant: str | None, data_models_dir: Path) -> str:
    if args_variant:
        if args_variant not in INGESTION_FOUNDATION_VARIABLES:
            raise SystemExit(
                f"ERROR: Unknown --variant '{args_variant}'.\n"
                f"  Supported: {', '.join(KNOWN_DATA_MODEL_DIRS)}"
            )
        return args_variant
    return detect_data_model_variant(data_models_dir)


# ── Config file writers ────────────────────────────────────────────────────────

_YAML_HEADER = (
    "# Managed by modules/common/cdf_project_foundation/scripts/setup_project.py\n"
    "# Re-run the wizard to refresh, or use --check in CI.\n"
)


def _skeleton_config(env: str, project: str) -> dict:
    return {
        "environment": {
            "name": env,
            "project": project,
            "validation-type": ENVIRONMENT_VALIDATION_TYPE.get(env, "dev"),
            "selected": ["modules"],
        },
        "variables": {"modules": {}},
    }


def _write_config_fresh(path: Path, env: str, project: str, overlay: dict) -> None:
    """Create a brand-new config file from the skeleton + overlay."""
    merged = deep_merge(_skeleton_config(env, project), overlay)
    path.write_text(
        _YAML_HEADER
        + yaml.dump(merged, sort_keys=False, allow_unicode=True, default_flow_style=False)
    )
    _ok(f"Created  {path.name}")


def _write_config_update(path: Path, project: str, overlay: dict) -> bool:
    """Update an existing config file in-place, preserving comments and blank lines.

    Returns ``True`` when at least one value changed.
    """
    lines = path.read_text().splitlines(keepends=True)
    changed = False

    # Update environment.project.
    _, c = _yaml_set_value(lines, "environment.project", project)
    changed = changed or c

    # Update every module variable from the overlay.
    for category, cat_vars in overlay.get("variables", {}).get("modules", {}).items():
        for module, mod_vars in cat_vars.items():
            for key, val in mod_vars.items():
                dotted = f"variables.modules.{category}.{module}.{key}"
                # Use yaml.dump for all non-string types to get correct YAML
                # representation (e.g. true/false for bools, [] for empty lists).
                yaml_val = (
                    val
                    if isinstance(val, str)
                    else yaml.dump(val, default_flow_style=True).strip()
                )
                old, c = _yaml_set_value(lines, dotted, yaml_val)
                if old is None and not c:
                    # Key absent — insert it into the parent section.
                    parent = f"variables.modules.{category}.{module}"
                    if _yaml_insert_key(lines, parent, key, yaml_val):
                        c = True
                changed = changed or c

    # Remove stale standalone-module keys now covered by cdf_project_foundation.
    for stale in _STALE_CTX_KEYS:
        if _yaml_find_line(lines, stale) is not None:
            _yaml_delete_key(lines, stale)
            changed = True

    if not changed:
        return False

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup = path.with_suffix(f".{timestamp}.bak")
    shutil.copy2(path, backup)
    path.write_text("".join(lines))
    from _style import _C  # local import avoids circular dependency at module level
    _ok(f"Updated  {path.name}  {_C.DIM}(backup: {backup.name}){_C.RESET}")
    return True


def write_config(path: Path, env: str, project: str, overlay: dict) -> bool:
    """Create or update a config file. Returns ``True`` if the file changed."""
    if not path.exists():
        _write_config_fresh(path, env, project, overlay)
        return True
    return _write_config_update(path, project, overlay)


# ── Redundant auth cleanup ─────────────────────────────────────────────────────

def remove_redundant_auth_files(repo_root: Path | None = None) -> list[Path]:
    """Remove contextualization auth group files covered by cdf_project_foundation."""
    ctx_dir = get_contextualization_dir(repo_root)
    removed: list[Path] = []
    for module_dir in list_installed_contextualization_modules(repo_root):
        for rel_path in CONTEXTUALIZATION_REDUNDANT_AUTH[module_dir]:
            auth_file = ctx_dir / module_dir / rel_path
            if auth_file.exists():
                auth_file.unlink()
                removed.append(auth_file)
                _ok(f"Removed redundant auth: {auth_file.relative_to(ctx_dir.parent.parent)}")
    return removed


# ── CI/CD generation ───────────────────────────────────────────────────────────

def _run_cicd_wizard(pack_root: Path) -> None:
    _section("CI/CD Pipeline Generation")
    if not prompt_yes_no("Generate GitHub Actions workflows for this project?", default=False):
        return

    enterprise = prompt("Enterprise slug (e.g. acme — used for project names like acme-dev)")
    if not enterprise:
        _warn("No enterprise slug provided — skipping CI/CD generation.")
        return

    generate_script = Path(__file__).parent / "generate_actions.py"
    if not generate_script.exists():
        _warn(f"Could not find generate_actions.py at {generate_script} — skipping.")
        return

    cmd = [sys.executable, str(generate_script), "--enterprise", enterprise, "--force"]
    from _style import _C
    print(f"\n  {_C.DIM}Running: {' '.join(cmd)}{_C.RESET}")
    result = subprocess.run(cmd, cwd=str(REPO_ROOT))
    if result.returncode == 0:
        _ok("CI/CD workflows generated.  See docs/FOUNDATION_CICD.md for next steps.")
    else:
        _warn("CI/CD generation completed with warnings — review the output above.")


# ── Main wizard ────────────────────────────────────────────────────────────────

def _run_wizard(
    args_variant: str | None,
    args_site: str,
    args_yes: bool,
    repo_root: Path | None = None,
) -> None:
    pack_root = get_pack_root(repo_root)
    variant = resolve_variant(args_variant, get_data_models_dir(repo_root))
    installed_ctx = list_installed_contextualization_modules(repo_root)

    _banner("Foundation Deployment Pack — Project Setup")
    _ok(f"Data model variant : {variant}")
    _ok(f"Pack root          : {pack_root}")
    if installed_ctx:
        _ok(f"Contextualization  : {', '.join(installed_ctx)}")
    else:
        _hint("No contextualization modules detected.")

    # ── Environment selection ─────────────────────────────────────────────────
    _section("Environment Selection")
    print("  Which environments would you like to set up?\n")
    choice = prompt_choice(
        [
            "All three — dev, test, prod  (recommended)",
            "dev only",
            "dev + prod  (skip test / staging)",
            "Custom — choose individually",
        ],
        default=1,
    )
    if choice == 1:
        selected_envs: tuple[str, ...] = ("dev", "test", "prod")
    elif choice == 2:
        selected_envs = ("dev",)
    elif choice == 3:
        selected_envs = ("dev", "prod")
    else:
        selected_envs = tuple(
            env for env in ENVIRONMENTS
            if prompt_yes_no(f"  Include environment '{env}'?", default=True)
        )
        if not selected_envs:
            raise SystemExit("No environments selected — nothing to do.")

    # ── CDF project names ─────────────────────────────────────────────────────
    _section("CDF Project Names")
    _hint("Format: <enterprise>-<env>  e.g. acme-dev, acme-test, acme-prod")
    _hint("Only lowercase letters, digits, and hyphens. Cannot be empty.")
    project_names: dict[str, str] = {}
    for env in selected_envs:
        while True:
            val = prompt(f"Project name for {env}").strip()
            if not val:
                _warn("Project name cannot be empty.")
                continue
            if not re.fullmatch(r"[a-z0-9][a-z0-9-]*[a-z0-9]", val):
                _warn("Use only lowercase letters, digits, and hyphens (e.g. acme-dev).")
                continue
            project_names[env] = val
            break

    # ── Site / location name (optional) ──────────────────────────────────────
    _section("Site / Location Name  (optional)")
    _hint("Used as suffix in access-group names (<persona>-<site>-<env>)")
    _hint("and as location_name in entity-matching variables.")
    _hint("Leave blank to omit.")
    if args_site:
        site = args_site
        _hint(f"Using --site value: {site}")
    else:
        site = prompt("Site / location name (e.g. oslo)", default="").strip()

    if site and not re.fullmatch(r"[a-z0-9_-]+", site):
        raise SystemExit(
            f"ERROR: site '{site}' contains invalid characters.\n"
            "  Only lowercase letters, digits, hyphens, and underscores are allowed."
        )

    # ── Group source IDs → .env ───────────────────────────────────────────────
    _section("Group Source IDs  (Entra ID object IDs)")
    _hint("Stored in .env and referenced as ${CONSUMER_SOURCE_ID} etc. in config.")
    _hint("Leave blank to skip — fill .env manually later.")
    env_path = pack_root / ".env"
    env_lines, env_vals, env_key_idx = parse_env_file(env_path)
    original_env_vals = dict(env_vals)

    for persona in PERSONAS:
        var = f"{persona.upper()}_SOURCE_ID"
        print(f"\n  {persona.capitalize()} group  →  {var}")
        prompt_env_var(var, env_vals, env_lines, env_key_idx)

    # ── ApplicationOwner (file_annotation only) ───────────────────────────────
    app_owner = ""
    if "cdf_file_annotation" in installed_ctx:
        _section("Streamlit Application Owner  (file annotation)")
        _hint("Email address(es) of the Streamlit app owner for cdf_file_annotation.")
        _hint("Separate multiple addresses with a comma.")
        while True:
            app_owner = prompt("Application owner email(s)", default="").strip()
            if not app_owner:
                break  # optional — skip if blank
            emails = [e.strip() for e in app_owner.split(",") if e.strip()]
            if all(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", e) for e in emails):
                break
            _warn("One or more addresses look invalid. Use format: name@domain.com")

    # ── Pending changes ───────────────────────────────────────────────────────
    targets = target_config_paths(pack_root, selected_envs)

    _section("Pending Changes")
    for env, path in targets.items():
        overlay = build_overlay(variant, env, site, installed_ctx, app_owner, repo_root)
        state = "create" if not path.exists() else "update"
        print(f"\n  [{state}] {path.name}")
        print(f"  environment.project  →  {project_names[env]}")
        records = [
            ChangeRecord(f"{cat}.{mod}.{key}", None, val)
            for cat, cat_vars in overlay["variables"]["modules"].items()
            for mod, mod_vars in cat_vars.items()
            for key, val in mod_vars.items()
        ]
        _show_changes_table(records)

    env_dirty = any(
        env_vals.get(f"{p.upper()}_SOURCE_ID") != original_env_vals.get(f"{p.upper()}_SOURCE_ID")
        for p in PERSONAS
    )
    if env_dirty:
        print("\n  [.env]")
        for persona in PERSONAS:
            var = f"{persona.upper()}_SOURCE_ID"
            old = original_env_vals.get(var, "(not set)")
            new = env_vals.get(var, "(not set)")
            if old != new:
                masked = new[:3] + "****" if len(new) > 6 else "****"
                print(f"  {var}: {old} → {masked}")

    # ── Confirm ───────────────────────────────────────────────────────────────
    print()
    if not args_yes:
        if not prompt_yes_no("Apply these changes?", default=True):
            print("  Aborted — no changes written.")
            sys.exit(0)

    # ── Write config files ────────────────────────────────────────────────────
    _section("Writing Config Files")
    changed_count = 0
    for env, path in targets.items():
        overlay = build_overlay(variant, env, site, installed_ctx, app_owner, repo_root)
        if write_config(path, env, project_names[env], overlay):
            changed_count += 1
        else:
            _hint(f"No change: {path.name}")

    # ── Write .env ────────────────────────────────────────────────────────────
    if env_dirty and env_lines:
        _section("Writing .env")
        if env_path.exists():
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            backup_env = env_path.with_suffix(f".{timestamp}.bak")
            shutil.copy2(env_path, backup_env)
            _ok(f"Updated .env  (backup: {backup_env.name})")
        else:
            _ok("Created .env")
        env_path.write_text("".join(env_lines))

    # ── Remove redundant auth files ───────────────────────────────────────────
    removed = remove_redundant_auth_files(repo_root)

    # ── CI/CD generation ──────────────────────────────────────────────────────
    _run_cicd_wizard(pack_root)

    # ── Summary ───────────────────────────────────────────────────────────────
    _section("Done")
    _ok(f"{changed_count} config file(s) created/updated.")
    if removed:
        _ok(f"{len(removed)} redundant auth file(s) removed.")
    if env_dirty:
        _ok(".env updated with group source IDs.")
    print()
    _hint("Next steps:")
    _hint("  1. Verify group source IDs in .env match your Entra ID object IDs.")
    _hint("  2. Confirm environment.project names in each config.<env>.yaml file.")
    _hint("  3. Add CI/CD secrets to GitHub Environments (IDP_CLIENT_SECRET).")
    print()


# ── --check mode (CI) ──────────────────────────────────────────────────────────

def collect_expected(
    variant: str,
    env: str,
    site: str,
    installed_ctx: list[str],
) -> dict[str, object]:
    overlay = build_overlay(variant, env, site, installed_ctx)  # app_owner omitted intentionally
    modules = overlay["variables"]["modules"]
    expected: dict[str, object] = {}
    for key, value in modules["common"]["cdf_project_foundation"].items():
        expected[f"common.cdf_project_foundation.{key}"] = value
    for module, overrides in modules.get("contextualization", {}).items():
        for key, value in overrides.items():
            expected[f"contextualization.{module}.{key}"] = value
    for module, overrides in modules.get("sourcesystem", {}).items():
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


def check_config_file(
    path: Path,
    variant: str,
    env: str,
    site: str,
    installed_ctx: list[str],
) -> list[str]:
    if not path.exists():
        return ["    (file missing — run without --check to create it)"]
    config = load_yaml(path)
    errors: list[str] = []
    for dotted, expected_value in collect_expected(variant, env, site, installed_ctx).items():
        actual = get_actual_value(config, dotted)
        if actual != expected_value:
            errors.append(f"    {dotted}: got {actual!r}, expected {expected_value!r}")
    return errors


def _run_check(
    args_variant: str | None,
    args_site: str,
    repo_root: Path | None = None,
) -> None:
    pack_root = get_pack_root(repo_root)
    variant = resolve_variant(args_variant, get_data_models_dir(repo_root))
    site = args_site.strip()
    targets = target_config_paths(pack_root, ENVIRONMENTS)
    installed_ctx = list_installed_contextualization_modules(repo_root)

    all_errors: dict[str, list[str]] = {}
    for env, path in targets.items():
        errs = check_config_file(path, variant, env, site, installed_ctx)
        if errs:
            all_errors[path.name] = errs

    target_set = set(targets.values())
    for path in find_env_configs(repo_root):
        if path in target_set:
            continue
        env_guess = path.name.split(".")[1] if "." in path.name else "dev"
        env_guess = env_guess if env_guess in ENVIRONMENTS else "dev"
        errs = check_config_file(path, variant, env_guess, site, installed_ctx)
        if errs:
            all_errors[path.name] = errs

    ctx_dir = get_contextualization_dir(repo_root)
    stale_auth: list[Path] = []
    for module_dir in list_installed_contextualization_modules(repo_root):
        for rel_path in CONTEXTUALIZATION_REDUNDANT_AUTH[module_dir]:
            if (ctx_dir / module_dir / rel_path).exists():
                stale_auth.append(ctx_dir / module_dir / rel_path)

    if all_errors:
        print(f"ERROR: Config file(s) out of sync with variant '{variant}':\n")
        for filename, errs in all_errors.items():
            print(f"  {filename}")
            for e in errs:
                print(e)
        print("\n  Run: python scripts/setup_project.py -y")
        sys.exit(1)
    if stale_auth:
        print("ERROR: Redundant auth file(s) still present (covered by cdf_project_foundation):")
        for p in stale_auth:
            print(f"  {p.relative_to(ctx_dir.parent.parent)}")
        print("\n  Run: python scripts/setup_project.py -y")
        sys.exit(1)
    print(f"OK: All config file(s) match variant '{variant}'. No stale auth files.")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="CI mode: exit 1 if any target config is out of sync with the installed variant",
    )
    parser.add_argument(
        "--yes", "-y",
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

    if args.check:
        _run_check(args.variant, args.site)
    else:
        _run_wizard(args.variant, args.site, args.yes)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Cancelled by user.")
        sys.exit(130)
