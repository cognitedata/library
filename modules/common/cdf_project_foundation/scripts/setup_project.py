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
from _env_io import parse_env_file  # noqa: F401 (re-exported for tests)
from _pack_config import (
    CONTEXTUALIZATION_REDUNDANT_AUTH,
    KNOWN_DATA_MODEL_DIRS,
    REPO_ROOT,
    TOOLS_REDUNDANT_AUTH,
    deep_merge,
    detect_data_model_variant,
    find_env_configs,
    get_contextualization_dir,
    get_data_models_dir,
    get_pack_root,
    get_sourcesystem_dir,
    list_installed_contextualization_modules,
    list_installed_source_system_modules,
    load_yaml,
)
from _prompts import prompt, prompt_choice, prompt_env_var, prompt_yes_no
from _style import _banner, _hint, _ok, _section, _warn
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


_MODULE_LABELS: dict[str, str] = {
    "cdf_pi_foundation":    "PI Foundation",
    "cdf_sap_foundation":   "SAP Foundation",
    "cdf_opcua_foundation": "OPC-UA Foundation",
    "cdf_db_foundation":    "DB Foundation",
    "cdf_files_foundation": "Files Foundation",
}


def _module_label(module: str) -> str:
    return _MODULE_LABELS.get(module, module)


def resolve_sourcesystem_variables(
    instance_space: str,
    installed_modules: list[str],
    location: str = "",
    integration_owners: dict[str, tuple[str, str]] | None = None,
    data_owners: dict[str, tuple[str, str]] | None = None,
) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for module in installed_modules:
        vars_: dict[str, Any] = {"instanceSpace": instance_space}
        if location:
            vars_["location"] = location
        if integration_owners and module in integration_owners:
            name, email = integration_owners[module]
            if name:
                vars_["integration_owner_name"] = name
            if email:
                vars_["integration_owner_email"] = email
        if data_owners and module in data_owners:
            name, email = data_owners[module]
            if name:
                vars_["data_owner_name"] = name
            if email:
                vars_["data_owner_email"] = email
        result[module] = vars_
    return result


def collect_sourcesystem_datasets(
    installed_modules: list[str],
    repo_root: Path | None = None,
) -> list[str]:
    """Read ``dataset`` from each installed foundation source system module's default.config.yaml.

    Only covers the five foundation modules (cdf_pi_foundation, cdf_sap_foundation,
    cdf_opcua_foundation, cdf_db_foundation, cdf_files_foundation).
    Returns a deduplicated ordered list of dataset external IDs.
    """
    ss_dir = get_sourcesystem_dir(repo_root)
    seen: set[str] = set()
    datasets: list[str] = []
    for module in installed_modules:
        cfg = ss_dir / module / "default.config.yaml"
        if not cfg.exists():
            continue
        data = load_yaml(cfg)
        ds = data.get("dataset")
        if ds and isinstance(ds, str) and ds not in seen:
            seen.add(ds)
            datasets.append(ds)
    return datasets


def build_foundation_vars(
    variant: str,
    env: str,
    site: str,
    datasets: list[str] | None = None,
) -> dict:
    """Variables block for ``variables.modules.common.cdf_project_foundation``."""
    ingestion = dict(INGESTION_FOUNDATION_VARIABLES[variant])
    ingestion["site"] = site
    if datasets is not None:
        ingestion["dataset"] = datasets
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
    integration_owners: dict[str, tuple[str, str]] | None = None,
    data_owners: dict[str, tuple[str, str]] | None = None,
    repo_root: Path | None = None,
) -> dict:
    """Full ``variables.modules`` overlay dict to merge into a config file."""
    instance_space = INGESTION_FOUNDATION_VARIABLES[variant]["instanceSpace"]
    installed_ss = list_installed_source_system_modules(repo_root)
    datasets = collect_sourcesystem_datasets(installed_ss, repo_root)
    ctx_vars = resolve_contextualization_variables(variant, instance_space, installed_ctx)

    if app_owner and "cdf_file_annotation" in ctx_vars:
        ctx_vars["cdf_file_annotation"]["ApplicationOwner"] = app_owner
    # site doubles as location_name for entity matching (same concept).
    if site and "cdf_entity_matching" in ctx_vars:
        ctx_vars["cdf_entity_matching"]["location_name"] = site

    modules_vars: dict[str, Any] = {
        "common": {
            "cdf_project_foundation": build_foundation_vars(variant, env, site, datasets or None),
        },
    }
    if ctx_vars:
        modules_vars["contextualization"] = ctx_vars
    if installed_ss:
        modules_vars["sourcesystem"] = resolve_sourcesystem_variables(
            instance_space, installed_ss, site, integration_owners, data_owners
        )
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
    """Remove auth group files covered by cdf_project_foundation.

    Covers contextualization modules (entity matching, file annotation) and
    tools/apps modules (qualitizer) whose standalone auth becomes redundant
    once the foundation persona groups are deployed.
    """
    from _pack_config import REPO_ROOT as _REPO_ROOT
    modules_root = (_REPO_ROOT if repo_root is None else repo_root) / "modules"
    removed: list[Path] = []

    # Contextualization modules.
    ctx_dir = get_contextualization_dir(repo_root)
    for module_dir in list_installed_contextualization_modules(repo_root):
        for rel_path in CONTEXTUALIZATION_REDUNDANT_AUTH[module_dir]:
            auth_file = ctx_dir / module_dir / rel_path
            if auth_file.exists():
                auth_file.unlink()
                removed.append(auth_file)
                _ok(f"Removed redundant auth: {auth_file.relative_to(modules_root)}")

    # Tools / apps modules.
    for module_rel, auth_files in TOOLS_REDUNDANT_AUTH.items():
        module_dir = modules_root / module_rel
        if not module_dir.is_dir():
            continue
        for rel_path in auth_files:
            auth_file = module_dir / rel_path
            if auth_file.exists():
                auth_file.unlink()
                removed.append(auth_file)
                _ok(f"Removed redundant auth: {auth_file.relative_to(modules_root)}")

    return removed


# ── CI/CD generation ───────────────────────────────────────────────────────────

def _read_existing_values(
    pack_root: Path,
    envs: tuple[str, ...],
    installed_ss: list[str],
) -> dict:
    """Read current values from existing config files to pre-fill prompts on re-run."""
    existing: dict = {
        "project_names": {},
        "site": "",
        "app_owner": "",
        "integration_owners": {},
        "data_owners": {},
    }
    for env in envs:
        path = pack_root / f"config.{env}.yaml"
        if not path.exists():
            continue
        cfg = load_yaml(path)
        proj = cfg.get("environment", {}).get("project", "")
        if proj:
            existing["project_names"][env] = proj
        modules = cfg.get("variables", {}).get("modules", {})
        foundation = modules.get("common", {}).get("cdf_project_foundation", {})
        if foundation.get("site"):
            existing["site"] = foundation["site"]
        app_owner = (
            modules.get("contextualization", {})
            .get("cdf_file_annotation", {})
            .get("ApplicationOwner", "")
        )
        if app_owner and app_owner != "<APPLICATION_OWNER>":
            existing["app_owner"] = app_owner
        ss = modules.get("sourcesystem", {})
        for module in installed_ss:
            mv = ss.get(module, {})
            if mv.get("integration_owner_name") or mv.get("integration_owner_email"):
                existing["integration_owners"][module] = (
                    mv.get("integration_owner_name", ""),
                    mv.get("integration_owner_email", ""),
                )
            if mv.get("data_owner_name") or mv.get("data_owner_email"):
                existing["data_owners"][module] = (
                    mv.get("data_owner_name", ""),
                    mv.get("data_owner_email", ""),
                )
    return existing


def _prompt_owner(
    label: str,
    default_name: str = "",
    default_email: str = "",
) -> tuple[str, str]:
    """Prompt for an owner's name and a validated email. Both are optional (blank skips)."""
    name = prompt(f"{label} name", default=default_name or None).strip()
    while True:
        email = prompt(f"{label} email", default=default_email or None).strip()
        if not email or re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
            return name, email
        _warn("Invalid email. Use format: name@domain.com")


def _all_same(owners: dict[str, tuple[str, str]]) -> bool:
    """Return True if all modules share the same owner (name, email) pair."""
    vals = list(owners.values())
    return bool(vals) and all(v == vals[0] for v in vals)


def _prompt_source_system_ownership(
    installed_ss: list[str],
    existing_integration: dict[str, tuple[str, str]] | None = None,
    existing_data: dict[str, tuple[str, str]] | None = None,
) -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, str]]]:
    """Prompt for integration and data owner details for installed source systems.

    Pre-fills prompts from *existing_integration* / *existing_data* on re-runs.
    Returns (integration_owners, data_owners) — each a dict of module → (name, email).
    """
    if not installed_ss:
        return {}, {}

    ei = existing_integration or {}
    ed = existing_data or {}

    _section("Source System Ownership")
    _hint(f"Installed: {', '.join(_module_label(m) for m in installed_ss)}")

    # ── Integration owner ─────────────────────────────────────────────────────
    print()
    integration_owners: dict[str, tuple[str, str]] = {}
    shared_int_default = _all_same(ei) if ei else True
    if prompt_yes_no("Same integration owner for all source systems?", default=shared_int_default):
        first = next(iter(ei.values()), ("", "")) if ei else ("", "")
        name, email = _prompt_owner("  Integration owner", *first)
        for m in installed_ss:
            integration_owners[m] = (name, email)
    else:
        for m in installed_ss:
            print(f"\n  {_module_label(m)}")
            dn, de = ei.get(m, ("", ""))
            integration_owners[m] = _prompt_owner("    Integration owner", dn, de)

    # ── Data owner ────────────────────────────────────────────────────────────
    print()
    data_owners: dict[str, tuple[str, str]] = {}
    shared_data_default = _all_same(ed) if ed else True
    if prompt_yes_no("Same data owner for all source systems?", default=shared_data_default):
        first = next(iter(ed.values()), ("", "")) if ed else ("", "")
        name, email = _prompt_owner("  Data owner", *first)
        for m in installed_ss:
            data_owners[m] = (name, email)
    else:
        for m in installed_ss:
            print(f"\n  {_module_label(m)}")
            dn, de = ed.get(m, ("", ""))
            data_owners[m] = _prompt_owner("    Data owner", dn, de)

    return integration_owners, data_owners


def _derive_enterprise(project_names: dict[str, str]) -> str | None:
    """Try to derive the enterprise slug from project names like ``acme-dev`` → ``acme``."""
    candidates: set[str] = set()
    for env, name in project_names.items():
        suffix = f"-{env}"
        if name.endswith(suffix):
            candidates.add(name[: -len(suffix)])
    return candidates.pop() if len(candidates) == 1 else None


def _run_cicd_wizard(pack_root: Path, project_names: dict[str, str] | None = None) -> None:
    _section("CI/CD Pipeline Generation")
    if not prompt_yes_no("Generate GitHub Actions workflows for this project?", default=False):
        return

    derived = _derive_enterprise(project_names or {})
    enterprise = prompt(
        "Enterprise slug (e.g. acme for acme-dev / acme-prod)",
        default=derived,
    ).strip()
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
    installed_ss = list_installed_source_system_modules(repo_root)

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

    # Load existing values from config files to pre-fill prompts on re-runs.
    existing = _read_existing_values(pack_root, selected_envs, installed_ss)
    targets = target_config_paths(pack_root, selected_envs)

    # ── CDF project names ─────────────────────────────────────────────────────
    _section("CDF Project Names")
    _hint("Format: <enterprise>-<env>  e.g. acme-dev, acme-test, acme-prod")
    _hint("Only lowercase letters, digits, and hyphens. Cannot be empty.")
    project_names: dict[str, str] = {}
    for env in selected_envs:
        while True:
            val = prompt(
                f"Project name for {env}",
                default=existing["project_names"].get(env) or None,
            ).strip()
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
        while True:
            site = prompt(
                "Site / location name (e.g. oslo)",
                default=existing["site"] or None,
            ).strip().lower()
            if not site or re.fullmatch(r"[a-z0-9_-]+", site):
                break
            _warn("Use only lowercase letters, digits, hyphens, and underscores.")

    # ── Source system ownership ───────────────────────────────────────────────
    integration_owners, data_owners = _prompt_source_system_ownership(
        installed_ss,
        existing_integration=existing["integration_owners"] or None,
        existing_data=existing["data_owners"] or None,
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
            app_owner = prompt(
                "Application owner email(s)",
                default=existing["app_owner"] or None,
            ).strip()
            if not app_owner:
                break
            emails = [e.strip() for e in app_owner.split(",") if e.strip()]
            if all(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", e) for e in emails):
                break
            _warn("One or more addresses look invalid. Use format: name@domain.com")

    # ── Review summary ────────────────────────────────────────────────────────
    env_dirty = any(
        env_vals.get(f"{p.upper()}_SOURCE_ID") != original_env_vals.get(f"{p.upper()}_SOURCE_ID")
        for p in PERSONAS
    )
    _section("Review")
    for env, path in targets.items():
        state = "create" if not path.exists() else "update"
        _ok(f"[{state}] {path.name}  —  project: {project_names[env]}")
    if env_dirty:
        _ok(".env  —  group source IDs updated")

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
        overlay = build_overlay(
            variant, env, site, installed_ctx, app_owner,
            integration_owners, data_owners, repo_root
        )
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
    _run_cicd_wizard(pack_root, project_names)

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
        help="Optional site / location name inserted into access-group names (<persona>-<site>-<env>)",
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
