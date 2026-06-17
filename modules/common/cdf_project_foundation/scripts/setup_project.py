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
    get_org_dir_name,
    get_pack_root,
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

ENVIRONMENT_VALIDATION_TYPE: dict[str, str] = {"dev": "dev", "test": "prod", "prod": "prod"}

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
        "instanceSpace": "inst_location",
    },
}

DATA_MODELS_MODULE_VARIABLES: dict[str, dict[str, str]] = {
    "isa_manufacturing_extension": {
        "isaSchemaSpace": "sp_isa_manufacturing",
        "isaInstanceSpace": "sp_isa_instance_space",
    },
    "cfihos_oil_and_gas_extension": {},
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

# Fallback category for modules that may live in old nested-category config files
# (e.g. created before the flat-structure migration).  Used by _write_config_update
# to try an alternative dotted path when the flat path is not found.
_MODULE_CATEGORY_FALLBACK: dict[str, str] = {
    "cdf_project_foundation":    "common",
    "cdf_entity_matching":       "contextualization",
    "cdf_file_annotation":       "contextualization",
    "cdf_pi_foundation":         "sourcesystem",
    "cdf_sap_foundation":        "sourcesystem",
    "cdf_opcua_foundation":      "sourcesystem",
    "cdf_db_foundation":         "sourcesystem",
    "cdf_files_foundation":      "sourcesystem",
    "isa_manufacturing_extension":    "datamodels",
    "cfihos_oil_and_gas_extension":   "datamodels",
}

# Keys that are stale in an existing config when cdf_project_foundation is
# present (foundation covers these capabilities; standalone modules added them
# before foundation was installed).
_STALE_CTX_KEYS: tuple[str, ...] = (
    # Contextualization stale keys.
    "variables.modules.contextualization.cdf_file_annotation.groupSourceId",
    "variables.modules.contextualization.cdf_entity_matching"
    ".entity_matching_processing_group_source_id",
    "variables.modules.cdf_entity_matching.reservedWordPrefix",
    "variables.modules.contextualization.cdf_entity_matching.reservedWordPrefix",
    # CFIHOS DM source IDs — covered by foundation persona groups.
    "variables.modules.cfihos_oil_and_gas_extension.owner_source_id",
    "variables.modules.cfihos_oil_and_gas_extension.read_source_id",
    "variables.modules.datamodels.cfihos_oil_and_gas_extension.owner_source_id",
    "variables.modules.datamodels.cfihos_oil_and_gas_extension.read_source_id",
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


def build_foundation_vars(
    variant: str,
    env: str,
    site: str,
    datasets: list[str] | None = None,
) -> dict:
    """Variables block for ``variables.modules.cdf_project_foundation``."""
    ingestion = dict(INGESTION_FOUNDATION_VARIABLES[variant])
    ingestion["site"] = site
    # Always write dataset so the key exists; empty list when no SS modules installed.
    ingestion["dataset"] = datasets if datasets is not None else []
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
    datasets: list[str] | None = None,
    cfihos_admin_user: str = "",
    cfihos_integration_owner_name: str = "",
    cfihos_integration_owner_email: str = "",
    repo_root: Path | None = None,
) -> dict:
    """Full ``variables.modules`` overlay dict to merge into a config file.

    Uses a *flat* module-name structure (no category wrappers) to match both
    Toolkit conventions and the output of ``generate_env_configs.py``, ensuring
    ``_write_config_update`` can locate every key in existing config files.

    ``datasets`` should be read from the existing ``config.<env>.yaml`` (populated
    by Toolkit at module-init time).  The ``default.config.yaml`` files are not
    reliable as Toolkit may delete them after consuming them.
    """
    instance_space = INGESTION_FOUNDATION_VARIABLES[variant]["instanceSpace"]

    installed_ss = list_installed_source_system_modules(repo_root)
    ctx_vars = resolve_contextualization_variables(variant, instance_space, installed_ctx)

    if app_owner and "cdf_file_annotation" in ctx_vars:
        ctx_vars["cdf_file_annotation"]["ApplicationOwner"] = app_owner
    if site and "cdf_entity_matching" in ctx_vars:
        ctx_vars["cdf_entity_matching"]["location_name"] = site

    # Always write dataset (even as empty list) so the key is always present.
    modules_vars: dict[str, Any] = {
        "cdf_project_foundation": build_foundation_vars(variant, env, site, datasets),
    }
    modules_vars.update(ctx_vars)
    if installed_ss:
        modules_vars.update(
            resolve_sourcesystem_variables(
                instance_space, installed_ss, site, integration_owners, data_owners
            )
        )
    if variant == "cfihos_oil_and_gas_extension":
        # CFIHOS uses its own space / instance_space variables — not the ISA ones.
        # instance_space is derived from site; space is fixed.
        cfihos_dm_vars: dict[str, Any] = {"instance_space": "inst_location", "environment": env}
        if cfihos_admin_user:
            cfihos_dm_vars["admin_user"] = cfihos_admin_user
        if cfihos_integration_owner_name:
            cfihos_dm_vars["integrationOwnerName"] = cfihos_integration_owner_name
        if cfihos_integration_owner_email:
            cfihos_dm_vars["integrationOwnerEmail"] = cfihos_integration_owner_email
        modules_vars[variant] = cfihos_dm_vars

        # If the search solution module is also installed, keep its instance_space
        # in sync with the enterprise module.
        data_models_dir = get_data_models_dir(repo_root)
        if (data_models_dir / "cfihos_oil_and_gas_extension_search").is_dir():
            modules_vars["cfihos_oil_and_gas_extension_search"] = {
                "instance_space": "inst_location",
                "environment": env,
            }
    else:
        # ISA variant — static variables (isaSchemaSpace, isaInstanceSpace).
        modules_vars[variant] = DATA_MODELS_MODULE_VARIABLES[variant]

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
    # Tries the flat path first (new structure), then the old nested-category path
    # (e.g. variables.modules.common.cdf_project_foundation.*) for backwards
    # compatibility with configs created before the flat-structure migration.
    for module, mod_vars in overlay.get("variables", {}).get("modules", {}).items():
        if not isinstance(mod_vars, dict):
            continue
        for key, val in mod_vars.items():
            yaml_val = (
                val
                if isinstance(val, str)
                else yaml.dump(val, default_flow_style=True).strip()
            )
            # 1. Try flat path: variables.modules.<module>.<key>
            old, c = _yaml_set_value(lines, f"variables.modules.{module}.{key}", yaml_val)
            if old is None and not c:
                # 2. Try legacy nested-category path.
                category = _MODULE_CATEGORY_FALLBACK.get(module)
                if category:
                    old, c = _yaml_set_value(
                        lines,
                        f"variables.modules.{category}.{module}.{key}",
                        yaml_val,
                    )
            if old is None and not c:
                # 3. Key truly absent — insert under flat path.
                if _yaml_insert_key(lines, f"variables.modules.{module}", key, yaml_val):
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
    modules_root = get_pack_root(repo_root) / "modules"
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


# ── Staging → test migration ──────────────────────────────────────────────────

def _migrate_staging_to_test(pack_root: Path) -> bool:
    """Rename ``config.staging.yaml`` → ``config.test.yaml`` with corrected fields.

    Toolkit creates a ``staging`` environment; the Foundation DP uses ``test``.
    Updates ``environment.name`` to ``test`` and ``environment.validation-type``
    to ``prod`` (staging is pre-prod and should use production-like validation).

    Returns ``True`` if a migration was performed.
    """
    staging = pack_root / "config.staging.yaml"
    test    = pack_root / "config.test.yaml"

    if not staging.exists():
        return False
    if test.exists():
        _warn(
            "Both config.staging.yaml and config.test.yaml exist — "
            "skipping automatic staging migration. Remove one manually."
        )
        return False

    lines = staging.read_text().splitlines(keepends=True)
    _yaml_set_value(lines, "environment.name", "test")
    _yaml_set_value(lines, "environment.validation-type", "prod")
    test.write_text("".join(lines))
    staging.unlink()
    _ok("Migrated config.staging.yaml → config.test.yaml  (validation-type: prod)")
    return True


# ── Data model auth patching ──────────────────────────────────────────────────

def patch_cfihos_auth_for_missing_search(repo_root: Path | None = None) -> list[Path]:
    """Remove ``{{search_space}}`` from cfihos auth files when the search module is absent.

    The ``cfihos_oil_and_gas_extension`` auth files include ``{{search_space}}`` in
    the ``dataModelsAcl`` scope.  When ``cfihos_oil_and_gas_extension_search`` is not
    installed that variable is undefined, which causes Toolkit to fail.  This function
    strips the offending list item from every auth YAML under the extension's auth/
    directory when the search module directory does not exist.

    Returns the list of files actually modified.
    """
    data_models_dir = get_data_models_dir(repo_root)
    search_module = data_models_dir / "cfihos_oil_and_gas_extension_search"
    cfihos_auth_dir = data_models_dir / "cfihos_oil_and_gas_extension" / "auth"

    if search_module.is_dir():
        return []  # search module present — {{search_space}} is valid, leave as-is
    if not cfihos_auth_dir.is_dir():
        return []  # cfihos extension not installed

    patched: list[Path] = []
    for auth_file in sorted(cfihos_auth_dir.glob("*.yaml")):
        original = auth_file.read_text()
        # Remove any line that contains only the {{search_space}} list item.
        new_lines = [
            line for line in original.splitlines(keepends=True)
            if "{{search_space}}" not in line
        ]
        if len(new_lines) < len(original.splitlines()):
            auth_file.write_text("".join(new_lines))
            patched.append(auth_file)
            _ok(
                f"Removed {{{{search_space}}}} from: "
                f"{auth_file.relative_to(data_models_dir.parent)}"
            )
    return patched


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
        "dataset": [],
        "app_owner": "",
        "integration_owners": {},
        "data_owners": {},
        "cfihos_admin_user": "",
        "cfihos_integration_owner_name": "",
        "cfihos_integration_owner_email": "",
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
        # Support both flat (new) and nested-category (old) structures.
        foundation = (
            modules.get("cdf_project_foundation")
            or modules.get("common", {}).get("cdf_project_foundation", {})
        )
        if foundation.get("site"):
            existing["site"] = foundation["site"]
        # Collect dataset values from installed sourcesystem module sections.
        # Supports both flat (new) and nested-category (old) config structures.
        ss_section = modules.get("sourcesystem") or modules
        ss_datasets: list[str] = []
        for module in installed_ss:
            mod_vars = ss_section.get(module, {}) if isinstance(ss_section, dict) else {}
            ds = mod_vars.get("dataset", "")
            if ds and isinstance(ds, str) and ds not in ss_datasets:
                ss_datasets.append(ds)
        if ss_datasets:
            existing["dataset"] = ss_datasets
        app_owner = (
            (modules.get("cdf_file_annotation") or
             modules.get("contextualization", {}).get("cdf_file_annotation", {}))
            .get("ApplicationOwner", "")
        )
        if app_owner and app_owner != "<APPLICATION_OWNER>":
            existing["app_owner"] = app_owner
        # CFIHOS DM owner fields (flat or nested datamodels category).
        cfihos_dm = (
            modules.get("cfihos_oil_and_gas_extension")
            or modules.get("datamodels", {}).get("cfihos_oil_and_gas_extension", {})
            or {}
        )
        _placeholder_emails = {"admin.user@firm.com", "integration.owner@firm.com"}
        if cfihos_dm.get("admin_user") and cfihos_dm["admin_user"] not in _placeholder_emails:
            existing["cfihos_admin_user"] = cfihos_dm["admin_user"]
        if cfihos_dm.get("integrationOwnerName") and cfihos_dm["integrationOwnerName"] != "Integration Owner":
            existing["cfihos_integration_owner_name"] = cfihos_dm["integrationOwnerName"]
        if cfihos_dm.get("integrationOwnerEmail") and cfihos_dm["integrationOwnerEmail"] not in _placeholder_emails:
            existing["cfihos_integration_owner_email"] = cfihos_dm["integrationOwnerEmail"]
        # Support both flat and nested-category structures.
        ss = modules if not modules.get("sourcesystem") else modules.get("sourcesystem", {})
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


def _run_cicd_wizard(pack_root: Path) -> list[Path]:
    """Run the CI/CD generation step.  Returns the list of files written (empty if skipped)."""
    _section("CI/CD Pipeline Generation")
    if not prompt_yes_no("Generate GitHub Actions workflows for this project?", default=False):
        return []

    generate_script = Path(__file__).parent / "generate_actions.py"
    if not generate_script.exists():
        _warn(f"Could not find generate_actions.py at {generate_script} — skipping.")
        return []

    cmd = [sys.executable, str(generate_script), "--force"]
    from _style import _C
    print(f"\n  {_C.DIM}Running: {' '.join(cmd)}{_C.RESET}")
    result = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")

    written: list[Path] = []
    for line in result.stdout.splitlines():
        if line.startswith("Wrote "):
            written.append(Path(line[6:].strip()))

    if result.returncode == 0:
        _ok("CI/CD workflows generated.  See docs/FOUNDATION_CICD.md for next steps.")
    else:
        _warn("CI/CD generation completed with warnings — review the output above.")
        for line in result.stderr.splitlines():
            _hint(f"  {line}")

    return written


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
    org_dir = get_org_dir_name()
    if org_dir:
        _ok(f"Organization dir   : {org_dir}  (from cdf.toml)")
    else:
        _hint("No organization directory set in cdf.toml — config files written to repo root.")
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

    # Migrate config.staging.yaml → config.test.yaml only when test env is selected.
    if "test" in selected_envs:
        _migrate_staging_to_test(pack_root)

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

    # ── CFIHOS DM owner config (only when CFIHOS variant is selected) ────────
    cfihos_admin_user = ""
    cfihos_integration_owner_name = ""
    cfihos_integration_owner_email = ""
    if variant == "cfihos_oil_and_gas_extension":
        _section("CFIHOS Data Model — Owner Configuration")
        _hint("Configures admin_user, integrationOwnerName, and integrationOwnerEmail")
        _hint("in the cfihos_oil_and_gas_extension module. Leave blank to skip.")

        while True:
            cfihos_admin_user = prompt(
                "Admin user email",
                default=existing.get("cfihos_admin_user") or None,
            ).strip()
            if not cfihos_admin_user or re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", cfihos_admin_user):
                break
            _warn("Invalid email. Use format: name@domain.com")

        cfihos_integration_owner_name = prompt(
            "Integration owner name",
            default=existing.get("cfihos_integration_owner_name") or None,
        ).strip()

        while True:
            cfihos_integration_owner_email = prompt(
                "Integration owner email",
                default=existing.get("cfihos_integration_owner_email") or None,
            ).strip()
            if not cfihos_integration_owner_email or re.fullmatch(
                r"[^@\s]+@[^@\s]+\.[^@\s]+", cfihos_integration_owner_email
            ):
                break
            _warn("Invalid email. Use format: name@domain.com")

    # ── Group source IDs → .env ───────────────────────────────────────────────
    _section("Group Source IDs  (Entra ID object IDs)")
    _hint("Stored in .env and referenced as ${CONSUMER_SOURCE_ID} etc. in config.")
    _hint("Leave blank to skip — fill .env manually later.")
    # .env always lives at repo root (same level as cdf.toml), regardless of
    # whether an organization directory is configured.
    env_path = (repo_root or REPO_ROOT) / ".env"
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
            integration_owners, data_owners,
            datasets=existing["dataset"],
            cfihos_admin_user=cfihos_admin_user,
            cfihos_integration_owner_name=cfihos_integration_owner_name,
            cfihos_integration_owner_email=cfihos_integration_owner_email,
            repo_root=repo_root,
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
    patched = patch_cfihos_auth_for_missing_search(repo_root)

    # ── CI/CD generation ──────────────────────────────────────────────────────
    cicd_files = _run_cicd_wizard(pack_root)

    # ── Summary ───────────────────────────────────────────────────────────────
    _section("Done")
    _ok(f"{changed_count} config file(s) created/updated.")
    if removed:
        _ok(f"{len(removed)} redundant auth file(s) removed.")
    if patched:
        _ok(f"{len(patched)} cfihos auth file(s) patched (search_space removed).")
    if env_dirty:
        _ok(".env updated with group source IDs.")
    if cicd_files:
        _ok(f"{len(cicd_files)} CI/CD workflow file(s) generated.")
    print()
    _hint("Next steps:")
    _hint("  1. Verify group source IDs in .env match your Entra ID object IDs.")
    _hint("  2. Confirm environment.project names in each config.<env>.yaml file.")
    if cicd_files:
        _hint("  3. Create GitHub Environments: dev-toolkit-credentials,")
        _hint("     test-toolkit-credentials, prod-toolkit-credentials")
        _hint("     (see docs/FOUNDATION_CICD.md for variable and secret details).")
        _hint("  4. Add IDP_CLIENT_SECRET to each GitHub Environment.")
        _hint("  5. Create and protect branches dev and main;")
        _hint("     open a PR to dev to validate dry-run.yml.")
    else:
        _hint("  3. Add CI/CD secrets to GitHub Environments (IDP_CLIENT_SECRET).")
    print()


# ── --check mode (CI) ──────────────────────────────────────────────────────────

def collect_expected(
    variant: str,
    env: str,
    site: str,
    installed_ctx: list[str],
) -> dict[str, object]:
    overlay = build_overlay(variant, env, site, installed_ctx)
    modules = overlay["variables"]["modules"]
    expected: dict[str, object] = {}
    for module, mod_vars in modules.items():
        if isinstance(mod_vars, dict):
            for key, value in mod_vars.items():
                expected[f"{module}.{key}"] = value
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
