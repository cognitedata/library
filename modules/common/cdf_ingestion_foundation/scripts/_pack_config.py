"""Shared helpers for configure_datamodel.py and build_workflow.py."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

MODULE_ROOT = Path(__file__).parent.parent
REPO_ROOT = MODULE_ROOT.parent.parent.parent  # scripts/ -> module/ -> modules/ -> repo/

KNOWN_DATA_MODEL_DIRS = (
    "isa_manufacturing_extension",
    "cfihos_oil_and_gas_extension",
)

# modules/sourcesystem/<dir> → enabledSources key in cdf_ingestion_foundation config
SOURCE_SYSTEM_DIR_TO_ENABLED_KEY: dict[str, str] = {
    "cdf_pi_foundation": "pi",
    "cdf_sap_foundation": "sap",
    "cdf_opcua_foundation": "opcua",
    "cdf_db_foundation": "db",
    "cdf_files_foundation": "files",
}

# Sources that have population/contextualization tasks in the ingestion workflow
WORKFLOW_TASK_SOURCE_KEYS = frozenset({"pi", "sap", "opcua"})


def get_org_dir_name(repo_root: Path | None = None) -> str | None:
    """Return default_organization_dir from cdf.toml, or None if unset/empty."""
    root = repo_root or REPO_ROOT
    toml_path = root / "cdf.toml"
    if not toml_path.exists():
        return None
    content = toml_path.read_text()
    m = re.search(r"""default_organization_dir\s*=\s*["']([^"']*)["']""", content)
    if not m:
        return None
    value = m.group(1).strip()
    return value or None


def get_pack_root(repo_root: Path | None = None) -> Path:
    """Repository root, or <repo>/<org-dir> when default_organization_dir is set."""
    root = repo_root or REPO_ROOT
    org = get_org_dir_name(root)
    if org:
        return root / org
    return root


def get_data_models_dir(repo_root: Path | None = None) -> Path:
    """Path to modules/data_models under the pack root (org-prefixed when configured)."""
    return get_pack_root(repo_root) / "modules" / "data_models"


def get_sourcesystem_dir(repo_root: Path | None = None) -> Path:
    """Path to modules/sourcesystem under the pack root (org-prefixed when configured)."""
    return get_pack_root(repo_root) / "modules" / "sourcesystem"


def detect_enabled_sources(repo_root: Path | None = None) -> dict[str, bool]:
    """
    Scan modules/sourcesystem/ and set enabledSources flags from installed module dirs.
    Returns all known keys (pi, sap, opcua, db, files); missing dirs are false.
    """
    sourcesystem_dir = get_sourcesystem_dir(repo_root)
    enabled = {key: False for key in SOURCE_SYSTEM_DIR_TO_ENABLED_KEY.values()}
    if not sourcesystem_dir.is_dir():
        return enabled
    for module_dir, key in SOURCE_SYSTEM_DIR_TO_ENABLED_KEY.items():
        if (sourcesystem_dir / module_dir).is_dir():
            enabled[key] = True
    return enabled


def list_installed_source_system_modules(repo_root: Path | None = None) -> list[str]:
    """Module directory names present under modules/sourcesystem/."""
    sourcesystem_dir = get_sourcesystem_dir(repo_root)
    if not sourcesystem_dir.is_dir():
        return []
    return [
        module_dir
        for module_dir in SOURCE_SYSTEM_DIR_TO_ENABLED_KEY
        if (sourcesystem_dir / module_dir).is_dir()
    ]


def get_default_env(repo_root: Path | None = None) -> str:
    """Read default_env from cdf.toml (falls back to dev)."""
    root = repo_root or REPO_ROOT
    toml_path = root / "cdf.toml"
    if toml_path.exists():
        content = toml_path.read_text()
        m = re.search(r"""default_env\s*=\s*["']([^"']+)["']""", content)
        if m:
            return m.group(1).strip()
    return "dev"


def find_env_configs(repo_root: Path | None = None) -> list[Path]:
    """
    Discover config.<env>.yaml files (CDF Toolkit order):
      1. <pack-root>/config.*.yaml
      2. <repo-root>/config.*.yaml  (when pack-root differs)
    Backup files (*.bak.*) are excluded.
    """
    root = repo_root or REPO_ROOT
    pack_root = get_pack_root(root)
    search_dirs: list[Path] = [pack_root]
    if pack_root != root:
        search_dirs.append(root)

    found: list[Path] = []
    seen: set[Path] = set()
    for search_dir in search_dirs:
        for path in sorted(search_dir.glob("config.*.yaml")):
            if ".bak." in path.name:
                continue
            resolved = path.resolve()
            if resolved not in seen:
                seen.add(resolved)
                found.append(path)
    return found


def find_env_config(env: str, repo_root: Path | None = None) -> Path | None:
    """Return config.<env>.yaml from discovered files, or None."""
    target = f"config.{env}.yaml"
    for path in find_env_configs(repo_root):
        if path.name == target:
            return path
    return None


def detect_data_model_variant(data_models_dir: Path) -> str:
    """
    Detect installed data model from subdirectory names under modules/data_models/.
    Raises SystemExit with a message when zero or multiple models are found.
    """
    if not data_models_dir.is_dir():
        raise SystemExit(
            f"ERROR: Data models directory not found: {data_models_dir}\n"
            "  Expected modules/data_models/ under the pack root."
        )

    present = [
        name
        for name in KNOWN_DATA_MODEL_DIRS
        if (data_models_dir / name).is_dir()
    ]
    if not present:
        raise SystemExit(
            f"ERROR: No supported data model found under {data_models_dir}\n"
            f"  Expected one of: {', '.join(KNOWN_DATA_MODEL_DIRS)}"
        )
    if len(present) > 1:
        raise SystemExit(
            f"ERROR: Multiple data models found under {data_models_dir}: {present}\n"
            "  Keep only one model directory per deployment pack."
        )
    return present[0]


def load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text()) or {}


def get_ingestion_config(config: dict) -> dict:
    """Return variables.modules.common.cdf_ingestion_foundation from a config file."""
    return (
        config.get("variables", {})
        .get("modules", {})
        .get("common", {})
        .get("cdf_ingestion_foundation", {})
    )


def deep_merge(base: dict, overlay: dict) -> dict:
    """Recursively merge overlay into base (overlay wins on conflicts)."""
    result = dict(base)
    for key, value in overlay.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result
