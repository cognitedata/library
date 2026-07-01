"""Shared path / config helpers for setup_project.py."""

import tomllib
from pathlib import Path

import yaml

MODULE_ROOT = Path(__file__).parent.parent


def _resolve_repo_root() -> Path:
    """Locate the Toolkit project root by searching up for ``cdf.toml``.

    Handles both layouts:
    - Flat   : ``<project>/modules/common/cdf_project_foundation/scripts/``
    - Org-dir: ``<project>/<org>/modules/common/cdf_project_foundation/scripts/``

    Falls back to four levels above this file when no ``cdf.toml`` is found
    (e.g. the library development repo which has no ``cdf.toml``).
    """
    for parent in Path(__file__).resolve().parents:
        if (parent / "cdf.toml").is_file():
            return parent
    # Fallback: scripts/ → cdf_project_foundation/ → common/ → modules/ → repo/
    return Path(__file__).resolve().parents[4]


REPO_ROOT = _resolve_repo_root()

KNOWN_DATA_MODEL_DIRS = (
    "isa_manufacturing_extension",
    "cfihos_oil_and_gas_extension",
)

SOURCE_SYSTEM_MODULE_DIRS: tuple[str, ...] = (
    "cdf_pi_extractor",
    "cdf_sap_extractor",
    "cdf_opcua_extractor",
    "cdf_db_extractor",
    "cdf_files_extractor",
)

# Contextualization modules whose standalone auth groups become redundant when
# cdf_foundation is present (it covers all required capabilities).
# Maps module directory name → auth file(s) to remove relative to the module root.
CONTEXTUALIZATION_REDUNDANT_AUTH: dict[str, tuple[str, ...]] = {
    "cdf_entity_matching": ("auth/entity.matching.processing.groups.Group.yaml",),
    "cdf_file_annotation": ("auth/file_annotation.Group.yaml",),
}

# Tools/apps modules whose standalone auth groups become redundant when
# cdf_foundation is present.
# Maps module path (relative to modules/) → auth file(s) relative to the module root.
TOOLS_REDUNDANT_AUTH: dict[str, tuple[str, ...]] = {
    "tools/apps/qualitizer": ("auth/apps.qualitizer.Group.yaml",),
    # CFIHOS DM ships its own owner/read auth groups, which are redundant when
    # cdf_project_foundation persona groups are deployed.
    "datamodels/cfihos_oil_and_gas_extension": (
        "auth/gp_cdf_owner_cfihos_oil_gas_data_model.group.yaml",
        "auth/gp_cdf_read_cfihos_oil_gas_data_model.group.yaml",
    ),
}


def get_org_dir_name(repo_root: Path | None = None) -> str | None:
    """Return default_organization_dir from cdf.toml, or None if unset/empty."""
    root = repo_root or REPO_ROOT
    toml_path = root / "cdf.toml"
    if not toml_path.exists():
        return None
    try:
        data = tomllib.loads(toml_path.read_text())
        default_dir = data.get("cdf", {}).get("default_organization_dir")
        if isinstance(default_dir, str):
            value = default_dir.strip()
            return value or None
    except tomllib.TOMLDecodeError as e:
        print(f"WARNING: Failed to parse TOML file {toml_path}: {e}")
    return None


def get_pack_root(repo_root: Path | None = None) -> Path:
    """Repository root, or <repo>/<org-dir> when default_organization_dir is set."""
    root = repo_root or REPO_ROOT
    org = get_org_dir_name(root)
    if org:
        return root / org
    return root


def get_data_models_dir(repo_root: Path | None = None) -> Path:
    """Path to modules/datamodels under the pack root (org-prefixed when configured)."""
    return get_pack_root(repo_root) / "modules" / "datamodels"


def get_sourcesystem_dir(repo_root: Path | None = None) -> Path:
    """Path to modules/sourcesystem under the pack root (org-prefixed when configured)."""
    return get_pack_root(repo_root) / "modules" / "sourcesystem"


def get_contextualization_dir(repo_root: Path | None = None) -> Path:
    """Path to modules/contextualization under the pack root."""
    return get_pack_root(repo_root) / "modules" / "contextualization"


def list_installed_contextualization_modules(repo_root: Path | None = None) -> list[str]:
    """Module directory names present under modules/contextualization/ that have redundant auth."""
    ctx_dir = get_contextualization_dir(repo_root)
    if not ctx_dir.is_dir():
        return []
    return [
        module_dir
        for module_dir in CONTEXTUALIZATION_REDUNDANT_AUTH
        if (ctx_dir / module_dir).is_dir()
    ]


def list_installed_source_system_modules(repo_root: Path | None = None) -> list[str]:
    """Module directory names present under modules/sourcesystem/."""
    sourcesystem_dir = get_sourcesystem_dir(repo_root)
    if not sourcesystem_dir.is_dir():
        return []
    return [
        module_dir
        for module_dir in SOURCE_SYSTEM_MODULE_DIRS
        if (sourcesystem_dir / module_dir).is_dir()
    ]


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


def detect_data_model_variant(data_models_dir: Path) -> str:
    """
    Detect installed data model from subdirectory names under modules/datamodels/.
    Raises SystemExit with a message when zero or multiple models are found.
    """
    if not data_models_dir.is_dir():
        raise SystemExit(
            f"ERROR: Data models directory not found: {data_models_dir}\n"
            "  Expected modules/datamodels/ under the pack root."
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
            "  Keep only one model directory per deployment pack,\n"
            "  or pass --variant to select one explicitly."
        )
    return present[0]


def load_yaml(path: Path) -> dict:
    try:
        return yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as e:
        raise SystemExit(f"ERROR: Failed to parse YAML file {path}:\n  {e}")


def deep_merge(base: dict, overlay: dict) -> dict:
    """Recursively merge overlay into base (overlay wins on conflicts)."""
    result = dict(base)
    for key, value in overlay.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result
