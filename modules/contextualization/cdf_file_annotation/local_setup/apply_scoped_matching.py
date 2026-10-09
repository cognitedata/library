"""Copy scoped-matching test resources into this module's Toolkit folders.

The default module matches CogniteFile and CogniteAsset with empty scope
properties. Run this script when you want to deploy the test-only site/unit model
and point default.config.yaml (and config.dev.yaml, when present) at it.

Payload files live under local_setup/payload/ — not data_modeling/ or
transformations/ — so Cognite Toolkit does not treat them as module resources
until they are copied.

Usage (from the repository root):

    python modules/contextualization/cdf_file_annotation/local_setup/apply_scoped_matching.py
    python modules/contextualization/cdf_file_annotation/local_setup/apply_scoped_matching.py --revert
"""

import argparse
import shutil
from pathlib import Path

LOCAL_SETUP_DIR = Path(__file__).resolve().parent
MODULE_DIR = LOCAL_SETUP_DIR.parent
REPO_ROOT = MODULE_DIR.parents[2]
CONFIG_DEV_PATH = REPO_ROOT / "config.dev.yaml"
CONFIG_DEV_MODULE_KEY = "cdf_file_annotation"

# Source paths are relative to local_setup/; destinations are relative to the module root.
RESOURCE_COPIES: tuple[tuple[str, str], ...] = (
    ("payload/scope.Space.yaml", "data_modeling/scope.Space.yaml"),
    ("payload/FileAnnotationScope.DataModel.yaml", "data_modeling/FileAnnotationScope.DataModel.yaml"),
    ("payload/AssetScope.Container.yaml", "data_modeling/containers/AssetScope.Container.yaml"),
    ("payload/FileScope.Container.yaml", "data_modeling/containers/FileScope.Container.yaml"),
    ("payload/Asset.View.yaml", "data_modeling/views/Asset.View.yaml"),
    ("payload/ScopedFile.View.yaml", "data_modeling/views/ScopedFile.View.yaml"),
    (
        "payload/tr_asset_names_to_scope.Transformation.yaml",
        "transformations/tr_asset_names_to_scope.Transformation.yaml",
    ),
    (
        "payload/tr_asset_names_to_scope.Transformation.sql",
        "transformations/tr_asset_names_to_scope.Transformation.sql",
    ),
    (
        "payload/tr_file_names_to_scope.Transformation.yaml",
        "transformations/tr_file_names_to_scope.Transformation.yaml",
    ),
    (
        "payload/tr_file_names_to_scope.Transformation.sql",
        "transformations/tr_file_names_to_scope.Transformation.sql",
    ),
)

# Always present so Toolkit can resolve {{ scope* }} in copied payload YAML.
_SCOPE_VARS: dict[str, str] = {
    "scopeSchemaSpace": "sp_file_annotation_scope",
    "scopeDmVersion": "v1",
    "scopeDataModelExternalId": "FileAnnotationScope_SOL",
}

_TEST_CONFIG: dict[str, str] = {
    "fileSchemaSpace": "sp_file_annotation_scope",
    "fileExternalId": "ScopedFile",
    "targetEntitySchemaSpace": "sp_file_annotation_scope",
    "targetEntityExternalId": "Asset",
    "primaryScopeProperty": "site",
    "secondaryScopeProperty": "unit",
    **_SCOPE_VARS,
}

_PROD_CONFIG: dict[str, str] = {
    "fileSchemaSpace": "cdf_cdm",
    "fileExternalId": "CogniteFile",
    "targetEntitySchemaSpace": "cdf_cdm",
    "targetEntityExternalId": "CogniteAsset",
    "primaryScopeProperty": '""',
    "secondaryScopeProperty": '""',
    **_SCOPE_VARS,
}


def _set_scalar(text: str, key: str, value: str) -> str:
    prefix = f"{key}:"
    lines = text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if stripped.startswith(prefix):
            newline = "\n" if line.endswith("\n") else ""
            indent = line[: len(line) - len(stripped)]
            lines[index] = f"{indent}{prefix} {value}{newline}"
            return "".join(lines)
    raise ValueError(f"Key {key!r} not found in default.config.yaml")


def _patch_default_config(module_dir: Path, values: dict[str, str]) -> None:
    config_path = module_dir / "default.config.yaml"
    text = config_path.read_text(encoding="utf-8")
    for key, value in values.items():
        text = _set_scalar(text, key, value)
    config_path.write_text(text, encoding="utf-8")


def _section_bounds(lines: list[str], section_key: str) -> tuple[int, int, str]:
    """Return (header_index, end_index_exclusive, child_indent) for a YAML mapping section."""
    header = f"{section_key}:"
    for header_index, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if stripped.rstrip("\r\n") != header:
            continue
        section_indent = line[: len(line) - len(stripped)]
        child_indent = f"{section_indent}  "
        end_index = header_index + 1
        while end_index < len(lines):
            next_line = lines[end_index]
            next_stripped = next_line.lstrip()
            if next_stripped == "" or next_stripped.startswith("#"):
                end_index += 1
                continue
            next_indent = next_line[: len(next_line) - len(next_stripped)]
            if len(next_indent) <= len(section_indent):
                break
            end_index += 1
        return header_index, end_index, child_indent
    raise ValueError(f"Section {section_key!r} not found")


def _patch_yaml_section(text: str, section_key: str, values: dict[str, str]) -> str:
    """Replace or insert scalar keys inside one indented YAML mapping section."""
    lines = text.splitlines(keepends=True)
    header_index, end_index, child_indent = _section_bounds(lines, section_key)
    pending = dict(values)
    for index in range(header_index + 1, end_index):
        stripped = lines[index].lstrip()
        if stripped.startswith("#") or stripped == "":
            continue
        for key in list(pending):
            prefix = f"{key}:"
            if stripped.startswith(prefix):
                newline = "\n" if lines[index].endswith("\n") else ""
                lines[index] = f"{child_indent}{prefix} {pending.pop(key)}{newline}"
                break
    if pending:
        insert_at = end_index
        addition = [f"{child_indent}{key}: {value}\n" for key, value in pending.items()]
        lines[insert_at:insert_at] = addition
    return "".join(lines)


def _patch_config_dev(repo_root: Path, values: dict[str, str]) -> None:
    config_path = repo_root / "config.dev.yaml"
    if not config_path.is_file():
        return
    text = config_path.read_text(encoding="utf-8")
    text = _patch_yaml_section(text, CONFIG_DEV_MODULE_KEY, values)
    config_path.write_text(text, encoding="utf-8")


def apply_scoped_matching(
    *,
    module_dir: Path,
    local_setup_dir: Path,
    repo_root: Path | None = None,
) -> None:
    """Copy payload files into Toolkit resource folders and point config at them."""
    for source_rel, dest_rel in RESOURCE_COPIES:
        source = local_setup_dir / source_rel
        if not source.is_file():
            raise FileNotFoundError(f"Missing test payload file: {source}")
        dest = module_dir / dest_rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, dest)
    _patch_default_config(module_dir, _TEST_CONFIG)
    _patch_config_dev(repo_root if repo_root is not None else REPO_ROOT, _TEST_CONFIG)


def revert_scoped_matching(*, module_dir: Path, repo_root: Path | None = None) -> None:
    """Remove copied payload files and restore CogniteFile / CogniteAsset defaults."""
    for _source_rel, dest_rel in RESOURCE_COPIES:
        dest = module_dir / dest_rel
        if dest.is_file():
            dest.unlink()
    _patch_default_config(module_dir, _PROD_CONFIG)
    _patch_config_dev(repo_root if repo_root is not None else REPO_ROOT, _PROD_CONFIG)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--revert",
        action="store_true",
        help="Remove copied resources and restore CDF CDM views with empty scope properties.",
    )
    args = parser.parse_args(argv)
    if args.revert:
        revert_scoped_matching(module_dir=MODULE_DIR)
    else:
        apply_scoped_matching(module_dir=MODULE_DIR, local_setup_dir=LOCAL_SETUP_DIR)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
