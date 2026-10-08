"""Copy scoped-matching test resources into this module's Toolkit folders.

The default module matches CogniteFile and CogniteAsset with empty scope
properties. Run this script when you want to deploy the test-only site/unit model
and point default.config.yaml at it.

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

_TEST_CONFIG: dict[str, str] = {
    "fileSchemaSpace": "sp_file_annotation_scope",
    "fileExternalId": "ScopedFile",
    "targetEntitySchemaSpace": "sp_file_annotation_scope",
    "targetEntityExternalId": "Asset",
    "primaryScopeProperty": "site",
    "secondaryScopeProperty": "unit",
}

_PROD_CONFIG: dict[str, str] = {
    "fileSchemaSpace": "cdf_cdm",
    "fileExternalId": "CogniteFile",
    "targetEntitySchemaSpace": "cdf_cdm",
    "targetEntityExternalId": "CogniteAsset",
    "primaryScopeProperty": '""',
    "secondaryScopeProperty": '""',
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


def apply_scoped_matching(*, module_dir: Path, local_setup_dir: Path) -> None:
    """Copy payload files into Toolkit resource folders and point config at them."""
    for source_rel, dest_rel in RESOURCE_COPIES:
        source = local_setup_dir / source_rel
        if not source.is_file():
            raise FileNotFoundError(f"Missing test payload file: {source}")
        dest = module_dir / dest_rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, dest)
    _patch_default_config(module_dir, _TEST_CONFIG)


def revert_scoped_matching(*, module_dir: Path) -> None:
    """Remove copied payload files and restore CogniteFile / CogniteAsset defaults."""
    for _source_rel, dest_rel in RESOURCE_COPIES:
        dest = module_dir / dest_rel
        if dest.is_file():
            dest.unlink()
    _patch_default_config(module_dir, _PROD_CONFIG)


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
