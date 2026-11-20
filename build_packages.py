#!/usr/bin/env python3
"""
Release script to create a zip archive of the registered modules.
Designed to run on merge to main branch and create GitHub releases.

The archive holds `packages.toml` and, whole, every module that `packages.toml` lists, plus the shared
files those modules reference through `extra_resources`. That is all `cdf modules init` reads from it:
unregistered modules and loose files such as `modules/__init__.py` or `modules/README.md` are left out.
"""

import os
import sys
import tomllib
import zipfile
from pathlib import Path

from validate_packages import parse_packages_registry

PACKAGES_FILE = "packages.toml"
SKIPPED_DIRS = {"__pycache__", ".pytest_cache", ".git"}


def _walk_files(directory: Path) -> list[Path]:
    """List every file below a directory, skipping Python/git cache folders."""
    found: list[Path] = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in SKIPPED_DIRS]
        found.extend(Path(root) / file for file in files)
    return found


def _extra_resource_locations(module_toml: Path) -> list[str]:
    """Read the `[[extra_resources]]` locations (relative to the modules folder) of one module."""
    with open(module_toml, "rb") as f:
        entries = tomllib.load(f).get("extra_resources", [])
    if not isinstance(entries, list):
        return []
    return [
        entry["location"] for entry in entries if isinstance(entry, dict) and isinstance(entry.get("location"), str)
    ]


def collect_archive_files(modules_dir: Path) -> list[Path]:
    """Collect the files that go into the archive, in a stable order and without duplicates.

    Args:
        modules_dir: The folder that holds `packages.toml` and the module folders.

    Returns:
        File paths below `modules_dir`: `packages.toml`, every file of each registered
        module and the files its `extra_resources` point to.

    Raises:
        ValueError: If `packages.toml` is not a valid registry.
        FileNotFoundError: If `packages.toml`, a registered module or an extra resource does not exist.
    """
    packages_file = modules_dir / PACKAGES_FILE
    with open(packages_file, "rb") as f:
        registry = parse_packages_registry(tomllib.load(f))
    if registry is None:
        raise ValueError(f"{packages_file} is not a valid registry")

    files: dict[Path, None] = {packages_file: None}
    module_paths = dict.fromkeys(path for package in registry.packages.values() for path in package.modules)
    for module_path in module_paths:
        module_dir = modules_dir / module_path
        if not (module_dir / "module.toml").is_file():
            raise FileNotFoundError(f"Registered module '{module_path}' has no module.toml at {module_dir}")
        files.update(dict.fromkeys(_walk_files(module_dir)))
        for location in _extra_resource_locations(module_dir / "module.toml"):
            extra = modules_dir / location
            if extra.is_dir():
                files.update(dict.fromkeys(_walk_files(extra)))
            elif extra.is_file():
                files[extra] = None
            else:
                raise FileNotFoundError(f"Extra resource '{location}' of module '{module_path}' does not exist")
    return list(files)


def create_modules_zip(output_name: str | None = None, modules_dir: Path = Path("modules")) -> str:
    """
    Create a zip archive of the registered modules (see the module docstring).

    Args:
        output_name: Optional custom name for the zip file (defaults to 'packages.zip')
        modules_dir: The folder that holds packages.toml and the module folders

    Returns:
        Path to the created zip file
    """
    # Check if modules directory exists
    if not modules_dir.exists():
        print(f"ERROR: {modules_dir} directory not found")
        sys.exit(1)

    # Use canonical name 'packages.zip' if no custom name provided
    if output_name is None:
        output_name = "packages.zip"

    # Ensure .zip extension
    if not output_name.endswith(".zip"):
        output_name += ".zip"

    output_path = Path(output_name)

    print(f"Creating zip archive: {output_path}")
    print(f"Source directory: {modules_dir}")

    try:
        files = collect_archive_files(modules_dir)
        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files:
                # Calculate relative path for the zip (relative to modules directory)
                arcname = file_path.relative_to(modules_dir).as_posix()

                print(f"  Adding: {arcname}")
                zipf.write(file_path, arcname)

        file_size = output_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)

        print(f"\n✅ Successfully created: {output_path}")
        print(f"📦 File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
        print(f"📁 Source: {modules_dir}")

        return str(output_path)

    except (OSError, ValueError, tomllib.TOMLDecodeError) as e:
        print(f"ERROR: Failed to create zip file: {e}")
        sys.exit(1)


def main() -> str:
    """Main function."""
    print("🚀 Starting modules build process...")
    print(f"📂 Current working directory: {Path.cwd()}")

    # Create the zip file
    zip_path = create_modules_zip()

    print("\n🎉 Build completed successfully!")
    print(f"📦 Output file: {zip_path}")

    return zip_path


if __name__ == "__main__":
    main()
