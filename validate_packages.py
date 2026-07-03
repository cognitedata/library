#!/usr/bin/env python3
"""
Validation script for packages.toml file.
Checks structure and validates that all module paths exist.
Assumes "modules" as the base folder where packages.toml is located.
"""

import os
import sys
import tomllib
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PackageSpec:
    """One deployment pack entry from packages.toml."""

    id: str
    title: str
    description: str
    modules: list[str]


@dataclass(frozen=True)
class PackagesRegistry:
    """Parsed packages.toml registry."""

    description: str
    packages: dict[str, PackageSpec]


def _require_non_empty_str(data: dict[str, object], key: str, context: str) -> str | None:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        print(f"ERROR: {context} missing or invalid '{key}' field")
        return None
    return value


def parse_packages_registry(data: dict[str, object]) -> PackagesRegistry | None:
    """Parse and validate the top-level packages.toml shape."""
    library_raw = data.get("library")
    if not isinstance(library_raw, dict):
        print("ERROR: Missing [library] header")
        return None

    description = _require_non_empty_str(library_raw, "description", "[library] section")
    if description is None:
        return None

    packages_raw = data.get("packages")
    if not isinstance(packages_raw, dict) or not packages_raw:
        print("ERROR: Missing or empty [packages] section")
        return None

    packages: dict[str, PackageSpec] = {}
    for package_name, package_data in packages_raw.items():
        if not isinstance(package_data, dict):
            print(f"ERROR: Package '{package_name}' must be a table")
            return None

        package_id = _require_non_empty_str(package_data, "id", f"Package '{package_name}'")
        title = _require_non_empty_str(package_data, "title", f"Package '{package_name}'")
        package_description = _require_non_empty_str(
            package_data, "description", f"Package '{package_name}'"
        )
        if package_id is None or title is None or package_description is None:
            return None

        modules_raw = package_data.get("modules")
        if not isinstance(modules_raw, list) or not modules_raw:
            print(f"ERROR: Package '{package_name}' modules must be a non-empty list")
            return None

        modules: list[str] = []
        for module_path in modules_raw:
            if not isinstance(module_path, str) or not module_path.strip():
                print(f"ERROR: Package '{package_name}' module path must be a non-empty string")
                return None
            modules.append(module_path)

        packages[package_name] = PackageSpec(
            id=package_id,
            title=title,
            description=package_description,
            modules=modules,
        )

    return PackagesRegistry(description=description, packages=packages)


def validate_module_paths(
    package_name: str,
    modules: list[str],
    base_path: str = "modules",
) -> bool:
    """Validate that all module paths exist in the filesystem."""
    base_path_obj = Path(base_path)

    if not base_path_obj.exists():
        print(f"ERROR: Base path '{base_path}' does not exist")
        return False

    for module_path in modules:
        full_path = base_path_obj / module_path
        if not full_path.exists():
            print(
                f"ERROR: Package '{package_name}' module path '{module_path}' does not exist at '{full_path}'"
            )
            return False

        module_toml = full_path / "module.toml"
        if not module_toml.exists():
            print(
                f"ERROR: Package '{package_name}' module path '{module_path}' does not have a module.toml file and is not a valid module"
            )
            return False

        with open(module_toml, "rb") as f:
            module_data = tomllib.load(f)

        module_raw = module_data.get("module")
        if not isinstance(module_raw, dict):
            print(
                f"ERROR: Package '{package_name}' module path '{module_path}' is missing a [module] table"
            )
            return False

        required_fields = {"id", "package_id", "title"}
        missing_fields = required_fields - set(module_raw.keys())
        if missing_fields:
            print(
                f"ERROR: Package '{package_name}' module path '{module_path}' does not have the following required fields: {missing_fields}"
            )
            return False

        extra_resources_raw = module_data.get("extra_resources", [])
        if not isinstance(extra_resources_raw, list):
            print(
                f"ERROR: Package '{package_name}' module '{module_path}' extra_resources must be a list"
            )
            return False

        for extra_resource in extra_resources_raw:
            if not isinstance(extra_resource, dict):
                print(
                    f"ERROR: Package '{package_name}' module '{module_path}' has an invalid extra_resource entry"
                )
                return False
            location = extra_resource.get("location")
            if not isinstance(location, str) or not location:
                print(
                    f"ERROR: Package '{package_name}' module '{module_path}' has an extra_resource without a location"
                )
                return False

            resource_path = base_path_obj / location
            if not resource_path.exists():
                print(
                    f"ERROR: Package '{package_name}' module '{module_path}' refers to a non-existent file: {resource_path}"
                )
                return False

        print(f"✓ Module '{module_path}' validated successfully")

    return True


def _package_id_prefix(package_id: str) -> str:
    """Return required module id prefix dp:<pack>: from a package_id."""
    short = package_id.removeprefix("dp:")
    return f"dp:{short}:"


def _allowed_id_prefixes_for_module(
    module_rel_path: str,
    package_id: str,
    packages: dict[str, PackageSpec],
) -> set[str]:
    """Prefixes allowed for a module id (primary pack + any pack that lists the module)."""
    prefixes = {_package_id_prefix(package_id)}
    for package_spec in packages.values():
        if module_rel_path in package_spec.modules:
            prefixes.add(_package_id_prefix(package_spec.id))
    return prefixes


def validate_module_id_prefixes(
    base_path: str = "modules",
    packages: dict[str, PackageSpec] | None = None,
) -> bool:
    """Ensure each module id uses dp:<pack>:<slug> for an allowed pack prefix."""
    base_path_obj = Path(base_path)
    mismatches: list[tuple[str, str, set[str]]] = []

    for module_toml in sorted(base_path_obj.rglob("module.toml")):
        with open(module_toml, "rb") as f:
            module_data = tomllib.load(f)
        module_raw = module_data.get("module")
        if not isinstance(module_raw, dict):
            continue
        module_id = module_raw.get("id")
        package_id = module_raw.get("package_id")
        if not isinstance(module_id, str) or not isinstance(package_id, str):
            continue
        if not module_id or not package_id:
            continue
        if not module_id.startswith("dp:") or module_id.count(":") < 2:
            rel_path = module_toml.relative_to(base_path_obj).as_posix()
            mismatches.append((rel_path, module_id, set()))
            continue
        rel_path = module_toml.parent.relative_to(base_path_obj).as_posix()
        allowed = (
            _allowed_id_prefixes_for_module(rel_path, package_id, packages)
            if packages
            else {_package_id_prefix(package_id)}
        )
        if not any(module_id.startswith(prefix) for prefix in allowed):
            mismatches.append((rel_path, module_id, allowed))

    if mismatches:
        print("\nERROR: Module id prefix is not allowed for this module:")
        for rel_path, module_id, allowed in mismatches:
            if not allowed:
                print(f"  {rel_path}: id={module_id!r} (expected dp:<pack>:<slug>)")
            else:
                allowed_str = ", ".join(sorted(allowed))
                print(f"  {rel_path}: id={module_id!r} (allowed prefixes: {allowed_str})")
        return False

    print("\n✓ All module ids use an allowed dp:<pack>: prefix")
    return True


def validate_unique_module_ids(base_path: str = "modules") -> bool:
    """Ensure every module.toml under modules/ has a unique `id`.

    Different modules sharing the same id breaks Toolkit's cherry-pick UX
    and any downstream telemetry keyed on the id.
    """
    base_path_obj = Path(base_path)
    ids_by_path: dict[str, str] = {}
    duplicates: defaultdict[str, list[str]] = defaultdict(list)

    for module_toml in sorted(base_path_obj.rglob("module.toml")):
        with open(module_toml, "rb") as f:
            module_data = tomllib.load(f)
        module_raw = module_data.get("module")
        if not isinstance(module_raw, dict):
            continue
        module_id = module_raw.get("id")
        if not isinstance(module_id, str) or not module_id:
            continue
        rel_path = module_toml.relative_to(base_path_obj).as_posix()
        if module_id in ids_by_path:
            duplicates[module_id].append(ids_by_path[module_id])
            duplicates[module_id].append(rel_path)
        else:
            ids_by_path[module_id] = rel_path

    if duplicates:
        print("\nERROR: Duplicate module ids detected:")
        for module_id, paths in duplicates.items():
            unique_paths = sorted(set(paths))
            print(f"  id = {module_id!r} used by:")
            for path in unique_paths:
                print(f"    - {path}")
        return False

    print(f"\n✓ All {len(ids_by_path)} module ids are unique")
    return True


def main() -> None:
    """Main validation function."""
    packages_file = "modules/packages.toml"

    if not os.path.exists(packages_file):
        print(f"ERROR: {packages_file} not found")
        sys.exit(1)

    try:
        with open(packages_file, "rb") as f:
            data = tomllib.load(f)

        print(f"✓ Successfully parsed {packages_file}")

        registry = parse_packages_registry(data)
        if registry is None:
            sys.exit(1)

        print("✓ [library] header validation passed")
        print(f"✓ Found {len(registry.packages)} packages")

        for package_name, package_spec in registry.packages.items():
            print(f"\nValidating package: {package_name}")
            print(f"✓ Package '{package_name}' structure validation passed")

            if not validate_module_paths(package_name, package_spec.modules, "modules"):
                sys.exit(1)

        if not validate_module_id_prefixes("modules", registry.packages):
            sys.exit(1)

        if not validate_unique_module_ids("modules"):
            sys.exit(1)

        print(
            f"\n🎉 All validation checks passed! {len(registry.packages)} packages validated successfully."
        )

    except tomllib.TOMLDecodeError as e:
        print(f"ERROR: Invalid TOML format: {e}")
        sys.exit(1)
    except OSError as e:
        print(f"ERROR: Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
