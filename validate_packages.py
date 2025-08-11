#!/usr/bin/env python3
"""
Validation script for packages.toml file.
Checks structure and validates that all module paths exist.
"""

import sys
import os
from pathlib import Path
from typing import Dict, List, Any
import tomllib


def validate_library_header(data: Dict[str, Any]) -> bool:
    """Validate that the library header exists and has required fields."""
    if 'library' not in data:
        print("ERROR: Missing [library] header")
        return False
    
    library = data['library']
    if 'description' not in library:
        print("ERROR: [library] section missing 'description' field")
        return False
    
    if not library['description'] or not isinstance(library['description'], str):
        print("ERROR: [library] description must be a non-empty string")
        return False
    
    print("✓ [library] header validation passed")
    return True


def validate_packages(data: Dict[str, Any]) -> bool:
    """Validate that packages exist and have required structure."""
    if 'packages' not in data:
        print("ERROR: Missing [packages] section")
        return False
    
    packages = data['packages']
    if not packages:
        print("ERROR: No packages defined")
        return False
    
    print(f"✓ Found {len(packages)} packages")
    return True


def validate_package_structure(package_name: str, package_data: Dict[str, Any]) -> bool:
    """Validate individual package structure."""
    # Check required fields
    required_fields = ['title', 'description', 'modules']
    for field in required_fields:
        if field not in package_data:
            print(f"ERROR: Package '{package_name}' missing '{field}' field")
            return False
    
    # Validate title and description are non-empty strings
    if not isinstance(package_data['title'], str) or not package_data['title'].strip():
        print(f"ERROR: Package '{package_name}' title must be a non-empty string")
        return False
    
    if not isinstance(package_data['description'], str) or not package_data['description'].strip():
        print(f"ERROR: Package '{package_name}' description must be a non-empty string")
        return False
    
    # Validate modules is a list with at least one item
    if not isinstance(package_data['modules'], list):
        print(f"ERROR: Package '{package_name}' modules must be a list")
        return False
    
    if len(package_data['modules']) == 0:
        print(f"ERROR: Package '{package_name}' modules list cannot be empty")
        return False
    
    print(f"✓ Package '{package_name}' structure validation passed")
    return True


def validate_module_paths(package_name: str, modules: List[str], base_path: str = "modules") -> bool:
    """Validate that all module paths exist in the filesystem."""
    base_path_obj = Path(base_path)
    
    if not base_path_obj.exists():
        print(f"ERROR: Base path '{base_path}' does not exist")
        return False
    
    for module_path in modules:
        if not isinstance(module_path, str):
            print(f"ERROR: Package '{package_name}' module path must be a string, got {type(module_path)}")
            return False
        
        full_path = base_path_obj / module_path
        if not full_path.exists():
            print(f"ERROR: Package '{package_name}' module path '{module_path}' does not exist at '{full_path}'")
            return False
        
        print(f"✓ Module path '{module_path}' exists")
    
    return True


def main():
    """Main validation function."""
    packages_file = "modules/packages.toml"
    
    # Check if packages.toml exists
    if not os.path.exists(packages_file):
        print(f"ERROR: {packages_file} not found")
        sys.exit(1)
    
    try:
        # Read and parse TOML file
        with open(packages_file, 'rb') as f:
            data = tomllib.load(f)
        
        print(f"✓ Successfully parsed {packages_file}")
        
        # Validate library header
        if not validate_library_header(data):
            sys.exit(1)
        
        # Validate packages section exists
        if not validate_packages(data):
            sys.exit(1)
        
        # Validate each package
        packages = data['packages']
        for package_name, package_data in packages.items():
            print(f"\nValidating package: {package_name}")
            
            # Validate package structure
            if not validate_package_structure(package_name, package_data):
                sys.exit(1)
            
            # Validate module paths
            if not validate_module_paths(package_name, package_data['modules']):
                sys.exit(1)
        
        print(f"\n🎉 All validation checks passed! {len(packages)} packages validated successfully.")
        
    except tomllib.TOMLDecodeError as e:
        print(f"ERROR: Invalid TOML format: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
