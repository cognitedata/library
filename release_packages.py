#!/usr/bin/env python3
"""
Release script to create a zip archive of the modules folder.
Designed to run on merge to main branch and create GitHub releases.
"""

import os
import sys
import zipfile
from pathlib import Path


def create_modules_zip(output_name: str = None) -> str:
    """
    Create a zip archive of the modules folder.

    Args:
        output_name: Optional custom name for the zip file (defaults to 'packages.zip')

    Returns:
        Path to the created zip file
    """
    modules_dir = Path("modules")

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
        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            # Walk through the modules directory
            for root, dirs, files in os.walk(modules_dir):
                # Skip __pycache__ and other common Python cache directories
                dirs[:] = [
                    d for d in dirs if d not in ["__pycache__", ".pytest_cache", ".git"]
                ]

                # Add files to zip
                for file in files:
                    file_path = Path(root) / file
                    # Calculate relative path for the zip (relative to modules directory)
                    arcname = file_path.relative_to(modules_dir)

                    print(f"  Adding: {arcname}")
                    zipf.write(file_path, arcname)

        # Get file size
        file_size = output_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)

        print(f"\n✅ Successfully created: {output_path}")
        print(f"📦 File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
        print(f"📁 Source: {modules_dir}")

        return str(output_path)

    except Exception as e:
        print(f"ERROR: Failed to create zip file: {e}")
        sys.exit(1)


def main():
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
