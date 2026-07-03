"""Tests for validate_packages registry parsing."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validate_packages import PackageSpec, PackagesRegistry, parse_packages_registry


def test_parse_packages_registry_accepts_minimal_valid_shape() -> None:
    data = {
        "library": {"description": "Test library"},
        "packages": {
            "demo": {
                "id": "dp:demo",
                "title": "Demo",
                "description": "Demo pack",
                "modules": ["common/cdf_common"],
            }
        },
    }

    registry = parse_packages_registry(data)

    assert registry == PackagesRegistry(
        description="Test library",
        packages={
            "demo": PackageSpec(
                id="dp:demo",
                title="Demo",
                description="Demo pack",
                modules=["common/cdf_common"],
            )
        },
    )


def test_parse_packages_registry_rejects_missing_library() -> None:
    assert parse_packages_registry({"packages": {}}) is None


def test_parse_packages_registry_rejects_empty_module_path() -> None:
    data = {
        "library": {"description": "Test library"},
        "packages": {
            "demo": {
                "id": "dp:demo",
                "title": "Demo",
                "description": "Demo pack",
                "modules": [""],
            }
        },
    }

    assert parse_packages_registry(data) is None
