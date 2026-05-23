"""
Tests for configuration validation utilities.

Tests cover hierarchy config validation, extract config validation, and error formatting.
"""

import sys
from pathlib import Path
from typing import Any, Dict

import pytest

# Add module root to path
module_root = Path(__file__).parent.parent
if str(module_root) not in sys.path:
    sys.path.insert(0, str(module_root))

from functions.shared.utils.config_validator import (
    format_validation_errors,
    validate_extract_config,
    validate_hierarchy_config,
)


class TestValidateHierarchyConfig:
    """Test hierarchy configuration validation."""

    def test_validate_hierarchy_config_valid(
        self, sample_hierarchy_config: Dict[str, Any]
    ) -> None:
        """Test validating a valid hierarchy configuration."""
        # Act
        errors = validate_hierarchy_config(sample_hierarchy_config)

        # Assert
        assert isinstance(errors, list)
        # Should have no errors (warnings are OK)
        error_count = len([e for e in errors if e.startswith("❌")])
        assert error_count == 0

    def test_validate_hierarchy_config_missing_hierarchy_levels(self) -> None:
        """Test validating hierarchy config with missing scope_hierarchy."""
        config: Dict[str, Any] = {}

        errors = validate_hierarchy_config(config)

        assert len(errors) > 0
        assert any("❌" in e and "scope_hierarchy" in e for e in errors)

    def test_validate_hierarchy_config_invalid_hierarchy_levels_type(self) -> None:
        """Test validating hierarchy config with invalid levels type."""
        config = {
            "scope_hierarchy": {
                "type": "hierarchy",
                "levels": "invalid",
                "locations": [],
            },
        }

        errors = validate_hierarchy_config(config)

        assert len(errors) > 0
        assert any("❌" in e and "levels" in e for e in errors)

    def test_validate_hierarchy_config_too_few_levels(self) -> None:
        """Test validating hierarchy config with too few levels."""
        config = {
            "scope_hierarchy": {
                "type": "hierarchy",
                "levels": ["site"],
                "locations": [],
            },
        }

        errors = validate_hierarchy_config(config)

        assert any("⚠️" in e and "levels" in e for e in errors)

    def test_validate_hierarchy_config_duplicate_levels(self) -> None:
        """Test validating hierarchy config with duplicate level names."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "hierarchy_levels": ["site", "plant", "site"],  # Duplicate
                    "scope": [],
                },
            },
        }

        # Act
        errors = validate_hierarchy_config(config)

        # Assert
        assert len(errors) > 0
        assert any("❌" in e and "Duplicate" in e for e in errors)

    def test_validate_hierarchy_config_missing_scope(self) -> None:
        """Test validating hierarchy config with missing scope."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "hierarchy_levels": ["site", "plant"],
                },
            },
        }

        # Act
        errors = validate_hierarchy_config(config)

        # Assert
        assert len(errors) > 0
        assert any("❌" in e and "scope" in e for e in errors)

    def test_validate_hierarchy_config_location_missing_name(self) -> None:
        """Test validating hierarchy config with location missing name."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "hierarchy_levels": ["site", "plant"],
                    "scope": [
                        {
                            # Missing "name" field
                            "description": "Test site",
                        },
                    ],
                },
            },
        }

        # Act
        errors = validate_hierarchy_config(config)

        # Assert
        assert len(errors) > 0
        assert any("❌" in e and "name" in e for e in errors)

    def test_validate_hierarchy_config_location_empty_name(self) -> None:
        """Test validating hierarchy config with location missing id and name."""
        config = {
            "scope_hierarchy": {
                "type": "hierarchy",
                "levels": ["site", "unit"],
                "locations": [
                    {
                        "name": "",
                        "description": "Test site",
                    },
                ],
            },
        }

        errors = validate_hierarchy_config(config)

        assert len(errors) > 0
        assert any("❌" in e and ("name" in e or "id" in e) for e in errors)

    def test_validate_hierarchy_config_files_at_non_last_level(self) -> None:
        """Test validating hierarchy config with files at non-last level."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "hierarchy_levels": ["site", "plant"],
                    "scope": [
                        {
                            "name": "SITE_1",
                            "files": ["FILE-001"],  # Files at site level (not last)
                            "locations": [
                                {
                                    "name": "PLANT_1",
                                },
                            ],
                        },
                    ],
                },
            },
        }

        # Act
        errors = validate_hierarchy_config(config)

        # Assert
        # Should have warning about files at non-last level
        assert any("⚠️" in e and "files" in e for e in errors)

    def test_validate_hierarchy_config_last_level_has_locations(self) -> None:
        """Test validating hierarchy config where last level has child locations."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "hierarchy_levels": ["site", "plant"],
                    "scope": [
                        {
                            "name": "SITE_1",
                            "locations": [
                                {
                                    "name": "PLANT_1",
                                    "locations": [  # Last level shouldn't have locations
                                        {"name": "AREA_1"},
                                    ],
                                },
                            ],
                        },
                    ],
                },
            },
        }

        # Act
        errors = validate_hierarchy_config(config)

        # Assert
        assert len(errors) > 0
        assert any("❌" in e and "last level" in e and "locations" in e for e in errors)

    def test_validate_hierarchy_config_files_not_list(self) -> None:
        """Test validating hierarchy config where files is not a list."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "hierarchy_levels": ["site"],
                    "scope": [
                        {
                            "name": "SITE_1",
                            "files": "FILE-001",  # Should be list
                        },
                    ],
                },
            },
        }

        # Act
        errors = validate_hierarchy_config(config)

        # Assert
        assert len(errors) > 0
        assert any("❌" in e and "files" in e and "list" in e for e in errors)

    def test_validate_hierarchy_config_nested_structure(
        self, sample_hierarchy_config: Dict[str, Any]
    ) -> None:
        """Test validating nested location structure."""
        # Act
        errors = validate_hierarchy_config(sample_hierarchy_config)

        # Assert
        # Should validate nested structure correctly
        error_count = len([e for e in errors if e.startswith("❌")])
        assert error_count == 0


class TestValidateExtractConfig:
    """Test extract configuration validation."""

    def test_validate_extract_config_valid(
        self, sample_extract_config: Dict[str, Any]
    ) -> None:
        """Test validating a valid extract configuration."""
        # Act
        errors = validate_extract_config(sample_extract_config)

        # Assert
        assert isinstance(errors, list)
        error_count = len([e for e in errors if e.startswith("❌")])
        assert error_count == 0

    def test_validate_extract_config_missing_patterns(self) -> None:
        """Test validating extract config with missing patterns."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "limit": -1,
                },
            },
        }

        # Act
        errors = validate_extract_config(config)

        # Assert
        assert len(errors) > 0
        assert any("❌" in e and "patterns" in e for e in errors)

    def test_validate_extract_config_pattern_not_dict(self) -> None:
        """Test validating extract config with pattern that is not a dict."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "patterns": [
                        "invalid",  # Should be dict
                    ],
                },
            },
        }

        # Act
        errors = validate_extract_config(config)

        # Assert
        assert len(errors) > 0
        assert any("❌" in e and "dictionary" in e for e in errors)

    def test_validate_extract_config_pattern_missing_samples(self) -> None:
        """Test validating extract config with pattern missing samples."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "patterns": [
                        {
                            "category": "equipment",
                            # Missing "samples" or "sample"
                        },
                    ],
                },
            },
        }

        # Act
        errors = validate_extract_config(config)

        # Assert
        assert len(errors) > 0
        assert any("❌" in e and "sample" in e for e in errors)

    def test_validate_extract_config_pattern_samples_not_list(self) -> None:
        """Test validating extract config with pattern samples not a list."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "patterns": [
                        {
                            "category": "equipment",
                            "samples": "P-101",  # Should be list
                        },
                    ],
                },
            },
        }

        # Act
        errors = validate_extract_config(config)

        # Assert
        assert len(errors) > 0
        assert any("❌" in e and "list" in e for e in errors)

    def test_validate_extract_config_pattern_empty_samples(self) -> None:
        """Test validating extract config with pattern having empty samples."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "patterns": [
                        {
                            "category": "equipment",
                            "samples": [],  # Empty list
                        },
                    ],
                },
            },
        }

        # Act
        errors = validate_extract_config(config)

        # Assert
        # Should have warning about empty samples
        assert any("⚠️" in e and "empty" in e for e in errors)

    def test_validate_extract_config_pattern_with_sample_singular(self) -> None:
        """Test validating extract config with pattern using 'sample' (singular)."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "patterns": [
                        {
                            "category": "equipment",
                            "sample": ["P-101"],  # Singular form
                        },
                    ],
                },
            },
        }

        # Act
        errors = validate_extract_config(config)

        # Assert
        # Should be valid (both 'sample' and 'samples' are accepted)
        error_count = len([e for e in errors if e.startswith("❌")])
        assert error_count == 0

    def test_validate_extract_config_invalid_limit(self) -> None:
        """Test validating extract config with invalid limit."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "patterns": [
                        {
                            "category": "equipment",
                            "samples": ["P-101"],
                        },
                    ],
                    "limit": -5,  # Invalid (must be -1 or positive)
                },
            },
        }

        # Act
        errors = validate_extract_config(config)

        # Assert
        assert len(errors) > 0
        assert any("❌" in e and "limit" in e for e in errors)

    def test_validate_extract_config_invalid_batch_size(self) -> None:
        """Test validating extract config with invalid batch_size."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "patterns": [
                        {
                            "category": "equipment",
                            "samples": ["P-101"],
                        },
                    ],
                    "batch_size": -1,  # Invalid (must be null or positive)
                },
            },
        }

        # Act
        errors = validate_extract_config(config)

        # Assert
        assert len(errors) > 0
        assert any("❌" in e and "batch_size" in e for e in errors)

    def test_validate_extract_config_valid_limit_negative_one(self) -> None:
        """Test validating extract config with valid limit of -1."""
        # Arrange
        config = {
            "config": {
                "data": {
                    "patterns": [
                        {
                            "category": "equipment",
                            "samples": ["P-101"],
                        },
                    ],
                    "limit": -1,  # Valid (all files)
                },
            },
        }

        # Act
        errors = validate_extract_config(config)

        # Assert
        error_count = len([e for e in errors if e.startswith("❌")])
        assert error_count == 0


class TestFormatValidationErrors:
    """Test validation error formatting."""

    def test_format_validation_errors_no_errors(self) -> None:
        """Test formatting when there are no errors."""
        # Arrange
        errors: list[str] = []

        # Act
        result = format_validation_errors(errors)

        # Assert
        assert "✅" in result
        assert "valid" in result.lower()

    def test_format_validation_errors_with_errors(self) -> None:
        """Test formatting when there are errors."""
        # Arrange
        errors = [
            "❌ Error 1",
            "❌ Error 2",
        ]

        # Act
        result = format_validation_errors(errors)

        # Assert
        assert "❌" in result
        assert "ERRORS" in result
        assert "Error 1" in result
        assert "Error 2" in result

    def test_format_validation_errors_with_warnings(self) -> None:
        """Test formatting when there are warnings."""
        # Arrange
        errors = [
            "⚠️  Warning 1",
            "⚠️  Warning 2",
        ]

        # Act
        result = format_validation_errors(errors)

        # Assert
        assert "⚠️" in result
        assert "WARNINGS" in result
        assert "Warning 1" in result
        assert "Warning 2" in result

    def test_format_validation_errors_with_errors_and_warnings(self) -> None:
        """Test formatting when there are both errors and warnings."""
        # Arrange
        errors = [
            "❌ Error 1",
            "⚠️  Warning 1",
            "❌ Error 2",
        ]

        # Act
        result = format_validation_errors(errors)

        # Assert
        assert "❌" in result
        assert "⚠️" in result
        assert "ERRORS" in result
        assert "WARNINGS" in result
        assert "Error 1" in result
        assert "Warning 1" in result

    def test_format_validation_errors_structure(self) -> None:
        """Test that formatted errors have proper structure."""
        # Arrange
        errors = [
            "❌ Error 1",
            "⚠️  Warning 1",
        ]

        # Act
        result = format_validation_errors(errors)

        # Assert
        # Should have sections
        assert "=" in result  # Separator lines
        assert "CONFIGURATION VALIDATION RESULTS" in result
