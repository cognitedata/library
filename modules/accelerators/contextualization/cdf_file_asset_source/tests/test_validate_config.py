"""
Tests for validate_config.py script.

Tests cover configuration file validation and main function.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
import yaml

# Add module root to path
module_root = Path(__file__).parent.parent
if str(module_root) not in sys.path:
    sys.path.insert(0, str(module_root))


class TestValidateConfigFile:
    """Test validate_config_file function."""

    @patch("validate_config.validate_extract_config")
    @patch("validate_config.validate_hierarchy_config")
    @patch("validate_config.format_validation_errors")
    def test_validate_config_file_extract_config(
        self,
        mock_format_errors: Mock,
        mock_validate_hierarchy: Mock,
        mock_validate_extract: Mock,
        temp_dir: Path,
    ) -> None:
        """Test validating extract config file."""
        # Arrange
        config = {
            "externalId": "ctx_extract_assets_by_pattern_default",
            "config": {
                "data": {
                    "patterns": [{"category": "equipment", "samples": ["P-101"]}],
                },
            },
        }
        config_path = temp_dir / "extract_config.yaml"
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f)

        mock_validate_extract.return_value = []
        mock_format_errors.return_value = "✅ Configuration is valid!"

        # Import after patching
        from validate_config import validate_config_file

        # Act
        result = validate_config_file(config_path)

        # Assert
        assert result is True
        mock_validate_extract.assert_called_once()
        mock_format_errors.assert_called_once()

    @patch("validate_config.validate_extract_config")
    @patch("validate_config.validate_hierarchy_config")
    @patch("validate_config.format_validation_errors")
    def test_validate_config_file_hierarchy_config(
        self,
        mock_format_errors: Mock,
        mock_validate_hierarchy: Mock,
        mock_validate_extract: Mock,
        temp_dir: Path,
    ) -> None:
        """Test validating hierarchy config file."""
        # Arrange
        config = {
            "externalId": "ctx_create_asset_hierarchy_default",
            "config": {
                "data": {
                    "hierarchy_levels": ["site", "plant"],
                    "scope": [],
                },
            },
        }
        config_path = temp_dir / "hierarchy_config.yaml"
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f)

        mock_validate_hierarchy.return_value = []
        mock_format_errors.return_value = "✅ Configuration is valid!"

        # Import after patching
        from validate_config import validate_config_file

        # Act
        result = validate_config_file(config_path)

        # Assert
        assert result is True
        mock_validate_hierarchy.assert_called_once()
        mock_format_errors.assert_called_once()

    @patch("validate_config.validate_extract_config")
    @patch("validate_config.validate_hierarchy_config")
    @patch("validate_config.format_validation_errors")
    def test_validate_config_file_write_config(
        self,
        mock_format_errors: Mock,
        mock_validate_hierarchy: Mock,
        mock_validate_extract: Mock,
        temp_dir: Path,
    ) -> None:
        """Test validating write config file (basic validation)."""
        # Arrange
        config = {
            "externalId": "ctx_write_asset_hierarchy_default",
            "config": {
                "data": {},
            },
        }
        config_path = temp_dir / "write_config.yaml"
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f)

        # Import after patching
        from validate_config import validate_config_file

        # Act
        result = validate_config_file(config_path)

        # Assert
        assert result is True
        # Write config should not call validation functions
        mock_validate_extract.assert_not_called()
        mock_validate_hierarchy.assert_not_called()

    def test_validate_config_file_nonexistent_file(self, temp_dir: Path) -> None:
        """Test validating nonexistent config file."""
        # Arrange
        nonexistent_path = temp_dir / "nonexistent.yaml"

        # Import
        from validate_config import validate_config_file

        # Act
        result = validate_config_file(nonexistent_path)

        # Assert
        assert result is False

    def test_validate_config_file_invalid_yaml(self, temp_dir: Path) -> None:
        """Test validating invalid YAML file."""
        # Arrange
        invalid_path = temp_dir / "invalid.yaml"
        invalid_path.write_text("invalid: yaml: content: [unclosed")

        # Import
        from validate_config import validate_config_file

        # Act
        result = validate_config_file(invalid_path)

        # Assert
        assert result is False

    @patch("validate_config.validate_extract_config")
    @patch("validate_config.format_validation_errors")
    def test_validate_config_file_with_errors(
        self,
        mock_format_errors: Mock,
        mock_validate_extract: Mock,
        temp_dir: Path,
    ) -> None:
        """Test validating config file with errors."""
        # Arrange
        config = {
            "externalId": "ctx_extract_assets_by_pattern_default",
            "config": {
                "data": {
                    "patterns": [],
                },
            },
        }
        config_path = temp_dir / "extract_config.yaml"
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f)

        mock_validate_extract.return_value = ["❌ Error 1", "❌ Error 2"]
        mock_format_errors.return_value = "❌ ERRORS"

        # Import after patching
        from validate_config import validate_config_file

        # Act
        result = validate_config_file(config_path)

        # Assert
        assert result is False

    @patch("validate_config.validate_extract_config")
    @patch("validate_config.format_validation_errors")
    def test_validate_config_file_with_warnings_only(
        self,
        mock_format_errors: Mock,
        mock_validate_extract: Mock,
        temp_dir: Path,
    ) -> None:
        """Test validating config file with warnings only (should pass)."""
        # Arrange
        config = {
            "externalId": "ctx_extract_assets_by_pattern_default",
            "config": {
                "data": {
                    "patterns": [{"category": "equipment", "samples": []}],
                },
            },
        }
        config_path = temp_dir / "extract_config.yaml"
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f)

        mock_validate_extract.return_value = ["⚠️  Warning 1"]
        mock_format_errors.return_value = "⚠️  WARNINGS"

        # Import after patching
        from validate_config import validate_config_file

        # Act
        result = validate_config_file(config_path)

        # Assert
        assert result is True  # Warnings are OK, should pass

    @patch("validate_config.validate_extract_config")
    @patch("validate_config.validate_hierarchy_config")
    @patch("validate_config.format_validation_errors")
    def test_validate_config_file_unknown_config_type(
        self,
        mock_format_errors: Mock,
        mock_validate_hierarchy: Mock,
        mock_validate_extract: Mock,
        temp_dir: Path,
    ) -> None:
        """Test validating config file with unknown config type."""
        # Arrange
        config = {
            "externalId": "unknown_config_type",
            "config": {
                "data": {},
            },
        }
        config_path = temp_dir / "unknown_config.yaml"
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f)

        # Import after patching
        from validate_config import validate_config_file

        # Act
        result = validate_config_file(config_path)

        # Assert
        assert result is True  # Unknown config type is OK (just a warning)


class TestMainFunction:
    """Test main function."""

    @patch("validate_config.validate_config_file")
    @patch("sys.argv", ["validate_config.py"])
    def test_main_with_default_configs(self, mock_validate: Mock) -> None:
        """Test main function with default configs."""
        # Arrange
        mock_validate.return_value = True

        # Import
        from validate_config import main

        # Act
        main()

        # Assert
        # Should be called for default configs
        assert mock_validate.call_count >= 1

    @patch("validate_config.validate_config_file")
    @patch("sys.argv", ["validate_config.py", "custom_config.yaml"])
    @patch("pathlib.Path.exists")
    def test_main_with_custom_config(
        self,
        mock_exists: Mock,
        mock_validate: Mock,
    ) -> None:
        """Test main function with custom config path."""
        # Arrange
        mock_exists.return_value = True
        mock_validate.return_value = True

        # Import
        from validate_config import main

        # Act
        main()

        # Assert
        mock_validate.assert_called_once()

    @patch("sys.argv", ["validate_config.py", "nonexistent.yaml"])
    @patch("pathlib.Path.exists")
    def test_main_with_nonexistent_config(self, mock_exists: Mock) -> None:
        """Test main function with nonexistent config file."""
        # Arrange
        mock_exists.return_value = False

        # Import
        from validate_config import main

        # Act & Assert
        with pytest.raises(SystemExit):
            main()

    @patch("validate_config.validate_config_file")
    @patch("sys.argv", ["validate_config.py"])
    def test_main_with_validation_failure(self, mock_validate: Mock) -> None:
        """Test main function when validation fails."""
        # Arrange
        mock_validate.return_value = False

        # Import
        from validate_config import main

        # Act & Assert
        with pytest.raises(SystemExit) as exc_info:
            main()

        # Assert exit code is 1 (failure)
        assert exc_info.value.code == 1

    @patch("validate_config.validate_config_file")
    @patch("sys.argv", ["validate_config.py"])
    def test_main_with_validation_success(self, mock_validate: Mock) -> None:
        """Test main function when validation succeeds."""
        # Arrange
        mock_validate.return_value = True

        # Import
        from validate_config import main

        # Act & Assert
        with pytest.raises(SystemExit) as exc_info:
            main()

        # Assert exit code is 0 (success)
        assert exc_info.value.code == 0
