"""
Tests for AssetTagClassifier class.

Tests cover pattern matching, classification, validation, and file I/O operations.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
import yaml

# Add module root to path
module_root = Path(__file__).parent.parent
if str(module_root) not in sys.path:
    sys.path.insert(0, str(module_root))

from asset_tag_classifier import AssetTagClassifier, classify_assets_from_file


class TestAssetTagClassifierInitialization:
    """Test AssetTagClassifier initialization."""

    def test_init_with_valid_config(self, sample_config_yaml: Path) -> None:
        """Test initialization with valid config file."""
        # Arrange & Act
        classifier = AssetTagClassifier(sample_config_yaml)

        # Assert
        assert classifier.config_path == sample_config_yaml
        assert classifier.config is not None
        assert classifier.patterns is not None
        assert classifier.compiled_patterns is not None

    def test_init_with_nonexistent_config_raises_error(self, temp_dir: Path) -> None:
        """Test initialization with nonexistent config file raises FileNotFoundError."""
        # Arrange
        nonexistent_path = temp_dir / "nonexistent.yaml"

        # Act & Assert
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            AssetTagClassifier(nonexistent_path)

    def test_init_with_document_patterns(
        self, sample_config_yaml: Path, sample_document_patterns_yaml: Path
    ) -> None:
        """Test initialization with document patterns."""
        # Arrange & Act
        classifier = AssetTagClassifier(
            sample_config_yaml, sample_document_patterns_yaml
        )

        # Assert
        assert classifier.document_patterns_path == sample_document_patterns_yaml
        assert classifier.compiled_document_patterns is not None
        assert len(classifier.compiled_document_patterns) > 0

    def test_init_without_document_patterns(self, sample_config_yaml: Path) -> None:
        """Test initialization without document patterns."""
        # Arrange & Act
        classifier = AssetTagClassifier(sample_config_yaml)

        # Assert
        assert classifier.document_patterns_path is None
        assert classifier.compiled_document_patterns == []


class TestAssetTagClassifierPatternMatching:
    """Test pattern matching functionality."""

    def test_classify_tag_with_matching_pattern(self, sample_config_yaml: Path) -> None:
        """Test classifying a tag that matches a pattern."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)
        tag = "FCV-101"

        # Act
        result = classifier.classify_tag(tag)

        # Assert
        assert result is not None
        assert "resourceType" in result
        assert "resourceSubType" in result
        assert "resourceDescription" in result

    def test_classify_tag_with_no_match_returns_none(
        self, sample_config_yaml: Path
    ) -> None:
        """Test classifying a tag that doesn't match any pattern."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)
        tag = "UNKNOWN-999"

        # Act
        result = classifier.classify_tag(tag)

        # Assert
        assert result is None

    def test_classify_tag_with_empty_string_returns_none(
        self, sample_config_yaml: Path
    ) -> None:
        """Test classifying an empty tag returns None."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)

        # Act
        result = classifier.classify_tag("")

        # Assert
        assert result is None

    def test_classify_tag_with_none_returns_none(
        self, sample_config_yaml: Path
    ) -> None:
        """Test classifying None returns None."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)

        # Act
        result = classifier.classify_tag(None)  # type: ignore[arg-type]

        # Assert
        assert result is None

    def test_classify_tag_priority_ordering(self, temp_dir: Path) -> None:
        """Test that patterns are matched in priority order."""
        # Arrange - Create config with overlapping patterns, different priorities
        config = {
            "asset_tag_patterns": {
                "instruments": [
                    {
                        "name": "Low Priority Pattern",
                        "pattern": r"\bFCV[-_]?\d{1,6}[A-Z]?\b",
                        "type": "INSTRUMENT",
                        "description": "Low priority",
                        "priority": 100,
                    },
                    {
                        "name": "High Priority Pattern",
                        "pattern": r"\bFCV[-_]?\d{1,6}[A-Z]?\b",
                        "type": "INSTRUMENT",
                        "description": "High priority",
                        "priority": 10,
                    },
                ],
            },
        }
        config_path = temp_dir / "priority_config.yaml"
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f)

        classifier = AssetTagClassifier(config_path)
        tag = "FCV-101"

        # Act
        result = classifier.classify_tag(tag)

        # Assert - Should match high priority pattern first
        assert result is not None
        assert result["matched_pattern"] == "High Priority Pattern"

    def test_classify_document_tag(
        self, sample_config_yaml: Path, sample_document_patterns_yaml: Path
    ) -> None:
        """Test classifying a document tag."""
        # Arrange
        classifier = AssetTagClassifier(
            sample_config_yaml, sample_document_patterns_yaml
        )
        tag = "P&ID-101"

        # Act
        result = classifier.classify_tag(tag)

        # Assert
        assert result is not None
        assert result["pattern_category"] == "document"
        assert "Document" in result["resourceType"]


class TestAssetTagClassifierValidation:
    """Test validation functionality."""

    def test_validate_tag_with_length_rule(self, temp_dir: Path) -> None:
        """Test validation with length rule."""
        # Arrange
        config = {
            "asset_tag_patterns": {
                "instruments": [
                    {
                        "name": "Test Pattern",
                        "pattern": r"\bFCV[-_]?\d{1,6}[A-Z]?\b",
                        "type": "INSTRUMENT",
                        "description": "Test",
                        "priority": 10,
                        "validation_rules": [
                            {
                                "type": "length",
                                "min": 5,
                                "max": 10,
                                "message": "Tag length must be between 5 and 10",
                            },
                        ],
                    },
                ],
            },
        }
        config_path = temp_dir / "validation_config.yaml"
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f)

        classifier = AssetTagClassifier(config_path)

        # Act - Valid tag
        valid_result = classifier.classify_tag("FCV-101", validate=True)
        # Act - Invalid tag (too short)
        invalid_result = classifier.classify_tag("FCV", validate=True)

        # Assert
        assert valid_result is not None
        assert invalid_result is None

    def test_validate_tag_with_starts_with_rule(self, temp_dir: Path) -> None:
        """Test validation with starts_with rule."""
        # Arrange
        config = {
            "asset_tag_patterns": {
                "instruments": [
                    {
                        "name": "Test Pattern",
                        "pattern": r"\bFCV[-_]?\d{1,6}[A-Z]?\b",
                        "type": "INSTRUMENT",
                        "description": "Test",
                        "priority": 10,
                        "validation_rules": [
                            {
                                "type": "starts_with",
                                "value": "FCV",
                                "case_sensitive": True,
                                "message": "Tag must start with FCV",
                            },
                        ],
                    },
                ],
            },
        }
        config_path = temp_dir / "validation_config.yaml"
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f)

        classifier = AssetTagClassifier(config_path)

        # Act
        valid_result = classifier.classify_tag("FCV-101", validate=True)
        invalid_result = classifier.classify_tag("PCV-101", validate=True)

        # Assert
        assert valid_result is not None
        assert invalid_result is None

    def test_validate_tag_with_ends_with_rule(self, temp_dir: Path) -> None:
        """Test validation with ends_with rule."""
        # Arrange
        config = {
            "asset_tag_patterns": {
                "instruments": [
                    {
                        "name": "Test Pattern",
                        "pattern": r"\bFCV[-_]?\d{1,6}[A-Z]?\b",
                        "type": "INSTRUMENT",
                        "description": "Test",
                        "priority": 10,
                        "validation_rules": [
                            {
                                "type": "ends_with",
                                "value": ["101", "102"],
                                "case_sensitive": True,
                                "message": "Tag must end with 101 or 102",
                            },
                        ],
                    },
                ],
            },
        }
        config_path = temp_dir / "validation_config.yaml"
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f)

        classifier = AssetTagClassifier(config_path)

        # Act
        valid_result = classifier.classify_tag("FCV-101", validate=True)
        invalid_result = classifier.classify_tag("FCV-999", validate=True)

        # Assert
        assert valid_result is not None
        assert invalid_result is None

    def test_validate_tag_without_validation(self, temp_dir: Path) -> None:
        """Test that validation can be disabled."""
        # Arrange
        config = {
            "asset_tag_patterns": {
                "instruments": [
                    {
                        "name": "Test Pattern",
                        "pattern": r"\bFCV[-_]?\d{1,6}[A-Z]?\b",
                        "type": "INSTRUMENT",
                        "description": "Test",
                        "priority": 10,
                        "validation_rules": [
                            {
                                "type": "length",
                                "min": 10,
                                "max": 20,
                                "message": "Tag must be 10-20 characters",
                            },
                        ],
                    },
                ],
            },
        }
        config_path = temp_dir / "validation_config.yaml"
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f)

        classifier = AssetTagClassifier(config_path)

        # Act - With validation (should fail)
        result_with_validation = classifier.classify_tag("FCV-101", validate=True)
        # Act - Without validation (should pass)
        result_without_validation = classifier.classify_tag("FCV-101", validate=False)

        # Assert
        assert result_with_validation is None
        assert result_without_validation is not None


class TestAssetTagClassifierClassification:
    """Test classification functionality."""

    def test_classify_assets_list(
        self, sample_config_yaml: Path, sample_assets_json: Path
    ) -> None:
        """Test classifying a list of assets."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)
        with open(sample_assets_json, "r", encoding="utf-8") as f:
            assets = json.load(f)

        # Act
        classified = classifier.classify_assets(assets, tag_field="externalId")

        # Assert
        assert isinstance(classified, list)
        assert len(classified) == len(assets)
        assert "resourceType" in classified[0]

    def test_classify_assets_single_dict(self, sample_config_yaml: Path) -> None:
        """Test classifying a single asset dictionary."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)
        asset = {"externalId": "FCV-101", "name": "Flow Control Valve 101"}

        # Act
        classified = classifier.classify_assets(asset, tag_field="externalId")

        # Assert
        assert isinstance(classified, dict)
        assert "resourceType" in classified

    def test_classify_assets_with_custom_tag_field(
        self, sample_config_yaml: Path
    ) -> None:
        """Test classifying assets with custom tag field name."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)
        assets = [{"tag": "FCV-101", "name": "Flow Control Valve 101"}]

        # Act
        classified = classifier.classify_assets(assets, tag_field="tag")

        # Assert
        assert len(classified) == 1
        assert "resourceType" in classified[0]

    def test_classify_assets_skip_classified(self, sample_config_yaml: Path) -> None:
        """Test skipping already classified assets."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)
        assets = [
            {
                "externalId": "FCV-101",
                "resourceType": "Control Valve",
                "resourceSubType": "Flow Control Valve",
            },
            {"externalId": "PIT-201", "name": "Pressure Transmitter 201"},
        ]

        # Act
        classified = classifier.classify_assets(
            assets, tag_field="externalId", skip_classified=True
        )

        # Assert
        assert len(classified) == 2
        # First asset should remain unchanged
        assert classified[0]["resourceType"] == "Control Valve"
        # Second asset should be classified
        assert "resourceType" in classified[1]

    def test_classify_assets_with_empty_tag(self, sample_config_yaml: Path) -> None:
        """Test classifying assets with empty tag field."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)
        assets = [{"externalId": "", "name": "Asset without tag"}]

        # Act
        classified = classifier.classify_assets(assets, tag_field="externalId")

        # Assert
        assert len(classified) == 1
        assert classified[0]["resourceType"] == ""
        assert classified[0]["resourceSubType"] == ""

    def test_classify_assets_invalid_input_raises_error(
        self, sample_config_yaml: Path
    ) -> None:
        """Test that invalid input raises ValueError."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)

        # Act & Assert
        with pytest.raises(
            ValueError, match="Assets must be a dictionary or list of dictionaries"
        ):
            classifier.classify_assets("invalid", tag_field="externalId")


class TestAssetTagClassifierFileIO:
    """Test file I/O operations."""

    def test_load_assets_from_json(
        self, sample_config_yaml: Path, sample_assets_json: Path
    ) -> None:
        """Test loading assets from JSON file."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)

        # Act
        assets = classifier.load_assets(sample_assets_json)

        # Assert
        assert isinstance(assets, list)
        assert len(assets) > 0
        assert "externalId" in assets[0]

    def test_load_assets_from_yaml(
        self, sample_config_yaml: Path, sample_assets_yaml: Path
    ) -> None:
        """Test loading assets from YAML file."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)

        # Act
        assets = classifier.load_assets(sample_assets_yaml)

        # Assert
        assert isinstance(assets, list)
        assert len(assets) > 0

    def test_load_assets_nonexistent_file_raises_error(
        self, sample_config_yaml: Path, temp_dir: Path
    ) -> None:
        """Test loading from nonexistent file raises FileNotFoundError."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)
        nonexistent_path = temp_dir / "nonexistent.json"

        # Act & Assert
        with pytest.raises(FileNotFoundError, match="Assets file not found"):
            classifier.load_assets(nonexistent_path)

    def test_load_assets_unsupported_format_raises_error(
        self, sample_config_yaml: Path, temp_dir: Path
    ) -> None:
        """Test loading from unsupported format raises ValueError."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)
        invalid_path = temp_dir / "assets.txt"
        invalid_path.write_text("invalid content")

        # Act & Assert
        with pytest.raises(ValueError, match="Unsupported file format"):
            classifier.load_assets(invalid_path)

    def test_save_assets_to_json(
        self, sample_config_yaml: Path, temp_dir: Path
    ) -> None:
        """Test saving assets to JSON file."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)
        assets = [{"externalId": "FCV-101", "resourceType": "Control Valve"}]
        output_path = temp_dir / "output.json"

        # Act
        classifier.save_assets(assets, output_path, format="json")

        # Assert
        assert output_path.exists()
        with open(output_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        assert len(loaded) == 1
        assert loaded[0]["externalId"] == "FCV-101"

    def test_save_assets_to_yaml(
        self, sample_config_yaml: Path, temp_dir: Path
    ) -> None:
        """Test saving assets to YAML file."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)
        assets = [{"externalId": "FCV-101", "resourceType": "Control Valve"}]
        output_path = temp_dir / "output.yaml"

        # Act
        classifier.save_assets(assets, output_path, format="yaml")

        # Assert
        assert output_path.exists()
        with open(output_path, "r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f)
        assert len(loaded) == 1
        assert loaded[0]["externalId"] == "FCV-101"

    def test_save_assets_unsupported_format_raises_error(
        self, sample_config_yaml: Path, temp_dir: Path
    ) -> None:
        """Test saving to unsupported format raises ValueError."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)
        assets = [{"externalId": "FCV-101"}]
        output_path = temp_dir / "output.xml"

        # Act & Assert
        with pytest.raises(ValueError, match="Unsupported output format"):
            classifier.save_assets(assets, output_path, format="xml")


class TestAssetTagClassifierHelperMethods:
    """Test helper methods."""

    def test_to_camel_case(self, sample_config_yaml: Path) -> None:
        """Test _to_camel_case conversion."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)

        # Act & Assert
        assert classifier._to_camel_case("HEAT_EXCHANGER") == "Heat Exchanger"
        assert classifier._to_camel_case("CONTROL_VALVE") == "Control Valve"
        assert classifier._to_camel_case("") == ""
        assert classifier._to_camel_case("single") == "Single"

    def test_extract_process_variable_from_tag(self, sample_config_yaml: Path) -> None:
        """Test extracting process variable from tag."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)

        # Act & Assert
        assert classifier._extract_process_variable_from_tag("FCV-101") == "Flow"
        assert classifier._extract_process_variable_from_tag("PCV-201") == "Pressure"
        assert classifier._extract_process_variable_from_tag("TCV-301") == "Temperature"
        assert classifier._extract_process_variable_from_tag("LCV-401") == "Level"

    def test_extract_instrument_function_from_tag(
        self, sample_config_yaml: Path
    ) -> None:
        """Test extracting instrument function from tag."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)

        # Act & Assert
        assert (
            classifier._extract_instrument_function_from_tag("FIC-101") == "Controller"
        )
        assert (
            classifier._extract_instrument_function_from_tag("PIT-201") == "Transmitter"
        )
        assert classifier._extract_instrument_function_from_tag("FE-301") == "Element"

    def test_is_already_classified(self, sample_config_yaml: Path) -> None:
        """Test checking if asset is already classified."""
        # Arrange
        classifier = AssetTagClassifier(sample_config_yaml)

        # Act & Assert
        classified_asset = {
            "resourceType": "Control Valve",
            "resourceSubType": "Flow Control Valve",
        }
        assert classifier._is_already_classified(classified_asset) is True

        unclassified_asset = {"externalId": "FCV-101"}
        assert classifier._is_already_classified(unclassified_asset) is False

        # Test with unclassified placeholder values
        placeholder_asset = {"resourceType": "undefined", "resourceSubType": "null"}
        assert classifier._is_already_classified(placeholder_asset) is False


class TestClassifyAssetsFromFile:
    """Test the convenience function classify_assets_from_file."""

    def test_classify_assets_from_file_json(
        self, sample_config_yaml: Path, sample_assets_json: Path, temp_dir: Path
    ) -> None:
        """Test classifying assets from JSON file."""
        # Arrange
        output_path = temp_dir / "classified.json"

        # Act
        classified = classify_assets_from_file(
            sample_assets_json,
            sample_config_yaml,
            tag_field="externalId",
            output_path=output_path,
            output_format="json",
        )

        # Assert
        assert isinstance(classified, list)
        assert len(classified) > 0
        assert output_path.exists()

    def test_classify_assets_from_file_yaml(
        self, sample_config_yaml: Path, sample_assets_yaml: Path, temp_dir: Path
    ) -> None:
        """Test classifying assets from YAML file."""
        # Arrange
        output_path = temp_dir / "classified.yaml"

        # Act
        classified = classify_assets_from_file(
            sample_assets_yaml,
            sample_config_yaml,
            tag_field="externalId",
            output_path=output_path,
            output_format="yaml",
        )

        # Assert
        assert isinstance(classified, list)
        assert len(classified) > 0
        assert output_path.exists()

    def test_classify_assets_from_file_without_output(
        self, sample_config_yaml: Path, sample_assets_json: Path
    ) -> None:
        """Test classifying assets from file without saving output."""
        # Act
        classified = classify_assets_from_file(
            sample_assets_json,
            sample_config_yaml,
            tag_field="externalId",
        )

        # Assert
        assert isinstance(classified, list)
        assert len(classified) > 0
