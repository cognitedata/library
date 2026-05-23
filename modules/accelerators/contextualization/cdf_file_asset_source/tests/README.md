# Tests for create_asset_hierarchy_from_files

This directory contains comprehensive pytest tests for the `create_asset_hierarchy_from_files` module.

## Test Structure

### Test Files

- **`test_asset_tag_classifier.py`** - Tests for the `AssetTagClassifier` class
  - Initialization and configuration loading
  - Pattern matching and classification
  - Validation rules
  - File I/O operations (JSON/YAML)
  - Helper methods (camel case conversion, process variable extraction, etc.)

- **`test_common.py`** - Tests for common utility functions
  - Cognite client setup
  - File ID extraction from nodes
  - Uploaded time extraction from nodes
  - Constants

- **`test_config_validator.py`** - Tests for configuration validation utilities
  - Hierarchy configuration validation
  - Extract configuration validation
  - Error formatting

- **`test_validate_config.py`** - Tests for the validate_config.py script
  - Configuration file validation
  - Main function execution
  - Error handling

### Fixtures

- **`conftest.py`** - Shared pytest fixtures
  - `temp_dir` - Temporary directory for test files
  - `sample_config_yaml` - Sample asset tag pattern configuration
  - `sample_document_patterns_yaml` - Sample document patterns configuration
  - `sample_assets_json` - Sample assets in JSON format
  - `sample_assets_yaml` - Sample assets in YAML format
  - `mock_cognite_client` - Mock CogniteClient for testing
  - `sample_hierarchy_config` - Sample hierarchy configuration
  - `sample_extract_config` - Sample extract configuration
  - `invalid_hierarchy_config` - Invalid hierarchy configuration for testing
  - `invalid_extract_config` - Invalid extract configuration for testing

## Running Tests

### Run all tests
```bash
pytest modules/create_asset_hierarchy_from_files/tests/
```

### Run specific test file
```bash
pytest modules/create_asset_hierarchy_from_files/tests/test_asset_tag_classifier.py
```

### Run specific test class
```bash
pytest modules/create_asset_hierarchy_from_files/tests/test_asset_tag_classifier.py::TestAssetTagClassifierInitialization
```

### Run specific test
```bash
pytest modules/create_asset_hierarchy_from_files/tests/test_asset_tag_classifier.py::TestAssetTagClassifierInitialization::test_init_with_valid_config
```

### Run with coverage
```bash
pytest modules/create_asset_hierarchy_from_files/tests/ --cov=modules/create_asset_hierarchy_from_files --cov-report=html
```

### Run with verbose output
```bash
pytest modules/create_asset_hierarchy_from_files/tests/ -v
```

## Test Coverage

The test suite covers:

- ✅ Asset tag classification and pattern matching
- ✅ Configuration validation (hierarchy and extract configs)
- ✅ File I/O operations (JSON/YAML)
- ✅ Validation rules (length, starts_with, ends_with, etc.)
- ✅ Cognite client setup and utilities
- ✅ Error handling and edge cases
- ✅ Helper methods and utilities

## Test Standards

Tests follow the project's testing standards:

- **Arrange-Act-Assert (AAA) pattern** for clear test structure
- **Descriptive test names** that explain what is being tested
- **One assertion per test** when possible
- **Mocking external dependencies** (CogniteClient, file system, etc.)
- **Parametrized tests** for similar scenarios
- **Edge case testing** (empty inputs, None values, invalid data)

## Dependencies

Tests require:
- `pytest` - Testing framework
- `pytest-mock` - Mocking utilities
- `pyyaml` - YAML file handling
- Standard library modules (unittest.mock, pathlib, etc.)
