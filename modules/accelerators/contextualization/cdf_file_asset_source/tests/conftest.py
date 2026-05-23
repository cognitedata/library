"""
Shared pytest fixtures for create_asset_hierarchy_from_files tests.
"""

import json
import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, Mock

import pytest
import yaml


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for test files."""
    return tmp_path


@pytest.fixture
def sample_config_yaml(temp_dir: Path) -> Path:
    """Create a sample config YAML file for testing."""
    config = {
        "asset_tag_patterns": {
            "instruments": [
                {
                    "name": "Flow Controller",
                    "pattern": r"\bFCV[-_]?\d{1,6}[A-Z]?\b",
                    "type": "INSTRUMENT",
                    "equipment_type": "CONTROL_VALVE",
                    "description": "Flow Control Valve",
                    "priority": 10,
                },
                {
                    "name": "Pressure Transmitter",
                    "pattern": r"\bPIT[-_]?\d{1,6}[A-Z]?\b",
                    "type": "INSTRUMENT",
                    "equipment_type": "TRANSMITTER",
                    "description": "Pressure Indicator Transmitter",
                    "priority": 20,
                },
            ],
            "equipment": [
                {
                    "name": "Pump",
                    "pattern": r"\bP[-_]?\d{1,6}[A-Z]?\b",
                    "type": "EQUIPMENT",
                    "equipment_type": "PUMP",
                    "equipment_subtype": "PUMP",
                    "description": "General Pump",
                    "priority": 30,
                },
            ],
        },
    }
    config_path = temp_dir / "test_config.yaml"
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f)
    return config_path


@pytest.fixture
def sample_document_patterns_yaml(temp_dir: Path) -> Path:
    """Create a sample document patterns YAML file for testing."""
    config = {
        "document_patterns": [
            {
                "name": "Piping Instrumentation Diagram",
                "pattern": r"\bP&?ID[-_]?\d{1,6}[A-Z]?\b",
                "type": "DOCUMENT",
                "document_type": "PIPING_INSTRUMENTATION_DIAGRAM",
                "description": "Piping and Instrumentation Diagram",
                "priority": 5,
            },
        ],
    }
    config_path = temp_dir / "test_document_patterns.yaml"
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f)
    return config_path


@pytest.fixture
def sample_assets_json(temp_dir: Path) -> Path:
    """Create a sample assets JSON file for testing."""
    assets = [
        {"externalId": "FCV-101", "name": "Flow Control Valve 101"},
        {"externalId": "PIT-201", "name": "Pressure Transmitter 201"},
        {"externalId": "P-301", "name": "Pump 301"},
    ]
    assets_path = temp_dir / "test_assets.json"
    with open(assets_path, "w", encoding="utf-8") as f:
        json.dump(assets, f, indent=2)
    return assets_path


@pytest.fixture
def sample_assets_yaml(temp_dir: Path) -> Path:
    """Create a sample assets YAML file for testing."""
    assets = [
        {"externalId": "FCV-101", "name": "Flow Control Valve 101"},
        {"externalId": "PIT-201", "name": "Pressure Transmitter 201"},
        {"externalId": "P-301", "name": "Pump 301"},
    ]
    assets_path = temp_dir / "test_assets.yaml"
    with open(assets_path, "w", encoding="utf-8") as f:
        yaml.dump(assets, f)
    return assets_path


@pytest.fixture
def mock_cognite_client() -> MagicMock:
    """Create a mock CogniteClient for testing."""
    client = MagicMock()
    client.time_series = MagicMock()
    client.assets = MagicMock()
    client.files = MagicMock()
    client.data_modeling = MagicMock()
    return client


@pytest.fixture
def sample_hierarchy_config() -> Dict[str, Any]:
    """Create a sample hierarchy configuration for testing."""
    return {
        "scope_hierarchy": {
            "type": "hierarchy",
            "levels": ["site", "unit", "area", "system"],
            "locations": [
                {
                    "id": "MAIN_SITE",
                    "name": "MAIN_SITE",
                    "locations": [
                        {
                            "id": "UNIT_A",
                            "name": "UNIT_A",
                            "locations": [
                                {
                                    "id": "AREA_1",
                                    "name": "AREA_1",
                                    "locations": [
                                        {
                                            "id": "COOLING_SYS",
                                            "name": "COOLING_SYS",
                                            "files": ["CW-001", "CW-002"],
                                            "locations": [],
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ],
        },
    }


@pytest.fixture
def sample_extract_config() -> Dict[str, Any]:
    """Create a sample extract configuration for testing."""
    return {
        "externalId": "ctx_extract_assets_by_pattern_default",
        "config": {
            "data": {
                "patterns": [
                    {
                        "category": "equipment",
                        "samples": ["P-101", "V-201"],
                    },
                ],
                "limit": -1,
                "batch_size": 5,
            },
        },
    }


@pytest.fixture
def invalid_hierarchy_config() -> Dict[str, Any]:
    """Create an invalid hierarchy configuration for testing."""
    return {
        "externalId": "ctx_create_asset_hierarchy_default",
        "config": {
            "data": {
                "hierarchy_levels": [],  # Missing levels
                "scope": [],  # Missing scope
            },
        },
    }


@pytest.fixture
def invalid_extract_config() -> Dict[str, Any]:
    """Create an invalid extract configuration for testing."""
    return {
        "externalId": "ctx_extract_assets_by_pattern_default",
        "config": {
            "data": {
                "patterns": [],  # Missing patterns
                "limit": -5,  # Invalid limit
            },
        },
    }
