"""
Test configuration and fixtures for CDF Key Extraction System.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pytest


@pytest.fixture
def sample_config() -> Dict[str, Any]:
    """Sample configuration for testing."""
    return {
        "cdf": {
            "api_key": "test-api-key",
            "project": "test-project",
            "base_url": "https://api.cognite.com",
            "timeout": 30,
            "max_retries": 3,
            "batch_size": 100,
            "max_concurrent_requests": 10,
            "data_model_space": "sp_enterprise_process_industry",
            "data_model_version": "v1",
        },
        "source_views": [
            {
                "view_external_id": "CogniteEquipment",
                "view_space": "sp_enterprise_process_industry",
                "view_version": "v1",
                "entity_type": "asset",
                "batch_size": 100,
                "include_properties": ["name", "description", "equipmentType"],
                "filter": {
                    "equals": {"property": ["node", "type"], "value": "Equipment"}
                },
            }
        ],
        "extraction_rules": [
            {
                "name": "standard_pump_tag",
                "description": "Extracts standard pump tags",
                "method": "regex",
                "pattern": r"\bP[-_]?\d{2,4}[A-Z]?\b",
                "extraction_type": "candidate_key",
                "source_fields": [{"field_name": "name", "required": True}],
            }
        ],
        "aliasing": {
            "rules": [
                {
                    "name": "normalize_separators",
                    "type": "character_substitution",
                    "enabled": True,
                    "priority": 10,
                    "config": {"substitutions": {"_": "-", " ": "-"}},
                }
            ],
            "max_aliases_per_key": 50,
            "min_confidence": 0.7,
            "preserve_original": True,
        },
        "validation": {
            "min_confidence": 0.5,
            "max_keys_per_type": 10,
            "min_alias_length": 2,
            "max_alias_length": 50,
            "allowed_characters": "A-Za-z0-9-_/. ",
        },
        "deployment": {
            "environment": "test",
            "function_name": "test-key-extraction-function",
            "workflow_name": "test-key-extraction-workflow",
            "transformation_prefix": "test-key-extraction",
            "storage_prefix": "test-key-extraction-results",
            "workflow_timeout": 3600,
            "retry_policy": {"max_retries": 3, "retry_delay": 60},
        },
    }


@pytest.fixture
def sample_entities() -> list[Dict[str, Any]]:
    """Sample entities for testing."""
    return [
        {
            "id": "asset_001",
            "name": "P-101",
            "description": "Main feed pump for Tank T-301, controlled by FIC-2001",
            "metadata": {"site": "Plant_A", "equipmentType": "pump"},
        },
        {
            "id": "asset_002",
            "name": "FCV-2001A",
            "description": "Flow control valve for reactor feed system",
            "metadata": {"site": "Plant_B", "equipmentType": "valve"},
        },
        {
            "id": "asset_003",
            "name": "T-201",
            "description": "Storage tank with level indicator LIC-201",
            "metadata": {"site": "Plant_A", "equipmentType": "tank"},
        },
    ]


@pytest.fixture
def sample_tags() -> list[str]:
    """Sample tags for aliasing tests."""
    return ["P-101", "FCV-2001A", "T_201", "LIC-201", "FIC-2001"]


@pytest.fixture
def mock_cdf_client():
    """Mock CDF client for testing."""
    # This would be implemented with unittest.mock
    pass


@pytest.fixture
def results_output_dir():
    """Return the path to the results output directory."""
    return Path(__file__).parent / "results"


def save_extraction_results(
    output_dir: Path,
    filename: str,
    results: List[Dict[str, Any]],
    summary: Dict[str, Any] = None,
):
    """Save extraction results to a JSON file in the results directory.

    Args:
        output_dir: Path to the results directory
        filename: Name of the output file (will be prefixed with timestamp)
        results: List of extraction results
        summary: Optional summary statistics
    """
    # Ensure results directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not filename.endswith(".json"):
        filename += ".json"

    output_path = output_dir / filename

    # Prepare output data
    output_data = {
        "timestamp": datetime.now().isoformat(),
        "summary": summary or {},
        "results": results,
    }

    # Save to file
    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2)

    return output_path


def save_aliasing_results(
    output_dir: Path,
    filename: str,
    results: List[Dict[str, Any]],
    summary: Dict[str, Any] = None,
):
    """Save aliasing results to a JSON file in the results directory.

    Args:
        output_dir: Path to the results directory
        filename: Name of the output file (will be prefixed with timestamp)
        results: List of aliasing results
        summary: Optional summary statistics
    """
    # Ensure results directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not filename.endswith(".json"):
        filename += ".json"

    output_path = output_dir / filename

    # Prepare output data
    output_data = {
        "timestamp": datetime.now().isoformat(),
        "summary": summary or {},
        "results": results,
    }

    # Save to file
    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2)

    return output_path
