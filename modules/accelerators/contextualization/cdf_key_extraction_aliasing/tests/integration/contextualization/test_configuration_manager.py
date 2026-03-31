#!/usr/bin/env python3
"""
Integration Tests for Configuration Manager

This module provides integration tests for the Configuration Manager
used across key extraction and aliasing workflows.
"""

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.config.configuration_manager import (
    ConfigurationManager,
)


class TestConfigurationManager(unittest.TestCase):
    """Test cases for the Configuration Manager."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_manager = ConfigurationManager()

    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_yaml_file_loading(self):
        """Test loading configuration from YAML file."""
        yaml_file = Path(self.temp_dir) / "test_config.yaml"

        test_config = {"extraction_rules": [], "validation": {"min_confidence": 0.5}}

        with open(yaml_file, "w") as f:
            yaml.dump(test_config, f)

        loaded_config = self.config_manager.load_yaml_file(str(yaml_file))

        self.assertIsInstance(loaded_config, dict)
        self.assertIn("extraction_rules", loaded_config)
        self.assertIn("validation", loaded_config)

    def test_json_file_loading(self):
        """Test loading configuration from JSON file."""
        json_file = Path(self.temp_dir) / "test_config.json"
        test_config = {"test_key": "test_value", "nested": {"key": "value"}}
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(test_config, f)

        loaded_config = self.config_manager.load_json_file(json_file)

        self.assertEqual(loaded_config["test_key"], "test_value")
        self.assertEqual(loaded_config["nested"]["key"], "value")

    def test_missing_file_handling(self):
        """Missing YAML path raises FileNotFoundError."""
        missing_file = Path(self.temp_dir) / "missing.yaml"
        with self.assertRaises(FileNotFoundError):
            self.config_manager.load_yaml_file(missing_file)


if __name__ == "__main__":
    unittest.main()
