#!/usr/bin/env python3
"""
Integration Tests for Configuration Manager

This module provides integration tests for the Configuration Manager
used across key extraction and aliasing workflows.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.contextualization.key_extraction_aliasing.config.configuration_manager import (
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
        import shutil

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

    def test_environment_variable_loading(self):
        """Test loading configuration from environment variables."""
        # This test would require mocking environment variables
        # Implementation depends on the specific environment variable handling
        pass


if __name__ == "__main__":
    unittest.main()
