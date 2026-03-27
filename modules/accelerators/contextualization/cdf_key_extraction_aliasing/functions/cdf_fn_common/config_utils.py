"""
Configuration utility functions.

This module provides shared utilities for loading and parsing
configuration files, primarily YAML-based configurations.
"""

from typing import Any, Dict, Optional

import yaml

from .logger import CogniteFunctionLogger


def load_config_from_yaml(
    file_path: str, logger: Optional[CogniteFunctionLogger] = None
) -> Dict[str, Any]:
    """
    Load configuration from YAML file.

    Args:
        file_path: Path to the YAML configuration file
        logger: Optional logger instance for error logging

    Returns:
        Dictionary containing the parsed configuration, or empty dict on error
    """
    # Use provided logger or create a default one
    if logger is None:
        logger = CogniteFunctionLogger("INFO", False)

    try:
        with open(file_path, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.verbose("ERROR", f"Configuration file not found: {file_path}")
        return {}
    except yaml.YAMLError as e:
        logger.verbose("ERROR", f"Error parsing YAML configuration: {e}")
        return {}
