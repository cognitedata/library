"""
Configuration Management System for Key Extraction

.. note::
    **Long-term:** Production pipelines and Cognite Functions use YAML plus Pydantic models.
    This module is a parallel, schema-heavy stack kept for integration tests and tooling.
    Do not extend ``configuration_manager`` for new production features unless explicitly required.

This module provides comprehensive configuration management for key extraction
and aliasing operations. It includes YAML-based configuration, validation,
and environment management.

Features:
- YAML-based configuration files
- Configuration validation and schema checking
- Environment-specific configurations
- Aliasing settings configuration

Author: Darren Downtain
Version: 1.0.0
"""

import copy
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import jsonschema
import yaml

# Default post-alias validation (YAML template defaults).
_DEFAULT_ALIASING_VALIDATION: Dict[str, Any] = {
    "max_aliases_per_tag": 50,
    "min_confidence": 0.01,
    "validation_rules": [
        {
            "name": "alias_shape_invalid",
            "priority": 0,
            "expression_match": "fullmatch",
            "match": {
                "expressions": [
                    {
                        "pattern": r"^[0-9]{0,3}$",
                        "description": (
                            "Alias is only digits and shorter than 4 characters "
                            "(empty counts as invalid)"
                        ),
                    },
                    {
                        "pattern": r"^.{51,}$",
                        "description": "Alias exceeds maximum length 50",
                    },
                    {
                        "pattern": r"[^A-Za-z0-9_/. -]",
                        "description": (
                            "Character outside allowed set (letters, digits, _ / . - and space)"
                        ),
                    },
                ],
            },
            "confidence_modifier": {"mode": "explicit", "value": 0.0},
        },
    ],
}

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class AliasingSettings:
    """Configuration for aliasing operations."""

    rules: List[Dict[str, Any]] = field(default_factory=list)
    max_aliases_per_key: int = 50
    confidence_threshold: float = 0.7
    preserve_original: bool = True
    validation: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationSettings:
    """Key extraction validation (confidence rules, limits)."""

    min_confidence: float = 0.5
    max_keys_per_type: int = 10
    validation_rules: List[Dict[str, Any]] = field(default_factory=list)
    expression_match: Optional[str] = None


@dataclass
class KeyExtractionConfig:
    """Main configuration class for key extraction system."""

    aliasing: AliasingSettings = field(default_factory=AliasingSettings)
    validation: ValidationSettings = field(default_factory=ValidationSettings)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ConfigurationValidator:
    """Validates configuration files against schemas."""

    def __init__(self):
        """Initialize configuration validator."""
        self.schemas = self._load_schemas()

    def _load_schemas(self) -> Dict[str, Dict[str, Any]]:
        """Load JSON schemas for validation."""
        return {
            "aliasing_rule": {
                "type": "object",
                "required": ["name", "handler"],
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "handler": {
                        "type": "string",
                        "enum": [
                            "character_substitution",
                            "prefix_suffix",
                            "regex_substitution",
                            "case_transformation",
                            "semantic_expansion",
                            "related_instruments",
                            "hierarchical_expansion",
                            "document_aliases",
                            "leading_zero_normalization",
                            "pattern_recognition",
                            "pattern_based_expansion",
                            "alias_mapping_table",
                        ],
                    },
                    "enabled": {"type": "boolean"},
                    "priority": {"type": "integer", "minimum": 1, "maximum": 1000},
                    "preserve_original": {"type": "boolean"},
                    "config": {"type": "object"},
                    "conditions": {"type": "object"},
                },
            },
        }

    def validate_aliasing_rule(self, rule_config: Dict[str, Any]) -> List[str]:
        """Validate aliasing rule configuration."""
        errors = []
        try:
            jsonschema.validate(rule_config, self.schemas["aliasing_rule"])
        except jsonschema.ValidationError as e:
            errors.append(f"Aliasing rule validation error: {e.message}")
        return errors


class ConfigurationManager:
    """Manages configuration files and settings."""

    def __init__(self, config_dir: str = "config"):
        """Initialize configuration manager.

        Args:
            config_dir: Directory containing config files. Defaults to "config" (project root/config).
        """
        self.config_dir = Path(config_dir)
        self.validator = ConfigurationValidator()
        self.config_cache: Dict[str, KeyExtractionConfig] = {}

    def load_config(
        self, config_name: str = "default", environment: str = None
    ) -> KeyExtractionConfig:
        """
        Load configuration from file.

        Args:
            config_name: Name of the configuration file
            environment: Environment-specific override

        Returns:
            Loaded configuration
        """
        cache_key = f"{config_name}_{environment or 'default'}"
        if cache_key in self.config_cache:
            return self.config_cache[cache_key]

        # Load base configuration
        config_file = self.config_dir / f"{config_name}.yaml"
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")

        with open(config_file, "r") as f:
            config_data = yaml.safe_load(f)

        # Load environment-specific overrides
        if environment:
            env_file = self.config_dir / f"{config_name}_{environment}.yaml"
            if env_file.exists():
                with open(env_file, "r") as f:
                    env_data = yaml.safe_load(f)
                config_data = self._merge_configs(config_data, env_data)

        # Validate configuration
        validation_errors = self.validate_config(config_data)
        if validation_errors:
            raise ValueError(f"Configuration validation failed: {validation_errors}")

        # Convert to configuration objects
        config = self._parse_config(config_data)

        # Cache configuration
        self.config_cache[cache_key] = config

        return config

    def load_yaml_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load configuration from a YAML file.

        Args:
            file_path: Path to the YAML file

        Returns:
            Loaded configuration data
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"YAML file not found: {file_path}")

        with open(file_path, "r") as f:
            config_data = yaml.safe_load(f)

        return config_data

    def load_json_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load configuration from a JSON file.

        Args:
            file_path: Path to the JSON file

        Returns:
            Loaded configuration data
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"JSON file not found: {file_path}")

        with open(file_path, "r") as f:
            config_data = json.load(f)

        return config_data

    def save_config(
        self,
        config: KeyExtractionConfig,
        config_name: str = "default",
        environment: str = None,
    ) -> str:
        """
        Save configuration to file.

        Args:
            config: Configuration to save
            config_name: Name of the configuration file
            environment: Environment-specific file

        Returns:
            Path to saved file
        """
        # Convert configuration to dictionary
        config_data = self._config_to_dict(config)

        # Determine file path
        if environment:
            config_file = self.config_dir / f"{config_name}_{environment}.yaml"
        else:
            config_file = self.config_dir / f"{config_name}.yaml"

        # Ensure directory exists
        config_file.parent.mkdir(parents=True, exist_ok=True)

        # Save configuration
        with open(config_file, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)

        logger.info(f"Configuration saved to: {config_file}")
        return str(config_file)

    def validate_config(self, config_data: Dict[str, Any]) -> List[str]:
        """Validate configuration data."""
        errors = []

        return errors

    def _parse_config(self, config_data: Dict[str, Any]) -> KeyExtractionConfig:
        """Parse configuration data into configuration objects."""

        aliasing_data = config_data.get("aliasing", {})
        aliasing_settings = AliasingSettings(
            rules=[],
            max_aliases_per_key=aliasing_data.get("max_aliases_per_key", 50),
            confidence_threshold=aliasing_data.get("confidence_threshold", 0.7),
            preserve_original=True,
            validation=aliasing_data.get("validation", {}),
        )

        # Parse validation settings
        validation_data = config_data.get("validation", {})
        validation_settings = ValidationSettings(
            min_confidence=validation_data.get("min_confidence", 0.5),
            max_keys_per_type=validation_data.get("max_keys_per_type", 10),
            validation_rules=list(
                validation_data.get("validation_rules")
                or validation_data.get("confidence_match_rules")
                or []
            ),
            expression_match=validation_data.get("expression_match"),
        )

        return KeyExtractionConfig(
            aliasing=aliasing_settings,
            validation=validation_settings,
            metadata=config_data.get("metadata", {}),
        )

    def _config_to_dict(self, config: KeyExtractionConfig) -> Dict[str, Any]:
        """Convert configuration object to dictionary."""
        return {
            "aliasing": {
                "rules": config.aliasing.rules,
                "max_aliases_per_key": config.aliasing.max_aliases_per_key,
                "confidence_threshold": config.aliasing.confidence_threshold,
                "preserve_original": config.aliasing.preserve_original,
                "validation": config.aliasing.validation,
            },
            "validation": {
                "min_confidence": config.validation.min_confidence,
                "max_keys_per_type": config.validation.max_keys_per_type,
                "validation_rules": config.validation.validation_rules,
                **(
                    {"expression_match": config.validation.expression_match}
                    if config.validation.expression_match
                    else {}
                ),
            },
            "metadata": config.metadata,
        }

    def _merge_configs(
        self, base_config: Dict[str, Any], override_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge configuration dictionaries."""
        merged = copy.deepcopy(base_config)

        for key, value in override_config.items():
            if (
                key in merged
                and isinstance(merged[key], dict)
                and isinstance(value, dict)
            ):
                merged[key] = self._merge_configs(merged[key], value)
            else:
                merged[key] = value

        return merged

    def create_template_config(self, config_name: str = "template") -> str:
        """
        Create a template configuration file.

        Args:
            config_name: Name for the template configuration

        Returns:
            Path to created template file
        """
        template_config = {
            "aliasing": {
                "rules": [
                    {
                        "name": "normalize_separators",
                        "handler": "character_substitution",
                        "enabled": True,
                        "priority": 10,
                        "preserve_original": True,
                        "config": {"substitutions": {"_": "-", " ": "-"}},
                        "conditions": {},
                    },
                    {
                        "name": "generate_separator_variants",
                        "handler": "character_substitution",
                        "enabled": True,
                        "priority": 15,
                        "preserve_original": True,
                        "config": {
                            "substitutions": {"-": ["_", " ", ""]},
                            "cascade_substitutions": False,
                            "max_aliases_per_input": 20,
                        },
                        "conditions": {},
                    },
                    {
                        "name": "semantic_expansion",
                        "handler": "semantic_expansion",
                        "enabled": True,
                        "priority": 30,
                        "preserve_original": True,
                        "config": {
                            "type_mappings": {
                                "P": ["PUMP", "PMP"],
                                "V": ["VALVE", "VLV"],
                                "T": ["TANK", "TNK"],
                            },
                            "format_templates": ["{type}-{tag}", "{type}_{tag}"],
                            "auto_detect": True,
                        },
                        "conditions": {},
                    },
                ],
                "max_aliases_per_key": 50,
                "confidence_threshold": 0.7,
                "preserve_original": True,
                "validation": {
                    **dict(_DEFAULT_ALIASING_VALIDATION),
                    "max_aliases_per_tag": 30,
                },
            },
            "validation": {
                "min_confidence": 0.5,
                "max_keys_per_type": 10,
                "validation_rules": [],
            },
            "metadata": {
                "version": "1.0.0",
                "created_at": datetime.now().isoformat(),
                "description": "Template configuration for key extraction system",
            },
        }

        template_file = self.config_dir / f"{config_name}.yaml"
        template_file.parent.mkdir(parents=True, exist_ok=True)

        with open(template_file, "w") as f:
            yaml.dump(template_config, f, default_flow_style=False, indent=2)

        logger.info(f"Template configuration created: {template_file}")
        return str(template_file)


def load_config_from_env() -> KeyExtractionConfig:
    """Load configuration from environment variables."""
    config_manager = ConfigurationManager()

    # Create configuration from environment variables
    config_data = {
        "aliasing": {
            "rules": [],
            "max_aliases_per_key": int(os.getenv("ALIASING_MAX_ALIASES_PER_KEY", "50")),
            "confidence_threshold": float(
                os.getenv("ALIASING_CONFIDENCE_THRESHOLD", "0.7")
            ),
            "preserve_original": os.getenv("ALIASING_PRESERVE_ORIGINAL", "true").lower()
            == "true",
        },
        "validation": {
            "min_confidence": float(os.getenv("VALIDATION_MIN_CONFIDENCE", "0.5")),
            "max_keys_per_type": int(os.getenv("VALIDATION_MAX_KEYS_PER_TYPE", "10")),
            "validation_rules": [],
        },
        "metadata": {
            "loaded_from": "environment_variables",
            "loaded_at": datetime.now().isoformat(),
        },
    }

    # Validate and parse configuration
    validation_errors = config_manager.validate_config(config_data)
    if validation_errors:
        raise ValueError(
            f"Environment configuration validation failed: {validation_errors}"
        )

    return config_manager._parse_config(config_data)


def main():
    """Example usage of configuration management."""

    print("Configuration Management System for Key Extraction")
    print("=" * 60)

    # Initialize configuration manager
    config_manager = ConfigurationManager("config")

    # Create template configuration
    template_path = config_manager.create_template_config("example")
    print(f"Template configuration created: {template_path}")

    # Load configuration
    try:
        config = config_manager.load_config("example")
        print(f"Configuration loaded successfully")
        print(f"Aliasing Rules: {len(config.aliasing.rules)}")
    except Exception as e:
        print(f"Error loading configuration: {e}")

    # Validate configuration
    try:
        config_data = {
            "aliasing": {"rules": []},
            "validation": {"validation_rules": []},
        }

        errors = config_manager.validate_config(config_data)
        if errors:
            print(f"Validation errors: {errors}")
        else:
            print("Configuration validation passed")

    except Exception as e:
        print(f"Validation error: {e}")


if __name__ == "__main__":
    main()
