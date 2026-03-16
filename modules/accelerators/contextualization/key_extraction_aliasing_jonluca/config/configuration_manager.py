"""
Configuration Management System for Key Extraction

This module provides comprehensive configuration management for key extraction
and aliasing operations. It includes YAML-based configuration, validation,
and environment management.

Features:
- YAML-based configuration files
- Configuration validation and schema checking
- Environment-specific configurations
- Extraction rules configuration
- Aliasing rules configuration

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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ExtractionRuleSettings:
    """Configuration for extraction rules."""

    name: str
    description: str = ""
    extraction_type: str = "candidate_key"
    method: str = "regex"
    pattern: str = ""
    priority: int = 50
    enabled: bool = True
    scope_filters: Dict[str, Any] = field(default_factory=dict)
    min_confidence: float = 0.7
    case_sensitive: bool = False
    aliasing_rules: List[Dict[str, Any]] = field(default_factory=list)
    source_fields: List[Dict[str, Any]] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)


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
    """Configuration for validation operations."""

    min_confidence: float = 0.5
    max_keys_per_type: int = 10
    min_alias_length: int = 2
    max_alias_length: int = 50
    allowed_characters: str = r"A-Za-z0-9-_/. "


@dataclass
class KeyExtractionConfig:
    """Main configuration class for key extraction system."""

    extraction_rules: List[ExtractionRuleSettings] = field(default_factory=list)
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
            "extraction_rule": {
                "type": "object",
                "required": ["name", "method"],
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "description": {"type": "string"},
                    "extraction_type": {
                        "type": "string",
                        "enum": [
                            "candidate_key",
                            "foreign_key_reference",
                            "document_reference",
                        ],
                    },
                    "method": {
                        "type": "string",
                        "enum": [
                            "regex",
                            "fixed_width",
                            "token_reassembly",
                            "heuristic",
                        ],
                    },
                    "pattern": {"type": "string"},
                    "priority": {"type": "integer", "minimum": 1, "maximum": 1000},
                    "enabled": {"type": "boolean"},
                    "min_confidence": {
                        "type": "number",
                        "minimum": 0.0,
                        "maximum": 1.0,
                    },
                    "case_sensitive": {"type": "boolean"},
                    "source_fields": {"type": "array", "items": {"type": "object"}},
                },
            },
            "aliasing_rule": {
                "type": "object",
                "required": ["name", "type"],
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "type": {
                        "type": "string",
                        "enum": [
                            "character_substitution",
                            "prefix_suffix",
                            "regex_substitution",
                            "case_transformation",
                            "equipment_type_expansion",
                            "related_instruments",
                            "hierarchical_expansion",
                            "document_aliases",
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

    def validate_extraction_rule(self, rule_config: Dict[str, Any]) -> List[str]:
        """Validate extraction rule configuration."""
        errors = []
        try:
            jsonschema.validate(rule_config, self.schemas["extraction_rule"])
        except jsonschema.ValidationError as e:
            errors.append(f"Extraction rule validation error: {e.message}")

        # Additional validation for method-specific requirements
        method = rule_config.get("method")
        if method == "regex" and not rule_config.get("pattern"):
            errors.append("Regex method requires a pattern")
        elif method == "fixed_width" and not rule_config.get("config", {}).get(
            "field_definitions"
        ):
            errors.append("Fixed width method requires field_definitions in config")
        elif method == "token_reassembly" and not rule_config.get("config", {}).get(
            "tokenization"
        ):
            errors.append("Token reassembly method requires tokenization config")
        elif method == "heuristic" and not rule_config.get("config", {}).get(
            "heuristic_strategies"
        ):
            errors.append("Heuristic method requires heuristic_strategies in config")

        return errors

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

        # Validate extraction rules
        if "extraction_rules" in config_data:
            for i, rule_config in enumerate(config_data["extraction_rules"]):
                rule_errors = self.validator.validate_extraction_rule(rule_config)
                errors.extend(
                    [f"Extraction rule {i}: {error}" for error in rule_errors]
                )

        # Validate aliasing rules
        if "aliasing" in config_data and "rules" in config_data["aliasing"]:
            for i, rule_config in enumerate(config_data["aliasing"]["rules"]):
                rule_errors = self.validator.validate_aliasing_rule(rule_config)
                errors.extend([f"Aliasing rule {i}: {error}" for error in rule_errors])

        return errors

    def _parse_config(self, config_data: Dict[str, Any]) -> KeyExtractionConfig:
        """Parse configuration data into configuration objects."""

        # Parse extraction rules
        extraction_rules = []
        for rule_data in config_data.get("extraction_rules", []):
            extraction_rule = ExtractionRuleSettings(
                name=rule_data["name"],
                description=rule_data.get("description", ""),
                extraction_type=rule_data.get("extraction_type", "candidate_key"),
                method=rule_data["method"],
                pattern=rule_data.get("pattern", ""),
                priority=rule_data.get("priority", 50),
                enabled=rule_data.get("enabled", True),
                scope_filters=rule_data.get("scope_filters", {}),
                min_confidence=rule_data.get("min_confidence", 0.7),
                case_sensitive=rule_data.get("case_sensitive", False),
                aliasing_rules=rule_data.get("aliasing_rules", []),
                source_fields=rule_data.get("source_fields", []),
                config=rule_data.get("config", {}),
            )
            extraction_rules.append(extraction_rule)

        # Parse aliasing settings
        aliasing_data = config_data.get("aliasing", {})
        aliasing_settings = AliasingSettings(
            rules=aliasing_data.get("rules", []),
            max_aliases_per_key=aliasing_data.get("max_aliases_per_key", 50),
            confidence_threshold=aliasing_data.get("confidence_threshold", 0.7),
            preserve_original=aliasing_data.get("preserve_original", True),
            validation=aliasing_data.get("validation", {}),
        )

        # Parse validation settings
        validation_data = config_data.get("validation", {})
        validation_settings = ValidationSettings(
            min_confidence=validation_data.get("min_confidence", 0.5),
            max_keys_per_type=validation_data.get("max_keys_per_type", 10),
            min_alias_length=validation_data.get("min_alias_length", 2),
            max_alias_length=validation_data.get("max_alias_length", 50),
            allowed_characters=validation_data.get(
                "allowed_characters", r"A-Za-z0-9-_/. "
            ),
        )

        return KeyExtractionConfig(
            extraction_rules=extraction_rules,
            aliasing=aliasing_settings,
            validation=validation_settings,
            metadata=config_data.get("metadata", {}),
        )

    def _config_to_dict(self, config: KeyExtractionConfig) -> Dict[str, Any]:
        """Convert configuration object to dictionary."""
        return {
            "extraction_rules": [
                {
                    "name": rule.name,
                    "description": rule.description,
                    "extraction_type": rule.extraction_type,
                    "method": rule.method,
                    "pattern": rule.pattern,
                    "priority": rule.priority,
                    "enabled": rule.enabled,
                    "scope_filters": rule.scope_filters,
                    "min_confidence": rule.min_confidence,
                    "case_sensitive": rule.case_sensitive,
                    "aliasing_rules": rule.aliasing_rules,
                    "source_fields": rule.source_fields,
                    "config": rule.config,
                }
                for rule in config.extraction_rules
            ],
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
                "min_alias_length": config.validation.min_alias_length,
                "max_alias_length": config.validation.max_alias_length,
                "allowed_characters": config.validation.allowed_characters,
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
            "extraction_rules": [
                {
                    "name": "standard_pump_tag",
                    "description": "Extracts standard pump tags from equipment descriptions",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "pattern": r"\bP[-_]?\d{2,4}[A-Z]?\b",
                    "priority": 50,
                    "enabled": True,
                    "scope_filters": {"equipment_type": ["pump"]},
                    "min_confidence": 0.7,
                    "case_sensitive": False,
                    "aliasing_rules": [],
                    "source_fields": [
                        {
                            "field_name": "name",
                            "field_type": "string",
                            "required": True,
                            "priority": 1,
                        },
                        {
                            "field_name": "description",
                            "field_type": "string",
                            "required": False,
                            "priority": 2,
                        },
                    ],
                    "config": {"pattern": "P[-_]?\d{2,4}[A-Z]?"},
                },
                {
                    "name": "flow_instrument_tag",
                    "description": "Extracts ISA flow instrument tags",
                    "extraction_type": "foreign_key_reference",
                    "method": "regex",
                    "pattern": r"\bFIC[-_]?\d{4}[A-Z]?\b",
                    "priority": 30,
                    "enabled": True,
                    "scope_filters": {},
                    "min_confidence": 0.8,
                    "case_sensitive": False,
                    "aliasing_rules": [],
                    "source_fields": [
                        {
                            "field_name": "description",
                            "field_type": "string",
                            "required": False,
                        }
                    ],
                    "config": {"pattern": "FIC[-_]?\d{4}[A-Z]?"},
                },
            ],
            "aliasing": {
                "rules": [
                    {
                        "name": "normalize_separators",
                        "type": "character_substitution",
                        "enabled": True,
                        "priority": 10,
                        "preserve_original": True,
                        "config": {"substitutions": {"_": "-", " ": "-"}},
                        "conditions": {},
                    },
                    {
                        "name": "generate_separator_variants",
                        "type": "character_substitution",
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
                        "name": "equipment_type_expansion",
                        "type": "equipment_type_expansion",
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
                    "max_aliases_per_tag": 30,
                    "min_alias_length": 2,
                    "max_alias_length": 50,
                },
            },
            "validation": {
                "min_confidence": 0.5,
                "max_keys_per_type": 10,
                "min_alias_length": 2,
                "max_alias_length": 50,
                "allowed_characters": r"A-Za-z0-9-_/. ",
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
        "extraction_rules": [],
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
            "min_alias_length": int(os.getenv("VALIDATION_MIN_ALIAS_LENGTH", "2")),
            "max_alias_length": int(os.getenv("VALIDATION_MAX_ALIAS_LENGTH", "50")),
            "allowed_characters": os.getenv(
                "VALIDATION_ALLOWED_CHARACTERS", r"A-Za-z0-9-_/. "
            ),
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
        print(f"Extraction Rules: {len(config.extraction_rules)}")
        print(f"Aliasing Rules: {len(config.aliasing.rules)}")
    except Exception as e:
        print(f"Error loading configuration: {e}")

    # Validate configuration
    try:
        config_data = {
            "extraction_rules": [
                {"name": "test_rule", "method": "regex", "pattern": r"\b[A-Z]+\d+\b"}
            ],
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
