"""
CDF Adapter for Key Extraction Engine

This module provides adapters to convert CDF extraction pipeline configurations
to the format expected by the KeyExtractionEngine, enabling compatibility with
CDF workflow formats while maintaining existing functionality.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

# Shared rule normalization (used by both Pydantic and dict paths)
try:
    from utils.rule_utils import get_extraction_type_from_rule, normalize_method
except ImportError:
    try:
        # Local/package execution fallback
        from .utils.rule_utils import get_extraction_type_from_rule, normalize_method
    except ImportError:
        get_extraction_type_from_rule = None
        normalize_method = None

# Import CDF Config model internally (not exposed to users)
try:
    from config import Config

    CDF_CONFIG_AVAILABLE = True
except ImportError:
    try:
        # Local/package execution fallback
        from .config import Config

        CDF_CONFIG_AVAILABLE = True
    except ImportError:
        CDF_CONFIG_AVAILABLE = False
        Config = None


def convert_cdf_config_to_engine_config(cdf_config: Any) -> Dict[str, Any]:
    """
    Convert CDF Config (Pydantic model) to the dict format expected by KeyExtractionEngine.

    Args:
        cdf_config: CDF Config object from config.py (Pydantic model)

    Returns:
        Dict format compatible with KeyExtractionEngine
    """
    engine_config = {
        "extraction_rules": [],
        "validation": {"min_confidence": 0.5, "max_keys_per_type": 1000},
    }

    # Pipeline parameters (min_key_length, exclude_self_referencing_keys, etc.)
    try:
        engine_config["parameters"] = cdf_config.parameters.model_dump(
            mode="python", exclude_none=False
        )
    except Exception:
        engine_config["parameters"] = {}

    # Convert extraction rules
    for cdf_rule in cdf_config.data.extraction_rules:
        engine_rule = _convert_extraction_rule(cdf_rule)
        if engine_rule:
            engine_config["extraction_rules"].append(engine_rule)

    global_validation = getattr(cdf_config.data, "validation", None)
    if global_validation is not None:
        engine_config["validation"].update(
            global_validation.model_dump(mode="python", exclude_none=False)
        )

    source_views = getattr(cdf_config.data, "source_views", None)
    if source_views:
        engine_config["source_views"] = [
            v.model_dump(mode="python", exclude_none=False) for v in source_views
        ]
    elif getattr(cdf_config.data, "source_view", None) is not None:
        engine_config["source_views"] = [
            cdf_config.data.source_view.model_dump(mode="python", exclude_none=False)
        ]

    return engine_config


def _pydantic_extraction_rule_to_rule_data(cdf_rule: Any) -> Dict[str, Any]:
    """Serialize ExtractionRuleConfig to engine rule dicts (fields, handler, parameters, …)."""
    data = cdf_rule.model_dump(mode="python", by_alias=True)
    data["name"] = getattr(cdf_rule, "name", None) or data.get("rule_id", "unnamed_rule")
    return data


def _convert_extraction_rule(cdf_rule: Any) -> Optional[Dict[str, Any]]:
    """Convert a CDF ExtractionRuleConfig to engine format via the single dict path."""
    try:
        rule_data = _pydantic_extraction_rule_to_rule_data(cdf_rule)
        return _convert_rule_dict_to_engine_format(rule_data)
    except Exception as e:
        logger.error(
            "Error converting extraction rule %s: %s",
            getattr(cdf_rule, "name", cdf_rule),
            e,
        )
        return None


def _convert_yaml_direct_to_engine_config(
    config_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Convert YAML config data directly to engine format without Pydantic validation.

    This is a fallback method when Pydantic models are not available.
    It converts the config structure assuming it matches the expected format.
    """
    engine_config = {
        "extraction_rules": [],
        "validation": {"min_confidence": 0.5, "max_keys_per_type": 1000},
    }

    # Extract data section (can be nested under "data" or at top level)
    data_section = config_data.get("data", config_data)

    # Extract extraction rules from config.data.extraction_rules or top-level
    extraction_rules_data = data_section.get("extraction_rules", [])

    for rule_data in extraction_rules_data:
        engine_rule = _convert_rule_dict_to_engine_format(rule_data)
        if engine_rule:
            engine_config["extraction_rules"].append(engine_rule)

    # Add validation config (min_confidence, regexp_match, confidence_match_rules, …)
    validation_config = data_section.get("validation", {})
    if validation_config:
        engine_config["validation"].update(validation_config)

    # Add source_views if present (needed for CDF querying)
    source_views = data_section.get("source_views", [])
    if source_views:
        engine_config["source_views"] = source_views

    # key_extraction.config.parameters → engine (min_key_length, exclude_self_referencing_keys, …)
    top_params = config_data.get("parameters")
    if isinstance(top_params, dict) and top_params:
        engine_config["parameters"] = dict(top_params)

    return engine_config


def _convert_rule_dict_to_engine_format(
    rule_data: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Convert a rule dictionary to engine format; canonical handler id and fields[]."""
    try:
        method_raw = rule_data.get("handler")
        method_canonical = normalize_method(method_raw).value
        if method_canonical == "unsupported":
            logger.error(
                "Extraction rule %r: handler %r is not supported; skip rule",
                rule_data.get("name", rule_data.get("rule_id", "unknown")),
                method_raw,
            )
            return None
        extraction_type = (
            get_extraction_type_from_rule(rule_data).value
            if get_extraction_type_from_rule
            else rule_data.get("extraction_type", "candidate_key")
        )
        rid = rule_data.get("rule_id") or rule_data.get("name", "unnamed_rule")
        name = rule_data.get("name") or rid
        fields = rule_data.get("fields")
        if fields is None and rule_data.get("source_fields"):
            logger.error(
                "Extraction rule %r: use fields[] instead of source_fields; skip rule",
                name,
            )
            return None
        engine_rule: Dict[str, Any] = {
            "name": name,
            "rule_id": rid,
            "description": rule_data.get("description", ""),
            "priority": rule_data.get("priority", 100),
            "enabled": rule_data.get("enabled", True),
            "handler": method_canonical,
            "scope_filters": rule_data.get("scope_filters", {}),
            "extraction_type": extraction_type,
            "fields": list(fields or []),
            "entity_types": list(rule_data.get("entity_types") or []),
            "field_results_mode": rule_data.get("field_results_mode", "merge_all"),
            "result_template": rule_data.get("result_template"),
            "max_template_combinations": rule_data.get("max_template_combinations", 10000),
            "parameters": rule_data.get("parameters"),
            "validation": rule_data.get("validation"),
            "config": rule_data.get("config") if isinstance(rule_data.get("config"), dict) else {},
        }
        return engine_rule

    except Exception as e:
        logger.error(f"Error converting rule {rule_data.get('name', 'unknown')}: {e}")
        return None


def load_config_from_yaml(config_path: str, validate: bool = True) -> Dict[str, Any]:
    """
    Load CDF extraction pipeline config from YAML file and convert to engine format.

    This is a convenience function that loads a YAML config file, optionally validates it
    using the CDF Config Pydantic model, and converts it to the engine format.

    Args:
        config_path: Path to the YAML config file
        validate: Whether to validate using Pydantic model (default: True)

    Returns:
        Engine config dictionary compatible with KeyExtractionEngine

    Raises:
        ImportError: If CDF config models are not available and validate=True
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # Load YAML
    with open(config_file) as f:
        yaml_data = yaml.safe_load(f)

    if not isinstance(yaml_data, dict):
        raise ValueError("YAML root must be a mapping")
    # v1 scope document: unwrap key_extraction.config when root has key_extraction
    ke = yaml_data.get("key_extraction")
    if isinstance(ke, dict) and isinstance(ke.get("config"), dict):
        yaml_data = ke

    # Extract config section (split workflow shape or unwrapped key_extraction)
    config_data = yaml_data.get("config", yaml_data)

    # Option 1: Validate using Pydantic model (preferred if available)
    if validate and CDF_CONFIG_AVAILABLE:
        try:
            cdf_config = Config.model_validate(config_data)
            return convert_cdf_config_to_engine_config(cdf_config)
        except Exception as e:
            raise ValueError(f"Invalid config structure: {e}") from e

    # Option 2: Direct conversion without Pydantic validation (fallback)
    if not validate or not CDF_CONFIG_AVAILABLE:
        logger.warning(
            "Loading config without Pydantic validation. "
            "This is less strict but works when fn_dm_key_extraction module is not available."
        )
        return _convert_yaml_direct_to_engine_config(config_data)

    raise ImportError(
        "CDF Config models not available and validation is required. "
        "fn_dm_key_extraction module is required for loading YAML configs with validation."
    )


__all__ = [
    "convert_cdf_config_to_engine_config",
    "load_config_from_yaml",
]
