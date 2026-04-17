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

    # Add field_selection_strategy to config if needed
    engine_config["field_selection_strategy"] = cdf_config.data.field_selection_strategy

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
    """
    Serialize ExtractionRuleConfig to the same shape as YAML rule dicts
    (name, handler, parameters, source_fields, ...).
    """
    data = cdf_rule.model_dump(mode="python", by_alias=True)
    data["name"] = getattr(cdf_rule, "name", None) or data.get("rule_id", "unnamed_rule")
    sf = data.get("source_fields")
    if sf is not None and not isinstance(sf, list):
        data["source_fields"] = [sf]
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

    # Add field_selection_strategy
    engine_config["field_selection_strategy"] = data_section.get(
        "field_selection_strategy", "merge_all"
    )

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
    """Convert a rule dictionary to engine format; outputs canonical handler and extraction_type."""
    try:
        method_raw = rule_data.get("handler")
        method_canonical = normalize_method(method_raw).value
        extraction_type = (
            get_extraction_type_from_rule(rule_data).value
            if get_extraction_type_from_rule
            else rule_data.get("extraction_type", "candidate_key")
        )
        engine_rule = {
            "name": rule_data.get("name", "unnamed_rule"),
            "description": rule_data.get("description", ""),
            "priority": rule_data.get("priority", 100),
            "enabled": rule_data.get("enabled", True),
            "handler": method_canonical,
            "scope_filters": rule_data.get("scope_filters", {}),
            "extraction_type": extraction_type,
            "source_fields": _convert_source_fields_dict(
                rule_data.get("source_fields", [])
            ),
            "field_selection_strategy": rule_data.get(
                "field_selection_strategy"
            ),  # Per-rule strategy
            "config": {},
        }

        # Handle method-specific parameters
        params = rule_data.get("parameters", {})

        if method_canonical == "regex":
            engine_rule["pattern"] = params.get("pattern", "")
            engine_rule["case_sensitive"] = not params.get("regex_options", {}).get(
                "ignore_case", True
            )
            engine_rule["min_confidence"] = 0.7

            regex_opts = params.get("regex_options", {})
            engine_rule["config"] = {
                "early_termination": params.get("early_termination", False),
                "max_matches_per_field": params.get("max_matches_per_field", None),
                "validation_pattern": params.get("validation_pattern", None),
                "regex_options": {
                    "multiline": regex_opts.get("multiline", False),
                    "dotall": regex_opts.get("dotall", False),
                    "ignore_case": regex_opts.get("ignore_case", True),
                    "unicode": regex_opts.get("unicode", True),
                },
            }

            if "capture_groups" in params:
                engine_rule["config"]["capture_groups"] = params["capture_groups"]
            if "reassemble_format" in params:
                engine_rule["config"]["reassemble_format"] = params["reassemble_format"]

        elif method_canonical == "fixed width":
            engine_rule["config"] = _convert_fixed_width_params_dict(params)

        elif method_canonical == "token reassembly":
            engine_rule["config"] = _convert_token_reassembly_params_dict(params)

        elif method_canonical == "heuristic":
            engine_rule["config"] = _convert_heuristic_params_dict(params)
            if "scoring" in params:
                engine_rule["min_confidence"] = params["scoring"].get(
                    "min_confidence", 0.7
                )

        elif method_canonical == "passthrough":
            engine_rule["config"] = {}
            engine_rule["min_confidence"] = params.get("min_confidence", 1.0)

        return engine_rule

    except Exception as e:
        logger.error(f"Error converting rule {rule_data.get('name', 'unknown')}: {e}")
        return None


def _convert_source_fields_dict(source_fields_data: Any) -> List[Dict[str, Any]]:
    """Convert source fields dictionary/list to engine format."""
    if not source_fields_data:
        return []

    if not isinstance(source_fields_data, list):
        source_fields_data = [source_fields_data]

    engine_fields = []
    for field in source_fields_data:
        if isinstance(field, dict):
            engine_field = {
                "field_name": field.get("field_name", ""),
                "required": field.get("required", False),
                "priority": field.get("priority", 1),
                "max_length": field.get("max_length", 1000),
            }

            if "role" in field:
                engine_field["role"] = field["role"]
            if "preprocessing" in field:
                engine_field["preprocessing"] = field["preprocessing"]

            engine_fields.append(engine_field)

    return engine_fields


def _convert_fixed_width_params_dict(params: Dict[str, Any]) -> Dict[str, Any]:
    """Convert fixed width params dict to engine format."""
    config = {}

    if "field_definitions" in params:
        config["field_definitions"] = [
            {
                "name": fd.get("name", ""),
                "start_position": fd.get("start_position", 0),
                "end_position": fd.get("end_position", 0),
                "field_type": fd.get("field_type", "unknown"),
                "required": fd.get("required", True),
                "trim": fd.get("trim", True),
                "line": fd.get("line", None),
                "padding": fd.get("padding", "none"),
            }
            for fd in params["field_definitions"]
        ]

    for key in [
        "encoding",
        "record_delimiter",
        "record_length",
        "line_pattern",
        "skip_lines",
        "stop_on_empty",
    ]:
        if key in params:
            config[key] = params[key]

    return config


def _convert_token_reassembly_params_dict(params: Dict[str, Any]) -> Dict[str, Any]:
    """Convert token reassembly params dict to engine format."""
    config = {}

    if "tokenization" in params:
        tokenization = params["tokenization"]
        config["tokenization"] = {
            "separator_pattern": tokenization.get("separator_pattern", ""),
            "token_patterns": [
                {
                    "name": tp.get("name", ""),
                    "pattern": tp.get("pattern", ""),
                    "position": tp.get("position", 0),
                    "required": tp.get("required", True),
                    "component_type": tp.get("component_type", "unknown"),
                }
                for tp in tokenization.get("token_patterns", [])
            ],
        }

        if "min_tokens" in tokenization:
            config["tokenization"]["min_tokens"] = tokenization["min_tokens"]
        if "max_tokens" in tokenization:
            config["tokenization"]["max_tokens"] = tokenization["max_tokens"]

    if "assembly_rules" in params:
        config["assembly_rules"] = [
            {
                "name": ar.get("name", ""),
                "format": ar.get("format", ""),
                "conditions": ar.get("conditions", {}),
            }
            for ar in params["assembly_rules"]
        ]

    if "validation" in params:
        config["validation"] = {
            "validate_assembled": params["validation"].get("validate_assembled", True),
            "validation_pattern": params["validation"].get("validation_pattern", None),
        }

    return config


def _convert_heuristic_params_dict(params: Dict[str, Any]) -> Dict[str, Any]:
    """Convert heuristic params dict to engine format."""
    config = {}

    if "heuristic_strategies" in params:
        config["heuristic_strategies"] = []
        for strategy in params["heuristic_strategies"]:
            strategy_dict = {
                "name": strategy.get("strategy_id", strategy.get("name", "")),
                "weight": strategy.get("weight", 0.25),
                "method": strategy.get("method", ""),
                "rules": [],
            }

            for rule in strategy.get("rules", []):
                rule_dict = {}
                method = strategy.get("method", "")

                if method == "positional_detection":
                    rule_dict = {
                        "position": rule.get("position", 0),
                        "pattern": rule.get("pattern", ""),
                        "confidence_boost": rule.get("confidence_boost", 0),
                    }
                    if "keywords" in rule:
                        rule_dict["keywords"] = rule["keywords"]

                elif method == "frequency_analysis":
                    rule_dict = {
                        "analyze_corpus": rule.get("analyze_corpus", False),
                        "min_frequency": rule.get("min_frequency", 3),
                    }

                elif method == "context_inference":
                    rule_dict = {
                        "surrounding_keywords": rule.get(
                            "surrounding_keywords", {"positive": [], "negative": []}
                        ),
                        "context_window": rule.get("context_window", 20),
                        "keyword_proximity_bonus": rule.get(
                            "keyword_proximity_bonus", 0
                        ),
                    }

                if rule_dict:
                    strategy_dict["rules"].append(rule_dict)

            config["heuristic_strategies"].append(strategy_dict)

    if "scoring" in params:
        config["scoring"] = {
            "aggregation_method": params["scoring"].get(
                "aggregation_method", "weighted_average"
            ),
            "min_confidence": params["scoring"].get("min_confidence", 0.5),
            "normalize_scores": params["scoring"].get("normalize_scores", True),
        }

    if "confidence_modifiers" in params:
        config["confidence_modifiers"] = params["confidence_modifiers"]

    if "validation" in params:
        config["validation"] = params["validation"]

    return config


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
