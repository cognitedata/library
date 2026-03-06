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

# Import CDF Config model internally (not exposed to users)
try:
    from .config import Config, load_config_parameters

    CDF_CONFIG_AVAILABLE = True
except ImportError:
    CDF_CONFIG_AVAILABLE = False
    Config = None
    load_config_parameters = None


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

    # Convert extraction rules
    for cdf_rule in cdf_config.data.extraction_rules:
        engine_rule = _convert_extraction_rule(cdf_rule)
        if engine_rule:
            engine_config["extraction_rules"].append(engine_rule)

    # Add field_selection_strategy to config if needed
    engine_config["field_selection_strategy"] = cdf_config.data.field_selection_strategy

    return engine_config


def _convert_extraction_rule(cdf_rule: Any) -> Dict[str, Any]:
    """Convert a CDF ExtractionRuleConfig to engine format."""
    try:
        engine_rule = {
            "name": cdf_rule.name,
            "priority": cdf_rule.priority,
            "enabled": True,  # Default to enabled
            "method": cdf_rule.method,
            "extraction_type": "candidate_key",  # Default
            "source_fields": _convert_source_fields(cdf_rule.source_fields),
            "config": {},
        }

        # Convert method-specific parameters
        method_params = cdf_rule.method_parameters

        if cdf_rule.method == "regex":
            engine_rule["pattern"] = method_params.pattern
            engine_rule["case_sensitive"] = not method_params.regex_options.ignore_case
            engine_rule["min_confidence"] = 0.7  # Default

            # Store regex-specific config
            engine_rule["config"].update(
                {
                    "early_termination": getattr(
                        method_params, "early_termination", False
                    ),
                    "max_matches_per_field": getattr(
                        method_params, "max_matches_per_field", None
                    ),
                    "validation_pattern": getattr(
                        method_params, "validation_pattern", None
                    ),
                    "regex_options": {
                        "multiline": method_params.regex_options.multiline,
                        "dotall": method_params.regex_options.dotall,
                        "ignore_case": method_params.regex_options.ignore_case,
                        "unicode": method_params.regex_options.unicode,
                    },
                }
            )

            # Handle capture groups and reassembly
            if (
                hasattr(method_params, "capture_groups")
                and method_params.capture_groups
            ):
                engine_rule["config"]["capture_groups"] = [
                    {
                        "name": cg.name
                        if hasattr(cg, "name")
                        else (cg.get("name") if isinstance(cg, dict) else str(cg)),
                        "component_type": getattr(cg, "component_type", None)
                        if hasattr(cg, "component_type")
                        else (
                            cg.get("component_type") if isinstance(cg, dict) else None
                        ),
                    }
                    for cg in method_params.capture_groups
                ]

            if (
                hasattr(method_params, "reassemble_format")
                and method_params.reassemble_format
            ):
                engine_rule["config"][
                    "reassemble_format"
                ] = method_params.reassemble_format

        elif cdf_rule.method == "fixed width":
            # Convert fixed width parameters
            engine_rule["config"] = _convert_fixed_width_params(method_params)

        elif cdf_rule.method == "token reassembly":
            # Convert token reassembly parameters
            engine_rule["config"] = _convert_token_reassembly_params(method_params)

        elif cdf_rule.method == "heuristic":
            # Convert heuristic parameters
            engine_rule["config"] = _convert_heuristic_params(method_params)
            engine_rule["min_confidence"] = method_params.scoring.min_confidence

        return engine_rule

    except Exception as e:
        logger.error(f"Error converting extraction rule {cdf_rule.name}: {e}")
        return None


def _convert_source_fields(cdf_source_fields: Any) -> List[Dict[str, Any]]:
    """Convert CDF source fields to engine format."""
    engine_fields = []

    if cdf_source_fields is None:
        return engine_fields

    # Handle single field or list
    fields_list = (
        cdf_source_fields
        if isinstance(cdf_source_fields, list)
        else [cdf_source_fields]
    )

    for field in fields_list:
        engine_field = {
            "field_name": field.field_name,
            "field_type": field.field_type,
            "required": field.required,
            "priority": field.priority if hasattr(field, "priority") else 1,
            "max_length": field.max_length if hasattr(field, "max_length") else 1000,
        }

        if hasattr(field, "separator") and field.separator:
            engine_field["separator"] = field.separator

        if hasattr(field, "role") and field.role:
            engine_field["role"] = field.role

        if hasattr(field, "preprocessing") and field.preprocessing:
            engine_field["preprocessing"] = field.preprocessing

        engine_fields.append(engine_field)

    return engine_fields


def _convert_fixed_width_params(params: Any) -> Dict[str, Any]:
    """Convert fixed width method parameters."""
    config = {}

    if hasattr(params, "encoding") and params.encoding:
        config["encoding"] = params.encoding

    if hasattr(params, "record_delimiter") and params.record_delimiter:
        config["record_delimiter"] = params.record_delimiter

    if hasattr(params, "record_length") and params.record_length:
        config["record_length"] = params.record_length

    if hasattr(params, "line_pattern") and params.line_pattern:
        config["line_pattern"] = params.line_pattern

    if hasattr(params, "skip_lines") and params.skip_lines:
        config["skip_lines"] = params.skip_lines

    if hasattr(params, "stop_on_empty") and params.stop_on_empty:
        config["stop_on_empty"] = params.stop_on_empty

    if hasattr(params, "field_definitions") and params.field_definitions:
        config["field_definitions"] = [
            {
                "name": fd.name,
                "start_position": fd.start_position,
                "end_position": fd.end_position,
                "field_type": fd.field_type if hasattr(fd, "field_type") else "unknown",
                "required": fd.required,
                "trim": fd.trim if hasattr(fd, "trim") else True,
                "line": fd.line if hasattr(fd, "line") else None,
                "padding": fd.padding if hasattr(fd, "padding") else "none",
            }
            for fd in params.field_definitions
        ]

    return config


def _convert_token_reassembly_params(params: Any) -> Dict[str, Any]:
    """Convert token reassembly method parameters."""
    config = {}

    if hasattr(params, "tokenization") and params.tokenization:
        tokenization = {
            "separator_pattern": params.tokenization.separator_pattern,
            "token_patterns": [
                {
                    "name": tp.name,
                    "pattern": tp.pattern,
                    "position": tp.position,
                    "required": tp.required,
                    "component_type": getattr(tp, "component_type", "unknown"),
                }
                for tp in params.tokenization.token_patterns
            ],
        }

        if hasattr(params.tokenization, "min_tokens"):
            tokenization["min_tokens"] = params.tokenization.min_tokens
        if hasattr(params.tokenization, "max_tokens"):
            tokenization["max_tokens"] = params.tokenization.max_tokens

        config["tokenization"] = tokenization

    if hasattr(params, "assembly_rules") and params.assembly_rules:
        config["assembly_rules"] = [
            {
                "name": ar.name,
                "format": ar.format,
                "conditions": ar.conditions if hasattr(ar, "conditions") else {},
            }
            for ar in params.assembly_rules
        ]

    if hasattr(params, "validation") and params.validation:
        config["validation"] = {
            "validate_assembled": params.validation.validate_assembled,
            "validation_pattern": params.validation.validation_pattern
            if hasattr(params.validation, "validation_pattern")
            else None,
        }

    return config


def _convert_heuristic_params(params: Any) -> Dict[str, Any]:
    """Convert heuristic method parameters."""
    config = {}

    if hasattr(params, "heuristic_strategies") and params.heuristic_strategies:
        config["heuristic_strategies"] = []
        for strategy in params.heuristic_strategies:
            strategy_dict = {
                "name": strategy.strategy_id,
                "weight": strategy.weight,
                "method": strategy.method,
                "rules": [],
            }

            # Convert strategy rules based on method type
            for rule in strategy.rules:
                if strategy.method == "positional_detection":
                    rule_dict = {
                        "position": rule.position,
                        "pattern": rule.pattern,
                        "confidence_boost": rule.confidence_boost,
                    }
                    if hasattr(rule, "keywords"):
                        rule_dict["keywords"] = rule.keywords
                    strategy_dict["rules"].append(rule_dict)
                elif strategy.method == "frequency_analysis":
                    rule_dict = {
                        "analyze_corpus": rule.analyze_corpus,
                        "min_frequency": rule.min_frequency,
                        "pattern_stability_threshold": rule.pattern_stability_threshold,
                        "common_prefix_detection": rule.common_prefix_detection,
                        "common_suffix_detection": rule.common_suffix_detection,
                    }
                    strategy_dict["rules"].append(rule_dict)
                elif strategy.method == "context_inference":
                    rule_dict = {
                        "surrounding_keywords": dict(rule.surrounding_keywords)
                        if hasattr(rule, "surrounding_keywords")
                        else {"positive": [], "negative": []},
                        "context_window": rule.context_window,
                        "keyword_proximity_bonus": rule.keyword_proximity_bonus,
                    }
                    if hasattr(rule, "equipment_type_correlation"):
                        rule_dict["equipment_type_correlation"] = {
                            "enable": rule.equipment_type_correlation.enabled,
                            "type_indicators": dict(
                                rule.equipment_type_correlation.type_indicators
                            ),
                        }
                    strategy_dict["rules"].append(rule_dict)

            config["heuristic_strategies"].append(strategy_dict)

    if hasattr(params, "scoring") and params.scoring:
        config["scoring"] = {
            "aggregation_method": params.scoring.aggregation_method,
            "min_confidence": params.scoring.min_confidence,
            "normalize_scores": params.scoring.normalize_scores,
        }

    if hasattr(params, "confidence_modifiers") and params.confidence_modifiers:
        config["confidence_modifiers"] = params.confidence_modifiers

    if hasattr(params, "validation") and params.validation:
        config["validation"] = params.validation

    return config


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

    # Add validation config (including blacklist_keywords)
    validation_config = data_section.get("validation", {})
    if validation_config:
        engine_config["validation"].update(validation_config)

    # Add source_views if present (needed for CDF querying)
    source_views = data_section.get("source_views", [])
    if source_views:
        engine_config["source_views"] = source_views

    return engine_config


def _convert_rule_dict_to_engine_format(
    rule_data: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Convert a rule dictionary directly to engine format without Pydantic."""
    try:
        method = rule_data.get("method", "regex")
        engine_rule = {
            "name": rule_data.get("name", "unnamed_rule"),
            "description": rule_data.get("description", ""),
            "priority": rule_data.get("priority", 100),
            "enabled": rule_data.get("enabled", True),
            "method": method,
            "scope_filters": rule_data.get("scope_filters", {}),
            "extraction_type": rule_data.get(
                "extraction_type", "candidate_key"
            ),  # Use rule's extraction_type or default to candidate_key
            "source_fields": _convert_source_fields_dict(
                rule_data.get("source_fields", [])
            ),
            "field_selection_strategy": rule_data.get(
                "field_selection_strategy"
            ),  # Per-rule strategy
            "config": {},
        }

        # Normalize method name (convert "fixed width" -> "fixed_width", "token reassembly" -> "token_reassembly")
        method_normalized = method.replace(" ", "_")
        engine_rule["method"] = method_normalized

        # Handle method-specific parameters
        params = rule_data.get("parameters", {})

        if method == "regex" or method_normalized == "regex":
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

        elif method == "fixed width" or method_normalized == "fixed_width":
            engine_rule["config"] = _convert_fixed_width_params_dict(params)

        elif method == "token reassembly" or method_normalized == "token_reassembly":
            engine_rule["config"] = _convert_token_reassembly_params_dict(params)

        elif method == "heuristic" or method_normalized == "heuristic":
            engine_rule["config"] = _convert_heuristic_params_dict(params)
            if "scoring" in params:
                engine_rule["min_confidence"] = params["scoring"].get(
                    "min_confidence", 0.7
                )

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
                "field_type": field.get("field_type", "string"),
                "required": field.get("required", False),
                "priority": field.get("priority", 1),
                "max_length": field.get("max_length", 1000),
            }

            if "separator" in field:
                engine_field["separator"] = field["separator"]
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

    # Extract config section
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


def load_config_from_cdf(client: Any, pipeline_ext_id: str) -> Dict[str, Any]:
    """
    Load CDF extraction pipeline config from CDF and convert to engine format.

    Args:
        client: CogniteClient instance
        pipeline_ext_id: External ID of the extraction pipeline

    Returns:
        Engine config dictionary compatible with KeyExtractionEngine

    Raises:
        ImportError: If CDF config models are not available
        RuntimeError: If config cannot be retrieved from CDF
    """
    if not CDF_CONFIG_AVAILABLE:
        raise ImportError(
            "CDF Config models not available. "
            "fn_dm_key_extraction module is required for loading from CDF."
        )

    function_data = {"ExtractionPipelineExtId": pipeline_ext_id}
    cdf_config = load_config_parameters(client, function_data)
    return convert_cdf_config_to_engine_config(cdf_config)
