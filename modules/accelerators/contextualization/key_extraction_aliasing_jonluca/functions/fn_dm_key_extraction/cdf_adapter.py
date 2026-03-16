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
        "extraction_rules": cdf_config.data.extraction_rules,  # Pass directly now
        "validation": {"min_confidence": 0.5, "max_keys_per_type": 1000},
        "field_selection_strategy": cdf_config.data.field_selection_strategy,
    }

    return engine_config


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
            "Loading config without Pydantic validation not supported in simplified adapter."
        )
        raise NotImplementedError(
            "Direct YAML loading without Pydantic validation has been removed. "
            "Please ensure CDF Config models are available."
        )

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
    for key in ["encoding", "record_delimiter", "record_length", "line_pattern", "skip_lines", "stop_on_empty"]:
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
            {"name": ar.get("name", ""), "format": ar.get("format", ""), "conditions": ar.get("conditions", {})}
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
        config["heuristic_strategies"] = [
            {
                "name": strategy.get("strategy_id", strategy.get("name", "")),
                "weight": strategy.get("weight", 0.25),
            }
            for strategy in params["heuristic_strategies"]
        ]
    return config


def _convert_rule_dict_to_engine_format(rule_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert a rule dictionary to engine format without Pydantic."""
    try:
        method = rule_data.get("method", "regex")
        method_normalized = method.replace(" ", "_")
        engine_rule = {
            "name": rule_data.get("name", rule_data.get("rule_id", "unnamed_rule")),
            "priority": rule_data.get("priority", 100),
            "enabled": True,
            "method": method_normalized,
            "extraction_type": rule_data.get("extraction_type", "candidate_key"),
            "source_fields": _convert_source_fields_dict(rule_data.get("source_fields", [])),
            "field_selection_strategy": rule_data.get("field_selection_strategy"),
            "config": {},
        }
        params = rule_data.get("parameters", rule_data.get("config", {}))
        if method == "regex" or method_normalized == "regex":
            engine_rule["pattern"] = params.get("pattern", "")
            engine_rule["case_sensitive"] = not (params.get("regex_options") or {}).get("ignore_case", True)
            engine_rule["min_confidence"] = 0.7
            regex_opts = params.get("regex_options", {})
            engine_rule["config"] = {
                "early_termination": params.get("early_termination", False),
                "max_matches_per_field": params.get("max_matches_per_field"),
                "validation_pattern": params.get("validation_pattern"),
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
                engine_rule["min_confidence"] = (params["scoring"] or {}).get("min_confidence", 0.7)
        return engine_rule
    except Exception as e:
        logger.error(f"Error converting rule {rule_data.get('name', 'unknown')}: {e}")
        return None
