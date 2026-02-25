"""
CDF Adapter for Aliasing Engine

This module provides adapters to convert CDF extraction pipeline configurations
to the format expected by the AliasingEngine, enabling compatibility with
CDF workflow formats while maintaining existing functionality.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def convert_cdf_config_to_aliasing_config(cdf_config: Any) -> Dict[str, Any]:
    """
    Convert CDF Config (Pydantic model) to the dict format expected by AliasingEngine.

    Args:
        cdf_config: CDF Config object from config.py (Pydantic model)

    Returns:
        Dict format compatible with AliasingEngine
    """
    aliasing_config = {
        "rules": [],
        "validation": {
            "max_aliases_per_tag": 50,
            "min_alias_length": 1,
            "max_alias_length": 100,
            "allowed_characters": r"A-Za-z0-9-_/. ",
        },
    }

    # Extract aliasing rules from CDF config
    # Note: The CDF config might have aliasing rules in extraction rules
    # or in a separate aliasing section

    # If aliasing rules are embedded in extraction rules, extract them
    if hasattr(cdf_config, "data") and hasattr(cdf_config.data, "extraction_rules"):
        for rule in cdf_config.data.extraction_rules:
            if hasattr(rule, "aliasing_rules") and rule.aliasing_rules:
                for alias_rule_config in rule.aliasing_rules:
                    alias_rule = _convert_aliasing_rule(alias_rule_config)
                    if alias_rule:
                        aliasing_config["rules"].append(alias_rule)

    # If there's a dedicated aliasing section in config
    if hasattr(cdf_config, "aliasing") and cdf_config.aliasing:
        if hasattr(cdf_config.aliasing, "rules"):
            for alias_rule_config in cdf_config.aliasing.rules:
                alias_rule = _convert_aliasing_rule(alias_rule_config)
                if alias_rule:
                    aliasing_config["rules"].append(alias_rule)

    # If validation is specified in CDF config
    if hasattr(cdf_config, "aliasing") and hasattr(cdf_config.aliasing, "validation"):
        aliasing_config["validation"].update(cdf_config.aliasing.validation)

    return aliasing_config


def _convert_aliasing_rule(cdf_rule: Any) -> Dict[str, Any]:
    """Convert a CDF aliasing rule to engine format."""
    try:
        engine_rule = {
            "name": getattr(cdf_rule, "name", "unnamed_rule"),
            "type": getattr(cdf_rule, "type", "character_substitution"),
            "enabled": getattr(cdf_rule, "enabled", True),
            "priority": getattr(cdf_rule, "priority", 50),
            "preserve_original": getattr(cdf_rule, "preserve_original", True),
            "config": _convert_aliasing_config(cdf_rule),
            "conditions": getattr(cdf_rule, "conditions", {}),
            "description": getattr(cdf_rule, "description", ""),
        }

        return engine_rule

    except Exception as e:
        logger.error(f"Error converting aliasing rule: {e}")
        return None


def _convert_aliasing_config(cdf_rule: Any) -> Dict[str, Any]:
    """Convert aliasing rule configuration based on type."""
    rule_type = getattr(cdf_rule, "type", "character_substitution")
    config = {}

    if rule_type == "character_substitution":
        if hasattr(cdf_rule, "substitutions"):
            config["substitutions"] = dict(cdf_rule.substitutions)
        if hasattr(cdf_rule, "cascade_substitutions"):
            config["cascade_substitutions"] = cdf_rule.cascade_substitutions
        if hasattr(cdf_rule, "max_aliases_per_input"):
            config["max_aliases_per_input"] = cdf_rule.max_aliases_per_input
        if hasattr(cdf_rule, "bidirectional"):
            config["bidirectional"] = cdf_rule.bidirectional

    elif rule_type == "prefix_suffix":
        if hasattr(cdf_rule, "operation"):
            config["operation"] = cdf_rule.operation
        if hasattr(cdf_rule, "prefix"):
            config["prefix"] = cdf_rule.prefix
        if hasattr(cdf_rule, "suffix"):
            config["suffix"] = cdf_rule.suffix
        if hasattr(cdf_rule, "context_mapping"):
            config["context_mapping"] = dict(cdf_rule.context_mapping)
        if hasattr(cdf_rule, "resolve_from"):
            config["resolve_from"] = cdf_rule.resolve_from

    elif rule_type == "regex_substitution":
        if hasattr(cdf_rule, "patterns"):
            config["patterns"] = [
                {"pattern": p.pattern, "replacement": p.replacement}
                for p in cdf_rule.patterns
            ]
        elif hasattr(cdf_rule, "pattern"):
            config["pattern"] = cdf_rule.pattern
            if hasattr(cdf_rule, "replacement"):
                config["replacement"] = cdf_rule.replacement

    elif rule_type == "case_transformation":
        if hasattr(cdf_rule, "operations"):
            config["operations"] = list(cdf_rule.operations)
        elif hasattr(cdf_rule, "operation"):
            config["operation"] = cdf_rule.operation

    elif rule_type == "leading_zero_normalization":
        if hasattr(cdf_rule, "preserve_single_zero"):
            config["preserve_single_zero"] = cdf_rule.preserve_single_zero
        if hasattr(cdf_rule, "min_length"):
            config["min_length"] = cdf_rule.min_length

    elif rule_type == "equipment_type_expansion":
        if hasattr(cdf_rule, "type_mappings"):
            config["type_mappings"] = dict(cdf_rule.type_mappings)
        if hasattr(cdf_rule, "format_templates"):
            config["format_templates"] = list(cdf_rule.format_templates)
        if hasattr(cdf_rule, "auto_detect"):
            config["auto_detect"] = cdf_rule.auto_detect

    elif rule_type == "related_instruments":
        if hasattr(cdf_rule, "applicable_equipment_types"):
            config["applicable_equipment_types"] = list(
                cdf_rule.applicable_equipment_types
            )
        if hasattr(cdf_rule, "instrument_types"):
            config["instrument_types"] = [
                {
                    "prefix": it.prefix,
                    "applicable_to": list(it.applicable_to)
                    if hasattr(it, "applicable_to")
                    else [],
                }
                for it in cdf_rule.instrument_types
            ]
        if hasattr(cdf_rule, "format_rules"):
            config["format_rules"] = dict(cdf_rule.format_rules)

    elif rule_type == "hierarchical_expansion":
        if hasattr(cdf_rule, "hierarchy_levels"):
            config["hierarchy_levels"] = [
                {"level": hl.level, "format": hl.format}
                for hl in cdf_rule.hierarchy_levels
            ]
        if hasattr(cdf_rule, "generate_partial_paths"):
            config["generate_partial_paths"] = cdf_rule.generate_partial_paths

    elif rule_type == "document_aliases":
        if hasattr(cdf_rule, "pid_rules"):
            config["pid_rules"] = dict(cdf_rule.pid_rules)
        if hasattr(cdf_rule, "drawing_rules"):
            config["drawing_rules"] = dict(cdf_rule.drawing_rules)

    # For composite/config-based rules, just pass through the config
    if hasattr(cdf_rule, "config"):
        config.update(dict(cdf_rule.config))

    return config


def _convert_aliasing_rule_dict_to_engine_format(
    rule_data: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Convert an aliasing rule dictionary directly to engine format without Pydantic."""
    try:
        # Only convert if rule is enabled (or if enabled is not specified, default to True)
        enabled = rule_data.get("enabled", True)

        engine_rule = {
            "name": rule_data.get("name", "unnamed_rule"),
            "type": rule_data.get("type", "character_substitution"),
            "enabled": enabled,
            "priority": rule_data.get("priority", 50),
            "preserve_original": rule_data.get("preserve_original", True),
            "config": rule_data.get("config", {}),
            "scope_filters": rule_data.get("scope_filters", {}),
            "conditions": rule_data.get("conditions", {}),
            "description": rule_data.get("description", ""),
        }
        return engine_rule
    except Exception as e:
        logger.error(
            f"Error converting aliasing rule {rule_data.get('name', 'unknown')}: {e}"
        )
        return None


def _convert_yaml_direct_to_aliasing_config(
    config_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Convert YAML config data directly to aliasing engine format.

    This is a fallback method when Pydantic models are not available.
    It converts the config structure assuming it matches the expected format.
    """
    aliasing_config = {
        "rules": [],
        "validation": {
            "max_aliases_per_tag": 50,
            "min_alias_length": 2,
            "max_alias_length": 50,
            "allowed_characters": r"A-Za-z0-9-_/. ",
        },
    }

    # Extract data section - handle nested structure from pipeline configs
    # Format: {"config": {"data": {...}}}
    if "config" in config_data:
        data_section = config_data["config"].get("data", {})
    elif "data" in config_data:
        data_section = config_data["data"]
    else:
        data_section = config_data

    # Extract aliasing rules from config.data.aliasing_rules
    aliasing_rules_data = data_section.get("aliasing_rules", [])

    for rule_data in aliasing_rules_data:
        # Skip disabled rules (but still convert them so they're in the config if needed)
        enabled = rule_data.get("enabled", True)
        engine_rule = _convert_aliasing_rule_dict_to_engine_format(rule_data)
        if engine_rule:
            # Include all rules (enabled and disabled) so they can be toggled
            # The engine will check the enabled flag during execution
            aliasing_config["rules"].append(engine_rule)

    # Add validation if present
    if "validation" in data_section:
        aliasing_config["validation"].update(data_section["validation"])

    return aliasing_config
