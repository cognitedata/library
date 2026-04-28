"""
CDF Adapter for Aliasing Engine

This module provides adapters to convert CDF extraction pipeline configurations
to the format expected by the AliasingEngine, enabling compatibility with
CDF workflow formats while maintaining existing functionality.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Default post-alias validation: validation_rules (regex) + min_confidence + cap.
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


def _pydantic_aliasing_rule_to_rule_data(cdf_rule: Any) -> Dict[str, Any]:
    """
    Normalize a Pydantic aliasing rule or plain object to the same dict shape as YAML rules
    before `_convert_aliasing_rule_dict_to_engine_format`.

    Method-specific config always comes from `_convert_aliasing_config(cdf_rule)` so behavior
    matches the previous getattr-based path. Top-level fields use `model_dump` when available.
    """
    if hasattr(cdf_rule, "model_dump"):
        data = cdf_rule.model_dump(mode="python", by_alias=True)
        name = data.get("name") or data.get("rule_id") or "unnamed_rule"
        scope = data.get("scope_filters")
        if scope is None:
            scope = getattr(cdf_rule, "scope_filters", None) or {}
        conditions = data.get("conditions")
        if conditions is None:
            conditions = getattr(cdf_rule, "conditions", None) or {}
        val = data.get("validation")
        return {
            "name": name,
            "handler": data.get("handler", "character_substitution"),
            "enabled": data.get("enabled", True),
            "priority": data.get("priority", 50),
            "preserve_original": data.get("preserve_original", True),
            "config": _convert_aliasing_config(cdf_rule),
            "conditions": conditions if isinstance(conditions, dict) else {},
            "description": data.get("description", ""),
            "scope_filters": scope if isinstance(scope, dict) else {},
            "validation": val if isinstance(val, dict) else {},
        }

    val = getattr(cdf_rule, "validation", None)
    return {
        "name": getattr(cdf_rule, "name", "unnamed_rule"),
        "handler": getattr(cdf_rule, "handler", "character_substitution"),
        "enabled": getattr(cdf_rule, "enabled", True),
        "priority": getattr(cdf_rule, "priority", 50),
        "preserve_original": getattr(cdf_rule, "preserve_original", True),
        "config": _convert_aliasing_config(cdf_rule),
        "conditions": getattr(cdf_rule, "conditions", {}) or {},
        "description": getattr(cdf_rule, "description", ""),
        "scope_filters": getattr(cdf_rule, "scope_filters", {}) or {},
        "validation": val if isinstance(val, dict) else {},
    }


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
        "validation": dict(_DEFAULT_ALIASING_VALIDATION),
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
    if hasattr(cdf_config, "aliasing") and cdf_config.aliasing:
        av = getattr(cdf_config.aliasing, "validation", None)
        if av is not None:
            if hasattr(av, "model_dump"):
                aliasing_config["validation"].update(
                    av.model_dump(mode="python", exclude_none=False)
                )
            elif isinstance(av, dict):
                aliasing_config["validation"].update(av)
            else:
                aliasing_config["validation"].update(dict(av))

    return aliasing_config


def _convert_aliasing_rule(cdf_rule: Any) -> Optional[Dict[str, Any]]:
    """Convert a CDF aliasing rule to engine format via the same dict path as YAML."""
    try:
        rule_data = _pydantic_aliasing_rule_to_rule_data(cdf_rule)
        return _convert_aliasing_rule_dict_to_engine_format(rule_data)

    except Exception as e:
        logger.error(f"Error converting aliasing rule: {e}")
        return None


def _convert_aliasing_config(cdf_rule: Any) -> Dict[str, Any]:
    """Convert aliasing rule configuration based on handler."""
    rule_type = getattr(cdf_rule, "handler", "character_substitution")
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

    elif rule_type == "semantic_expansion":
        if hasattr(cdf_rule, "type_mappings"):
            config["type_mappings"] = dict(cdf_rule.type_mappings)
        if hasattr(cdf_rule, "format_templates"):
            config["format_templates"] = list(cdf_rule.format_templates)
        if hasattr(cdf_rule, "auto_detect"):
            config["auto_detect"] = cdf_rule.auto_detect
        if hasattr(cdf_rule, "include_isa_semantic_preset"):
            config["include_isa_semantic_preset"] = (
                cdf_rule.include_isa_semantic_preset
            )
        if hasattr(cdf_rule, "isa_preset"):
            config["isa_preset"] = cdf_rule.isa_preset
        if hasattr(cdf_rule, "isa_preset_path"):
            config["isa_preset_path"] = cdf_rule.isa_preset_path

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

    elif rule_type == "alias_mapping_table":
        nested = getattr(cdf_rule, "config", None)
        if isinstance(nested, dict):
            config.update(nested)

    # For composite/config-based rules, just pass through the config
    if hasattr(cdf_rule, "config"):
        config.update(dict(cdf_rule.config))

    return config


def _convert_aliasing_rule_dict_to_engine_format(
    rule_data: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Convert an aliasing rule dictionary directly to engine format without Pydantic."""
    try:
        from cdf_fn_common.pipeline_io import pipeline_io_dict_for_engine

        # Only convert if rule is enabled (or if enabled is not specified, default to True)
        enabled = rule_data.get("enabled", True)

        handler = rule_data.get("handler") or rule_data.get("type") or "character_substitution"
        io = pipeline_io_dict_for_engine(rule_data)
        val = rule_data.get("validation")
        engine_rule = {
            "name": rule_data.get("name", "unnamed_rule"),
            "handler": handler,
            "enabled": enabled,
            "priority": rule_data.get("priority", 50),
            "preserve_original": io["preserve_original"],
            "pipeline_input": io["pipeline_input"],
            "pipeline_output": io["pipeline_output"],
            "config": rule_data.get("config", {}),
            "scope_filters": rule_data.get("scope_filters", {}),
            "conditions": rule_data.get("conditions", {}),
            "description": rule_data.get("description", ""),
            "validation": val if isinstance(val, dict) else {},
        }
        return engine_rule
    except Exception as e:
        logger.error(
            f"Error converting aliasing rule {rule_data.get('name', 'unknown')}: {e}"
        )
        return None


def _convert_pathways_rules_for_engine(data_section: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize ``pathways.steps`` rule dicts to engine format (same as ``aliasing_rules``)."""
    pw = data_section.get("pathways")
    if not isinstance(pw, dict):
        return None
    steps_in = pw.get("steps")
    if not isinstance(steps_in, list):
        return None
    steps_out: List[Dict[str, Any]] = []
    for step in steps_in:
        if not isinstance(step, dict):
            continue
        mode = str(step.get("mode") or "sequential").strip().lower()
        if mode == "sequential":
            rules_raw = step.get("rules") or []
            converted: List[Dict[str, Any]] = []
            if isinstance(rules_raw, list):
                for r in rules_raw:
                    if isinstance(r, dict):
                        er = _convert_aliasing_rule_dict_to_engine_format(r)
                        if er:
                            converted.append(er)
            steps_out.append({"mode": "sequential", "rules": converted})
        elif mode == "parallel":
            branches_out: List[Dict[str, Any]] = []
            for br in step.get("branches") or []:
                rules_raw: List[Any] = []
                if isinstance(br, dict):
                    rules_raw = br.get("rules") or []
                elif isinstance(br, list):
                    rules_raw = br
                converted = []
                if isinstance(rules_raw, list):
                    for r in rules_raw:
                        if isinstance(r, dict):
                            er = _convert_aliasing_rule_dict_to_engine_format(r)
                            if er:
                                converted.append(er)
                branches_out.append({"rules": converted})
            steps_out.append({"mode": "parallel", "branches": branches_out})
    return {"steps": steps_out}


def scope_has_key_extraction_rules(scope_document: Optional[Dict[str, Any]]) -> bool:
    """True when scope includes non-empty ``key_extraction.config.data.extraction_rules`` (for attaching pipelines)."""
    if not isinstance(scope_document, dict):
        return False
    ke = scope_document.get("key_extraction")
    if not isinstance(ke, dict):
        return False
    kcfg = ke.get("config")
    if not isinstance(kcfg, dict):
        return False
    data = kcfg.get("data")
    if not isinstance(data, dict):
        return False
    er = data.get("extraction_rules")
    return isinstance(er, list) and len(er) > 0


def attach_extraction_aliasing_pipelines(
    aliasing_config: Dict[str, Any],
    scope_document: Optional[Dict[str, Any]],
) -> None:
    """Populate ``extraction_aliasing_pipelines`` on *aliasing_config* from scope ``key_extraction``.

    Each extraction rule id is mapped to its ``aliasing_pipeline`` list, or ``[]`` when the field
    is absent. The aliasing engine treats a **non-empty** list as an exclusive per-rule pipeline;
    an **empty** list means "no per-rule overrides" and execution **falls through** to global
    ``pathways`` / ``aliasing_rules`` (same as when no pipelines map exists).
    """
    if not isinstance(scope_document, dict):
        return
    ke = scope_document.get("key_extraction")
    if not isinstance(ke, dict):
        return
    kcfg = ke.get("config")
    if not isinstance(kcfg, dict):
        return
    data = kcfg.get("data")
    if not isinstance(data, dict):
        return
    erules = data.get("extraction_rules")
    if not isinstance(erules, list):
        return
    pipelines: Dict[str, List[Any]] = {}
    for rule in erules:
        if not isinstance(rule, dict):
            continue
        rid = rule.get("rule_id") or rule.get("name")
        if rid is None or not str(rid).strip():
            continue
        ap = rule.get("aliasing_pipeline")
        if ap is None:
            pipelines[str(rid).strip()] = []
        elif isinstance(ap, list):
            pipelines[str(rid).strip()] = ap
    if pipelines:
        aliasing_config["extraction_aliasing_pipelines"] = pipelines


def _convert_yaml_direct_to_aliasing_config(
    config_data: Dict[str, Any],
    *,
    scope_document: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Convert YAML config data directly to aliasing engine format.

    This is a fallback method when Pydantic models are not available.
    It converts the config structure assuming it matches the expected format.
    """
    aliasing_config = {
        "rules": [],
        "validation": dict(_DEFAULT_ALIASING_VALIDATION),
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

    pw_engine = _convert_pathways_rules_for_engine(data_section)
    if pw_engine is not None and pw_engine.get("steps"):
        aliasing_config["pathways"] = pw_engine

    # Add validation if present
    if "validation" in data_section:
        aliasing_config["validation"].update(data_section["validation"])

    attach_extraction_aliasing_pipelines(aliasing_config, scope_document)

    return aliasing_config
