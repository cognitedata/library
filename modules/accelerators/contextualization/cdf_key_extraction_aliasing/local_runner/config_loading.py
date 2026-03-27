"""Load pipeline YAML configs for the local CLI."""

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import yaml

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.cdf_adapter import (
    _convert_yaml_direct_to_aliasing_config,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import (
    _convert_rule_dict_to_engine_format,
)

from .paths import SCRIPT_DIR


def load_configs(
    logger: logging.Logger,
) -> Tuple[
    Dict[str, Any],
    Dict[str, Any],
    List[Dict[str, Any]],
    Optional[str],
    bool,
    Optional[str],
]:
    """Load extraction, aliasing configs, all source view configs, and persistence options."""

    pipelines_dir = SCRIPT_DIR / "pipelines"
    all_aliasing_rules = []
    alias_writeback_property: Optional[str] = None
    write_foreign_key_references = False
    foreign_key_writeback_property: Optional[str] = None

    for config_file in sorted(pipelines_dir.glob("*aliasing*.config.yaml")):
        try:
            with open(config_file, "r") as f:
                pipeline_config = yaml.safe_load(f)

            config_data = pipeline_config.get("config", {}).get("data", {})
            parameters = pipeline_config.get("config", {}).get("parameters", {})
            if isinstance(parameters, dict):
                if parameters.get("write_foreign_key_references") is True:
                    write_foreign_key_references = True
                    logger.info(
                        f"Loaded write_foreign_key_references from {config_file.name}"
                    )
                if (
                    foreign_key_writeback_property is None
                    and parameters.get("foreign_key_writeback_property") is not None
                ):
                    raw_fkp = parameters.get("foreign_key_writeback_property")
                    if isinstance(raw_fkp, str) and raw_fkp.strip():
                        foreign_key_writeback_property = raw_fkp.strip()
                        logger.info(
                            f"Loaded foreign_key_writeback_property from {config_file.name}: "
                            f"{foreign_key_writeback_property!r}"
                        )
            if (
                alias_writeback_property is None
                and isinstance(parameters, dict)
                and parameters.get("alias_writeback_property") is not None
            ):
                raw_prop = parameters.get("alias_writeback_property")
                if isinstance(raw_prop, str) and raw_prop.strip():
                    alias_writeback_property = raw_prop.strip()
                    logger.info(
                        f"Loaded alias_writeback_property from {config_file.name}: "
                        f"{alias_writeback_property!r}"
                    )

            aliasing_config = _convert_yaml_direct_to_aliasing_config(
                {"config": {"data": config_data}}
            )
            converted_rules = aliasing_config.get("rules", [])
            all_aliasing_rules.extend(converted_rules)
            logger.info(
                f"Loaded {len(converted_rules)} aliasing rules from {config_file.name}"
            )
        except Exception as e:
            logger.warning(
                f"Failed to load aliasing pipeline config {config_file.name}: {e}"
            )
            continue

    if not all_aliasing_rules:
        logger.warning("No aliasing pipeline configs found! Aliasing will be disabled.")
        aliasing_config = {"rules": [], "validation": {}}
    else:
        aliasing_config = {
            "rules": all_aliasing_rules,
            "validation": {
                "max_aliases_per_tag": 50,
                "min_alias_length": 2,
                "max_alias_length": 50,
                "allowed_characters": r"A-Za-z0-9-_/. ",
            },
        }
        logger.info(f"Total aliasing rules loaded: {len(all_aliasing_rules)}")

    all_extraction_rules = []
    all_source_views = []
    seen_source_views = set()

    for config_file in sorted(pipelines_dir.glob("*key_extraction*.config.yaml")):
        try:
            with open(config_file, "r") as f:
                pipeline_config = yaml.safe_load(f)

            config_data = pipeline_config.get("config", {}).get("data", {})

            rules = config_data.get("extraction_rules", [])
            converted_rules = []
            for rule in rules:
                converted_rule = _convert_rule_dict_to_engine_format(rule)
                if converted_rule:
                    converted_rules.append(converted_rule)
            all_extraction_rules.extend(converted_rules)
            logger.info(f"Loaded {len(converted_rules)} rules from {config_file.name}")

            source_views = config_data.get("source_views", [])
            for view in source_views:
                view_key = (
                    view.get("view_space", ""),
                    view.get("view_external_id", ""),
                    view.get("view_version", ""),
                    view.get("instance_space", ""),
                    view.get("entity_type", ""),
                )
                if view_key not in seen_source_views:
                    seen_source_views.add(view_key)
                    all_source_views.append(view)
                    logger.info(
                        f"Added source view: {view.get('view_external_id')} ({view.get('entity_type')})"
                    )
        except Exception as e:
            logger.warning(f"Failed to load pipeline config {config_file.name}: {e}")
            continue

    if not all_source_views:
        logger.warning(
            "No source views found in pipeline configs, using default CogniteAsset view"
        )
        all_source_views = [
            {
                "view_external_id": "CogniteAsset",
                "view_space": "cdf_cdm",
                "view_version": "v1",
                "entity_type": "asset",
            }
        ]

    validation_config = {"min_confidence": 0.5, "max_keys_per_type": 1000}
    for config_file in sorted(pipelines_dir.glob("*.config.yaml")):
        try:
            with open(config_file, "r") as f:
                pipeline_config = yaml.safe_load(f)
            config_data = pipeline_config.get("config", {}).get("data", {})
            pipeline_validation = config_data.get("validation", {})
            if pipeline_validation:
                validation_config.update(pipeline_validation)
                logger.info(f"Loaded validation config from {config_file.name}")
                break
        except Exception:
            continue

    extraction_config = {
        "extraction_rules": all_extraction_rules,
        "validation": validation_config,
    }

    logger.info(f"Total extraction rules loaded: {len(all_extraction_rules)}")
    logger.info(f"Total source views: {len(all_source_views)}")

    env_wfk = (os.getenv("WRITE_FOREIGN_KEY_REFERENCES") or "").strip().lower()
    if env_wfk in ("1", "true", "yes", "on"):
        write_foreign_key_references = True
    env_fkp = (os.getenv("FOREIGN_KEY_WRITEBACK_PROPERTY") or "").strip()
    if env_fkp:
        foreign_key_writeback_property = env_fkp

    return (
        extraction_config,
        aliasing_config,
        all_source_views,
        alias_writeback_property,
        write_foreign_key_references,
        foreign_key_writeback_property,
    )
