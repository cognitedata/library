"""Load combined scope YAML for the local CLI (config/scopes/<scope>/key_extraction_aliasing.yaml).

Falls back to merging split configs under config/examples/ when forced by env or when
no default combined file exists (see LEGACY_MERGE_ENV).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.cdf_adapter import (
    _convert_yaml_direct_to_aliasing_config,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import (
    _convert_rule_dict_to_engine_format,
)

from .paths import SCRIPT_DIR

DEFAULT_SCOPE = "default"
LEGACY_MERGE_ENV = "CDF_KEY_EXTRACTION_LOCAL_CONFIG_MODE"

# Combined v1 document (key_extraction + optional aliasing) per scope directory.
COMBINED_SCOPE_FILENAME = "key_extraction_aliasing.yaml"

_DEFAULT_ALIASING_VALIDATION: Dict[str, Any] = {
    "max_aliases_per_tag": 50,
    "min_alias_length": 2,
    "max_alias_length": 50,
    "allowed_characters": r"A-Za-z0-9-_/. ",
}


def _examples_dir() -> Path:
    return SCRIPT_DIR / "config" / "examples"


def _scope_dir(scope: str) -> Path:
    return SCRIPT_DIR / "config" / "scopes" / scope


def _combined_scope_file(scope: str) -> Path:
    return _scope_dir(scope) / COMBINED_SCOPE_FILENAME


def _merge_env_active() -> bool:
    v = (os.getenv(LEGACY_MERGE_ENV) or "").strip().lower()
    return v in ("merge", "1", "true", "yes", "on")


def _default_passthrough_rules_for_views(
    source_views: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    entity_types: List[str] = []
    for v in source_views:
        et = v.get("entity_type")
        if isinstance(et, str) and et.strip() and et not in entity_types:
            entity_types.append(et)
    if not entity_types:
        entity_types = ["asset"]
    rules: List[Dict[str, Any]] = []
    for et in entity_types:
        rules.append(
            {
                "name": f"default_passthrough_name_{et}",
                "method": "passthrough",
                "extraction_type": "candidate_key",
                "description": f"Default passthrough on name for {et}",
                "enabled": True,
                "priority": 50,
                "scope_filters": {"entity_type": [et]},
                "parameters": {"min_confidence": 1.0},
                "source_fields": [
                    {
                        "field_name": "name",
                        "required": True,
                        "max_length": 500,
                        "field_type": "string",
                        "priority": 1,
                        "role": "target",
                        "preprocessing": ["trim"],
                    }
                ],
                "field_selection_strategy": "first_match",
            }
        )
    return rules


def _load_from_combined_doc(
    logger: logging.Logger,
    doc: Dict[str, Any],
) -> Tuple[
    Dict[str, Any],
    Dict[str, Any],
    List[Dict[str, Any]],
    Optional[str],
    bool,
    Optional[str],
]:
    ke = doc.get("key_extraction")
    if not isinstance(ke, dict):
        raise ValueError("Scope YAML requires key_extraction mapping")
    ke_config = ke.get("config")
    if not isinstance(ke_config, dict):
        raise ValueError("key_extraction.config must be a mapping")
    config_data = ke_config.get("data")
    if not isinstance(config_data, dict):
        raise ValueError("key_extraction.config.data must be a mapping")

    source_views = list(config_data.get("source_views") or [])
    if not source_views:
        logger.warning(
            "No source_views in scope; using default CogniteAsset view (no instance_space)"
        )
        source_views = [
            {
                "view_external_id": "CogniteAsset",
                "view_space": "cdf_cdm",
                "view_version": "v1",
                "entity_type": "asset",
            }
        ]

    extraction_rules_raw = config_data.get("extraction_rules")
    if extraction_rules_raw is None:
        extraction_rules_raw = []
    if not isinstance(extraction_rules_raw, list):
        raise ValueError("extraction_rules must be a list")
    if len(extraction_rules_raw) == 0:
        logger.info(
            "Empty extraction_rules; injecting default passthrough on name per entity_type"
        )
        extraction_rules_raw = _default_passthrough_rules_for_views(source_views)

    all_extraction_rules: List[Any] = []
    for rule in extraction_rules_raw:
        converted = _convert_rule_dict_to_engine_format(rule)
        if converted:
            all_extraction_rules.append(converted)

    validation_config = {"min_confidence": 0.5, "max_keys_per_type": 1000}
    pv = config_data.get("validation")
    if isinstance(pv, dict) and pv:
        validation_config.update(pv)

    extraction_config = {
        "extraction_rules": all_extraction_rules,
        "validation": validation_config,
    }

    alias_writeback_property: Optional[str] = None
    write_foreign_key_references = False
    foreign_key_writeback_property: Optional[str] = None

    def _apply_aliasing_params(params: Any) -> None:
        nonlocal alias_writeback_property, write_foreign_key_references, foreign_key_writeback_property
        if not isinstance(params, dict):
            return
        if params.get("write_foreign_key_references") is True:
            write_foreign_key_references = True
        fkp = params.get("foreign_key_writeback_property")
        if isinstance(fkp, str) and fkp.strip():
            foreign_key_writeback_property = fkp.strip()
        awp = params.get("alias_writeback_property")
        if isinstance(awp, str) and awp.strip():
            alias_writeback_property = awp.strip()

    al = doc.get("aliasing")
    if al is None:
        logger.info("No aliasing branch; using identity passthrough (zero rules)")
        aliasing_config: Dict[str, Any] = {
            "rules": [],
            "validation": dict(_DEFAULT_ALIASING_VALIDATION),
        }
    else:
        if not isinstance(al, dict):
            raise ValueError("aliasing must be a mapping if present")
        al_cfg = al.get("config")
        if not isinstance(al_cfg, dict):
            raise ValueError("aliasing.config must be a mapping")
        _apply_aliasing_params(al_cfg.get("parameters"))
        al_data = al_cfg.get("data")
        if not isinstance(al_data, dict):
            raise ValueError("aliasing.config.data must be a mapping")
        rules_raw = al_data.get("aliasing_rules")
        if rules_raw is None:
            rules_raw = []
        if not isinstance(rules_raw, list):
            raise ValueError("aliasing_rules must be a list")
        if len(rules_raw) == 0:
            logger.info("Empty aliasing_rules; using identity passthrough")
            val = dict(_DEFAULT_ALIASING_VALIDATION)
            av = al_data.get("validation")
            if isinstance(av, dict) and av:
                val.update(av)
            aliasing_config = {"rules": [], "validation": val}
        else:
            aliasing_config = _convert_yaml_direct_to_aliasing_config({"config": al_cfg})

    env_wfk = (os.getenv("WRITE_FOREIGN_KEY_REFERENCES") or "").strip().lower()
    if env_wfk in ("1", "true", "yes", "on"):
        write_foreign_key_references = True
    env_fkp = (os.getenv("FOREIGN_KEY_WRITEBACK_PROPERTY") or "").strip()
    if env_fkp:
        foreign_key_writeback_property = env_fkp

    logger.info("Total extraction rules loaded: %s", len(all_extraction_rules))
    logger.info("Total source views: %s", len(source_views))
    logger.info(
        "Aliasing rules loaded: %s",
        len(aliasing_config.get("rules") or []),
    )

    return (
        extraction_config,
        aliasing_config,
        source_views,
        alias_writeback_property,
        write_foreign_key_references,
        foreign_key_writeback_property,
    )


def _load_legacy_merge_examples(
    logger: logging.Logger,
    *,
    forced_by_env: bool = False,
) -> Tuple[
    Dict[str, Any],
    Dict[str, Any],
    List[Dict[str, Any]],
    Optional[str],
    bool,
    Optional[str],
]:
    """Merge *aliasing*.config.yaml and *key_extraction*.config.yaml under config/examples/."""
    if forced_by_env:
        logger.warning(
            "Legacy merge mode (%s): merging split configs under config/examples/.",
            LEGACY_MERGE_ENV,
        )
    else:
        logger.warning(
            "No config/scopes/default/key_extraction_aliasing.yaml; "
            "falling back to legacy merge under config/examples/. "
            "Add a combined scope file or pass --config-path."
        )
    examples = _examples_dir()
    if not examples.is_dir():
        raise FileNotFoundError(
            f"Legacy merge requested but examples directory missing: {examples}"
        )

    all_aliasing_rules: List[Any] = []
    alias_writeback_property: Optional[str] = None
    write_foreign_key_references = False
    foreign_key_writeback_property: Optional[str] = None

    for config_file in sorted(examples.glob("*aliasing*.config.yaml")):
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                pipeline_config = yaml.safe_load(f)
            config_data = pipeline_config.get("config", {}).get("data", {})
            parameters = pipeline_config.get("config", {}).get("parameters", {})
            if isinstance(parameters, dict):
                if parameters.get("write_foreign_key_references") is True:
                    write_foreign_key_references = True
                if (
                    foreign_key_writeback_property is None
                    and parameters.get("foreign_key_writeback_property") is not None
                ):
                    raw_fkp = parameters.get("foreign_key_writeback_property")
                    if isinstance(raw_fkp, str) and raw_fkp.strip():
                        foreign_key_writeback_property = raw_fkp.strip()
                if (
                    alias_writeback_property is None
                    and parameters.get("alias_writeback_property") is not None
                ):
                    raw_prop = parameters.get("alias_writeback_property")
                    if isinstance(raw_prop, str) and raw_prop.strip():
                        alias_writeback_property = raw_prop.strip()
            aliasing_config = _convert_yaml_direct_to_aliasing_config(
                {"config": {"data": config_data}}
            )
            converted_rules = aliasing_config.get("rules", [])
            all_aliasing_rules.extend(converted_rules)
            logger.info(
                "Loaded %s aliasing rules from examples/%s",
                len(converted_rules),
                config_file.name,
            )
        except Exception as e:
            logger.warning("Failed to load %s: %s", config_file.name, e)

    if not all_aliasing_rules:
        logger.info("No aliasing files in examples merge; using identity passthrough")
        aliasing_config = {
            "rules": [],
            "validation": dict(_DEFAULT_ALIASING_VALIDATION),
        }
    else:
        aliasing_config = {
            "rules": all_aliasing_rules,
            "validation": dict(_DEFAULT_ALIASING_VALIDATION),
        }

    all_extraction_rules: List[Any] = []
    all_source_views: List[Dict[str, Any]] = []
    seen_source_views = set()

    for config_file in sorted(examples.glob("*key_extraction*.config.yaml")):
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                pipeline_config = yaml.safe_load(f)
            config_data = pipeline_config.get("config", {}).get("data", {})
            rules = config_data.get("extraction_rules", [])
            for rule in rules:
                converted_rule = _convert_rule_dict_to_engine_format(rule)
                if converted_rule:
                    all_extraction_rules.append(converted_rule)
            for view in config_data.get("source_views", []) or []:
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
        except Exception as e:
            logger.warning("Failed to load %s: %s", config_file.name, e)

    if not all_source_views:
        logger.warning("No source views from examples merge; using default CogniteAsset")
        all_source_views = [
            {
                "view_external_id": "CogniteAsset",
                "view_space": "cdf_cdm",
                "view_version": "v1",
                "entity_type": "asset",
            }
        ]

    if not all_extraction_rules:
        logger.info(
            "Merged extraction_rules empty; injecting passthrough-on-name per entity_type"
        )
        raw_rules = _default_passthrough_rules_for_views(all_source_views)
        for rule in raw_rules:
            converted = _convert_rule_dict_to_engine_format(rule)
            if converted:
                all_extraction_rules.append(converted)

    validation_config = {"min_confidence": 0.5, "max_keys_per_type": 1000}
    for config_file in sorted(examples.glob("*.config.yaml")):
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                pipeline_config = yaml.safe_load(f)
            config_data = pipeline_config.get("config", {}).get("data", {})
            pipeline_validation = config_data.get("validation", {})
            if pipeline_validation:
                validation_config.update(pipeline_validation)
                break
        except Exception:
            continue

    extraction_config = {
        "extraction_rules": all_extraction_rules,
        "validation": validation_config,
    }

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


def load_configs(
    logger: logging.Logger,
    scope: Optional[str] = None,
    config_path: Optional[str] = None,
) -> Tuple[
    Dict[str, Any],
    Dict[str, Any],
    List[Dict[str, Any]],
    Optional[str],
    bool,
    Optional[str],
]:
    """Load extraction and aliasing config for local runs.

    Resolution:
    - ``--config-path`` → load that file as combined v1 YAML.
    - Else if ``CDF_KEY_EXTRACTION_LOCAL_CONFIG_MODE`` is merge-like → legacy merge
      from ``config/examples/*``.
    - Else if ``--scope`` → ``config/scopes/<scope>/key_extraction_aliasing.yaml``.
    - Else if default combined file exists → load it.
    - Else → legacy merge from ``config/examples/*`` (deprecation path).
    """
    if config_path:
        p = Path(config_path).expanduser()
        if not p.is_absolute():
            p = Path.cwd() / p
        if not p.is_file():
            raise FileNotFoundError(f"Config file not found: {p}")
        logger.info("Loading scope from --config-path: %s", p)
        with open(p, "r", encoding="utf-8") as f:
            doc = yaml.safe_load(f)
        if not isinstance(doc, dict):
            raise ValueError("Scope YAML root must be a mapping")
        return _load_from_combined_doc(logger, doc)

    if _merge_env_active():
        return _load_legacy_merge_examples(logger, forced_by_env=True)

    sc = (scope or DEFAULT_SCOPE).strip() or DEFAULT_SCOPE
    p = _combined_scope_file(sc)
    if p.is_file():
        logger.info("Loading scope %r from %s", sc, p)
        with open(p, "r", encoding="utf-8") as f:
            doc = yaml.safe_load(f)
        if not isinstance(doc, dict):
            raise ValueError("Scope YAML root must be a mapping")
        return _load_from_combined_doc(logger, doc)

    if scope is not None and str(scope).strip():
        d = _scope_dir(sc)
        raise FileNotFoundError(
            f"No combined scope file in {d}: expected {COMBINED_SCOPE_FILENAME!r}. "
            "Create it or pass --config-path."
        )

    return _load_legacy_merge_examples(logger, forced_by_env=False)
