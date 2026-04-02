"""Load v1 scope YAML for the local CLI (module-root default or ``--config-path``)."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.cdf_adapter import (
    _DEFAULT_ALIASING_VALIDATION,
    _convert_yaml_direct_to_aliasing_config,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.scope_document_dm import (
    resolve_scope_document_source_views,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import (
    _convert_rule_dict_to_engine_format,
)

from .paths import SCRIPT_DIR

DEFAULT_SCOPE = "default"

# v1 scope document at module root (local runs only; CDF uses trigger-embedded configuration).
WORKFLOW_LOCAL_CONFIG_FILENAME = "workflow.local.config.yaml"
DEFAULT_SCOPE_DOCUMENT_PATH = SCRIPT_DIR / WORKFLOW_LOCAL_CONFIG_FILENAME

def resolve_scope_document_path(scope: Optional[str] = None) -> Path:
    """Resolve the default v1 scope YAML at the module root.

    Only ``scope='default'`` (or omitted) is supported without ``--config-path``.
    Other scope names raise: use ``--config-path`` to point at a v1 scope file.
    """
    sc = (scope or DEFAULT_SCOPE).strip() or DEFAULT_SCOPE
    if sc != DEFAULT_SCOPE:
        raise FileNotFoundError(
            f"Per-scope directory layout was removed. For scope {sc!r} pass --config-path "
            f"to a v1 scope YAML file, or use scope {DEFAULT_SCOPE!r} to load "
            f"{DEFAULT_SCOPE_DOCUMENT_PATH.name} at the module root."
        )
    if not DEFAULT_SCOPE_DOCUMENT_PATH.is_file():
        raise FileNotFoundError(
            f"Missing default scope document {DEFAULT_SCOPE_DOCUMENT_PATH}. "
            f"Add {WORKFLOW_LOCAL_CONFIG_FILENAME} at the module root or pass --config-path."
        )
    return DEFAULT_SCOPE_DOCUMENT_PATH


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


def _load_from_scope_document(
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

    source_views = resolve_scope_document_source_views(doc)

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
        "source_views": source_views,
    }
    ke_params = ke_config.get("parameters")
    if isinstance(ke_params, dict) and ke_params:
        extraction_config["parameters"] = dict(ke_params)

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
    - ``--config-path`` → load that file as a v1 scope document.
    - Else → ``workflow.local.config.yaml`` at module root when ``scope`` is ``default``.
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
        return _load_from_scope_document(logger, doc)

    sc = (scope or DEFAULT_SCOPE).strip() or DEFAULT_SCOPE
    p = resolve_scope_document_path(sc)
    logger.info("Loading scope %r from %s", sc, p)
    with open(p, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        raise ValueError("Scope YAML root must be a mapping")
    return _load_from_scope_document(logger, doc)
