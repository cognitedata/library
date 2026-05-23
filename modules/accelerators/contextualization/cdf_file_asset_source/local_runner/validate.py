"""Configuration validation (shared by CLI, API, and UI)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from local_runner.paths import DEFAULT_CONFIG_REL, PIPELINE_STEPS, get_module_root


def _validate_step(
    step: str, block: dict[str, Any], root: Path, *, full_doc: dict[str, Any] | None = None
) -> dict[str, Any]:
    rel = f"{DEFAULT_CONFIG_REL} → file_asset_source.{step}"
    parameters = block.get("parameters") or {}
    data = block.get("data") or {}
    if not isinstance(parameters, dict) or not isinstance(data, dict):
        return {
            "step": step,
            "path": rel,
            "valid": False,
            "errors": ["parameters and data must be mappings"],
            "warnings": [],
            "messages": [],
        }

    wrapper = {
        "externalId": f"file_asset_source_{step}",
        "config": {"parameters": parameters, "data": data},
    }

    try:
        from functions.shared.utils.config_validator import (
            format_validation_errors,
            validate_extract_config,
            validate_hierarchy_config,
        )
    except ImportError:
        return {
            "step": step,
            "path": rel,
            "valid": True,
            "errors": [],
            "warnings": ["Validation utilities not available; skipped."],
            "messages": [],
        }

    if step == "extract":
        errors = validate_extract_config(wrapper)
    elif step == "create":
        errors = validate_hierarchy_config(full_doc if isinstance(full_doc, dict) else wrapper)
    else:
        return {
            "step": step,
            "path": rel,
            "valid": True,
            "errors": [],
            "warnings": [],
            "messages": ["Write step: basic structure accepted."],
        }

    messages = format_validation_errors(errors).splitlines()
    has_errors = any(e.startswith("❌") for e in errors)
    warnings = [e for e in errors if e.startswith("⚠️")]
    hard_errors = [e for e in errors if e.startswith("❌")]

    return {
        "step": step,
        "path": rel,
        "valid": not has_errors,
        "errors": hard_errors,
        "warnings": warnings,
        "messages": messages,
    }


def validate_default_config(
    steps: list[str] | None = None,
) -> dict[str, Any]:
    """Validate ``file_asset_source`` steps in ``default.config.yaml``."""
    root = get_module_root()
    path = root / DEFAULT_CONFIG_REL
    if not path.is_file():
        return {
            "valid": False,
            "results": [
                {
                    "step": "all",
                    "path": DEFAULT_CONFIG_REL,
                    "valid": False,
                    "errors": [f"Config file not found: {DEFAULT_CONFIG_REL}"],
                    "warnings": [],
                    "messages": [],
                }
            ],
        }

    try:
        with open(path, encoding="utf-8") as f:
            doc = yaml.safe_load(f) or {}
    except Exception as e:
        return {
            "valid": False,
            "results": [
                {
                    "step": "all",
                    "path": DEFAULT_CONFIG_REL,
                    "valid": False,
                    "errors": [f"Error reading YAML: {e}"],
                    "warnings": [],
                    "messages": [],
                }
            ],
        }

    from functions.shared.utils.module_config import file_asset_source_section

    try:
        fas = file_asset_source_section(doc if isinstance(doc, dict) else {})
    except ValueError as e:
        return {
            "valid": False,
            "results": [
                {
                    "step": "all",
                    "path": DEFAULT_CONFIG_REL,
                    "valid": False,
                    "errors": [str(e)],
                    "warnings": [],
                    "messages": [],
                }
            ],
        }

    step_list = steps if steps else list(PIPELINE_STEPS)
    results = []
    for step in step_list:
        if step not in PIPELINE_STEPS:
            results.append(
                {
                    "step": step,
                    "path": DEFAULT_CONFIG_REL,
                    "valid": False,
                    "errors": [f"Unknown step {step!r}"],
                    "warnings": [],
                    "messages": [],
                }
            )
            continue
        block = fas.get(step)
        if not isinstance(block, dict):
            results.append(
                {
                    "step": step,
                    "path": f"{DEFAULT_CONFIG_REL} → file_asset_source.{step}",
                    "valid": False,
                    "errors": [f"Missing file_asset_source.{step}"],
                    "warnings": [],
                    "messages": [],
                }
            )
            continue
        results.append(_validate_step(step, block, root, full_doc=doc if isinstance(doc, dict) else None))

    all_valid = all(r["valid"] for r in results)
    return {"valid": all_valid, "results": results, "config_path": DEFAULT_CONFIG_REL}


def validate_pipeline_configs(
    rel_paths: list[str] | None = None,
) -> dict[str, Any]:
    """Backward-compatible entry: validates ``default.config.yaml`` (ignores legacy paths)."""
    _ = rel_paths
    return validate_default_config()
