"""
Load file_asset_source step configuration from default.config.yaml or workflow input.

Workflow tasks pass ``step`` (extract | create | write) and
``configuration`` (the ``file_asset_source`` object from workflow.input).
Local runs read ``default.config.yaml`` on disk when no inline configuration is set.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Literal, Type, TypeVar

import yaml

PipelineStep = Literal["extract", "create", "write"]

DEFAULT_CONFIG_FILENAME = "default.config.yaml"

STEP_FUNCTION_EXTERNAL_ID: dict[PipelineStep, str] = {
    "extract": "fn_dm_extract_assets_by_pattern",
    "create": "fn_dm_create_asset_hierarchy",
    "write": "fn_dm_write_asset_hierarchy",
}


def module_root() -> Path:
    env = os.environ.get("CDF_FILE_ASSET_SOURCE_ROOT")
    if env:
        return Path(env).resolve()
    # shared/utils -> functions -> module root
    return Path(__file__).resolve().parent.parent.parent.parent


def load_default_config(root: Path | None = None) -> Dict[str, Any]:
    """Parse ``default.config.yaml`` at module root."""
    path = (root or module_root()) / DEFAULT_CONFIG_FILENAME
    if not path.is_file():
        raise FileNotFoundError(f"Missing module config: {path}")
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    if not isinstance(doc, dict):
        raise ValueError(f"{path}: root must be a mapping")
    return doc


def file_asset_source_section(doc: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Return the ``file_asset_source`` mapping from a default config document."""
    if doc is None:
        doc = load_default_config()
    fas = doc.get("file_asset_source")
    if not isinstance(fas, dict):
        raise ValueError("default.config.yaml: missing or invalid 'file_asset_source' section")
    return fas


def scope_hierarchy_section(doc: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Return top-level ``scope_hierarchy`` from a default config document."""
    if doc is None:
        doc = load_default_config()
    sh = doc.get("scope_hierarchy")
    if not isinstance(sh, dict):
        raise ValueError("default.config.yaml: missing or invalid 'scope_hierarchy' section")
    return sh


def workflow_configuration(doc: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Full workflow ``input.configuration``: pipeline steps plus ``scope_hierarchy``.

    Cognite workflows pass this object as ``configuration`` on function tasks.
    """
    if doc is None:
        doc = load_default_config()
    out = dict(file_asset_source_section(doc))
    out["scope_hierarchy"] = dict(scope_hierarchy_section(doc))
    return out


def _inject_scope_into_create_data(
    configuration: Dict[str, Any] | None, data: Dict[str, Any]
) -> Dict[str, Any]:
    """Map ``scope_hierarchy`` into create-step ``data`` for legacy handlers."""
    if not configuration:
        return data
    sh = configuration.get("scope_hierarchy")
    if not isinstance(sh, dict):
        return data
    merged = dict(data)
    levels = sh.get("levels")
    if isinstance(levels, list) and levels:
        merged["hierarchy_levels"] = levels
    locations = sh.get("locations")
    if isinstance(locations, list) and locations:
        merged["scope"] = locations
    return merged


def step_block(
    step: PipelineStep,
    *,
    configuration: Dict[str, Any] | None = None,
    root: Path | None = None,
) -> Dict[str, Any]:
    """
    Return ``{parameters, data}`` for a pipeline step.

    ``configuration`` is the workflow ``file_asset_source`` object (keys extract/create/write).
    When omitted, reads from ``default.config.yaml``.
    """
    if configuration is not None:
        block = configuration.get(step)
        if not isinstance(block, dict):
            raise ValueError(
                f"configuration missing step {step!r}; "
                f"expected keys: extract, create, write"
            )
        return block
    fas = file_asset_source_section(load_default_config(root))
    block = fas.get(step)
    if not isinstance(block, dict):
        raise ValueError(f"file_asset_source.{step} missing or not a mapping")
    return block


def config_dict_for_step(
    step: PipelineStep,
    function_data: Dict[str, Any],
    *,
    root: Path | None = None,
) -> Dict[str, Any]:
    """Build ``{externalId, config: {parameters, data}}`` for Pydantic ``Config`` models."""
    inline = function_data.get("configuration")
    if inline is not None and not isinstance(inline, dict):
        raise ValueError("'configuration' must be a mapping (file_asset_source steps)")

    block = step_block(step, configuration=inline, root=root)
    params = block.get("parameters") or {}
    data = block.get("data") or {}
    if not isinstance(params, dict) or not isinstance(data, dict):
        raise ValueError(f"Step {step!r}: parameters and data must be mappings")

    if step == "create":
        cfg_for_scope = inline
        if cfg_for_scope is None:
            doc = load_default_config(root)
            cfg_for_scope = workflow_configuration(doc)
        data = _inject_scope_into_create_data(cfg_for_scope, data)

    return {
        "externalId": STEP_FUNCTION_EXTERNAL_ID[step],
        "config": {"parameters": params, "data": data},
    }


TConfig = TypeVar("TConfig")


def resolve_cdf_config(
    function_data: Dict[str, Any],
    step: PipelineStep,
    config_model: Type[TConfig],
    *,
    client: Any = None,
    root: Path | None = None,
) -> TConfig:
    """
    Resolve configuration for a workflow/local function invocation.

    Priority:
    1. ``function_data['_cdf_config']`` if already a ``Config`` instance
    2. ``function_data['configuration']`` + ``function_data['step']`` (workflow input)
    3. ``default.config.yaml`` on disk when ``step`` is set (local CLI)
    """
    existing = function_data.get("_cdf_config")
    if existing is not None:
        if isinstance(existing, config_model):
            return existing
        if isinstance(existing, dict):
            return config_model.model_validate(existing)

    step_key = function_data.get("step") or step
    if step_key not in ("extract", "create", "write"):
        raise ValueError(
            f"Invalid step {step_key!r}; expected extract, create, or write"
        )

    if "configuration" in function_data or step_key:
        cfg_dict = config_dict_for_step(step_key, function_data, root=root)
        return config_model.model_validate(cfg_dict)

    raise ValueError(
        "Missing configuration: pass 'configuration' and 'step' on function data, "
        "or run locally with default.config.yaml present"
    )
