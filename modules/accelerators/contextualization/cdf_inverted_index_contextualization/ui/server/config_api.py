"""Read and write default.config.yaml with validation."""

from __future__ import annotations

from pathlib import Path

import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ui.server.paths import CONFIG_PATH

router = APIRouter(prefix="/api/inverted-index", tags=["config"])


class ConfigBody(BaseModel):
    yaml_text: str = Field(min_length=1, max_length=500_000)


def _config_path() -> Path:
    return CONFIG_PATH


@router.get("/config")
def get_config() -> dict:
    path = _config_path()
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"Missing config file: {path}")
    text = path.read_text(encoding="utf-8")
    from inverted_index.config_loader import build_runtime_config, load_yaml_config

    yaml_cfg = load_yaml_config(path)
    runtime = build_runtime_config(yaml_cfg)
    return {
        "path": str(path),
        "yaml_text": text,
        "runtime": {
            "storage_backend": runtime["storage_config"].get("backend"),
            "raw_database": runtime["storage_config"].get("raw", {}).get("database"),
            "scope_enabled": bool(runtime["scope_config"].get("enabled")),
            "scope_fallback": runtime["scope_config"].get("fallback_scope_key"),
            "subscription_enabled": bool(runtime["subscription_config"].get("enabled")),
            "watch_property": runtime["subscription_config"].get("watch_property"),
            "index_field_count": len(runtime.get("index_field_config") or []),
            "instance_spaces": runtime.get("instance_spaces"),
        },
    }


@router.put("/config")
def put_config(body: ConfigBody) -> dict:
    path = _config_path()
    try:
        parsed = yaml.safe_load(body.yaml_text) or {}
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}") from e
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="Config root must be a mapping")

    from inverted_index.config_loader import build_runtime_config
    from inverted_index.storage.raw_adapter import validate_raw_scope_config

    try:
        runtime = build_runtime_config(parsed)
        validate_raw_scope_config(runtime["scope_config"])
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Config validation failed: {e}") from e

    path.write_text(body.yaml_text, encoding="utf-8")
    return get_config()
