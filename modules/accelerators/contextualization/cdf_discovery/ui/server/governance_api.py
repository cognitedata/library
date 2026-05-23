"""FastAPI routes for declared governance (access control) in CDF Discovery."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import yaml
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

from ui.server import governance_declared
from ui.server.governance_declared import PRIMARY_CONFIG

router = APIRouter(prefix="/api/governance/declared", tags=["governance-declared"])


def _discovery_module_root() -> Path:
    from ui.server.main import MODULE_ROOT

    return MODULE_ROOT


def _declared() -> Path:
    return governance_declared.declared_root(_discovery_module_root())


def active_config_path(
    x_config_rel: Optional[str] = Header(None, alias="X-Config-Rel"),
) -> Path:
    module_root = _discovery_module_root()
    governance_declared._ensure_governance_build_on_path(module_root)
    try:
        return governance_declared.active_config_path(_declared(), x_config_rel)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


class ConfigBody(BaseModel):
    content: str = Field(..., description="Full default.config.yaml text")


class MirrorBody(BaseModel):
    model: Dict[str, Any] = Field(..., description="scope_hierarchy, dimensions, spaces, groups")


class BuildBody(BaseModel):
    target: Literal["spaces", "groups", "all"] = "all"
    force: bool = False
    dry_run: bool = False


class FileBody(BaseModel):
    content: str


@router.get("/health")
def declared_health() -> Dict[str, Any]:
    declared = _declared()
    cfg = declared / PRIMARY_CONFIG
    return {
        "ok": True,
        "declared_root": str(declared),
        "config_exists": cfg.is_file(),
    }


@router.get("/config")
def get_config_raw(config_path: Path = Depends(active_config_path)) -> Dict[str, str]:
    rel = str(config_path.relative_to(_declared())).replace("\\", "/")
    if not config_path.is_file():
        return {"path": rel, "content": ""}
    return {"path": rel, "content": config_path.read_text(encoding="utf-8")}


@router.put("/config")
def put_config_raw(
    body: ConfigBody, config_path: Path = Depends(active_config_path)
) -> Dict[str, Any]:
    try:
        yaml.safe_load(body.content)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}") from e
    config_path.write_text(body.content, encoding="utf-8")
    rel = str(config_path.relative_to(_declared())).replace("\\", "/")
    return {"ok": True, "path": rel}


@router.get("/config/model")
def get_config_model(config_path: Path = Depends(active_config_path)) -> Dict[str, Any]:
    if not config_path.is_file():
        return {}
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


@router.put("/config/model")
def put_config_model(
    model: Dict[str, Any], config_path: Path = Depends(active_config_path)
) -> Dict[str, Any]:
    try:
        text = yaml.safe_dump(model, default_flow_style=False, sort_keys=False, allow_unicode=True)
        yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML structure: {e}") from e
    config_path.write_text(text, encoding="utf-8")
    rel = str(config_path.relative_to(_declared())).replace("\\", "/")
    return {"ok": True, "path": rel}


@router.post("/config/mirror")
def mirror_config(body: MirrorBody) -> Dict[str, Any]:
    written, skipped = governance_declared.mirror_slice(_discovery_module_root(), _declared(), body.model)
    return {"ok": True, "written": written, "skipped": skipped}


@router.post("/build")
def run_build(body: BuildBody, config_path: Path = Depends(active_config_path)) -> Dict[str, Any]:
    return governance_declared.run_build(
        discovery_module_root=_discovery_module_root(),
        declared=_declared(),
        config_path=config_path,
        target=body.target,
        force=body.force,
        dry_run=body.dry_run,
    )


@router.get("/artifacts")
def list_artifacts(
    kind: Literal["spaces", "groups"],
) -> Dict[str, List[str]]:
    paths = governance_declared.list_artifact_paths(_declared(), kind)
    if kind == "spaces":
        return {"spaces": paths, "groups": []}
    return {"spaces": [], "groups": paths}


@router.get("/artifacts/tree")
def artifacts_tree(
    kind: Literal["spaces", "groups"],
    prefix: str = "",
) -> Dict[str, Any]:
    return {
        "children": governance_declared.list_artifact_tree_children(
            _declared(), kind=kind, prefix=prefix
        )
    }


@router.get("/file")
def read_file(rel: str) -> Dict[str, str]:
    try:
        return governance_declared.read_file(_declared(), rel)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="File not found") from e


@router.put("/file")
def write_file(
    rel: str,
    body: FileBody,
    config_path: Path = Depends(active_config_path),
) -> Dict[str, Any]:
    try:
        return governance_declared.write_file(
            _declared(),
            config_path,
            _discovery_module_root(),
            rel,
            body.content,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}") from e


@router.get("/source-id-hint")
def source_id_hint(source_id: str = "") -> Dict[str, Any]:
    return governance_declared.source_id_hint(source_id)
