"""Local operator API — trusted workstation only; no authentication."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import yaml
from fastapi import Body, Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

_MODULE_DEFAULT = str(Path(__file__).resolve().parent.parent.parent)
MODULE_ROOT = Path(
    os.environ.get("CDF_ACCESS_CONTROL_ROOT")
    or os.environ.get("CDF_ACCESS_GOVERNANCE_ROOT")
    or _MODULE_DEFAULT
).resolve()
_SCRIPTS = MODULE_ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from governance_build.config_sources import (  # noqa: E402
    PRIMARY_CONFIG,
    all_registered_config_relpaths,
    mirror_access_control_slice,
    resolve_registered_config_path,
)

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

app = FastAPI(title="Access Control operator API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5183",
        "http://localhost:5183",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def active_config_path(x_config_rel: Optional[str] = Header(None, alias="X-Config-Rel")) -> Path:
    rel = (x_config_rel or "").strip() or PRIMARY_CONFIG
    try:
        return resolve_registered_config_path(MODULE_ROOT, rel)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


def _rel_from_config_path(config_path: Path) -> str:
    return str(config_path.relative_to(MODULE_ROOT)).replace("\\", "/")


class ConfigBody(BaseModel):
    content: str = Field(..., description="Full default.config.yaml text")


class BuildBody(BaseModel):
    force: bool = False
    dry_run: bool = False


class FileBody(BaseModel):
    content: str


class MirrorBody(BaseModel):
    model: Dict[str, Any] = Field(..., description="Document containing dimensions/spaces/groups to mirror")


@app.get("/api/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "module_root": str(MODULE_ROOT)}


@app.get("/api/config/sources")
def list_config_sources() -> Dict[str, Any]:
    rels = all_registered_config_relpaths(MODULE_ROOT)
    sources = []
    for rel in rels:
        p = MODULE_ROOT / rel
        sources.append({"rel": rel, "exists": p.is_file(), "primary": rel == PRIMARY_CONFIG})
    return {"primary": PRIMARY_CONFIG, "sources": sources}


@app.get("/api/config")
def get_config(config_path: Path = Depends(active_config_path)) -> Dict[str, str]:
    rel = _rel_from_config_path(config_path)
    if not config_path.is_file():
        return {"path": rel, "content": ""}
    return {"path": rel, "content": config_path.read_text(encoding="utf-8")}


@app.put("/api/config")
def put_config(body: ConfigBody, config_path: Path = Depends(active_config_path)) -> Dict[str, Any]:
    try:
        yaml.safe_load(body.content)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}") from e
    config_path.write_text(body.content, encoding="utf-8")
    return {"ok": True, "path": _rel_from_config_path(config_path)}


@app.get("/api/config/model")
def get_config_model(config_path: Path = Depends(active_config_path)) -> Dict[str, Any]:
    if not config_path.is_file():
        return {}
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


@app.put("/api/config/model")
def put_config_model(
    model: Dict[str, Any], config_path: Path = Depends(active_config_path)
) -> Dict[str, Any]:
    try:
        text = yaml.safe_dump(model, default_flow_style=False, sort_keys=False, allow_unicode=True)
        yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML structure: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    config_path.write_text(text, encoding="utf-8")
    return {"ok": True, "path": _rel_from_config_path(config_path)}


@app.post("/api/config/mirror")
def mirror_access_control(body: MirrorBody) -> Dict[str, Any]:
    written, skipped = mirror_access_control_slice(MODULE_ROOT, body.model, dry_run=False)
    return {"ok": True, "written": written, "skipped": skipped}


@app.post("/api/build")
def run_build(
    body: BuildBody, config_path: Path = Depends(active_config_path)
) -> Dict[str, Any]:
    """Run ``module.py build``; return stdout and stderr (logging uses stderr)."""
    cmd = [sys.executable, str(MODULE_ROOT / "module.py"), "build", "--config", str(config_path)]
    if body.force:
        cmd.append("--force")
    if body.dry_run:
        cmd.append("--dry-run")
    env = {**os.environ}
    scripts = str(_SCRIPTS)
    if scripts not in env.get("PYTHONPATH", "").split(os.pathsep):
        env["PYTHONPATH"] = f"{scripts}{os.pathsep}{env.get('PYTHONPATH', '')}".strip(os.pathsep)
    proc = subprocess.run(
        cmd,
        cwd=str(MODULE_ROOT),
        capture_output=True,
        text=True,
        env=env,
    )
    return {
        "ok": proc.returncode == 0,
        "exit_code": proc.returncode,
        "stdout": proc.stdout or "",
        "stderr": proc.stderr or "",
    }


@app.get("/api/artifacts")
def list_artifacts(kind: Literal["spaces", "groups", "all"] = "all") -> Dict[str, List[str]]:
    spaces: List[str] = []
    groups: List[str] = []
    sp = MODULE_ROOT / "spaces"
    au = MODULE_ROOT / "auth"
    if kind in ("spaces", "all") and sp.is_dir():
        spaces = sorted(str(p.relative_to(MODULE_ROOT)).replace("\\", "/") for p in sp.rglob("*.Space.yaml"))
    if kind in ("groups", "all") and au.is_dir():
        groups = sorted(str(p.relative_to(MODULE_ROOT)).replace("\\", "/") for p in au.rglob("*.Group.yaml"))
    return {"spaces": spaces, "groups": groups}


@app.get("/api/file")
def read_file(rel: str) -> Dict[str, str]:
    path = _safe_rel_path(rel)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return {
        "path": str(path.relative_to(MODULE_ROOT)),
        "content": path.read_text(encoding="utf-8"),
    }


def _safe_rel_path(rel: str) -> Path:
    if ".." in rel.split("/") or rel.startswith(("/", "\\")):
        raise HTTPException(status_code=400, detail="Invalid path")
    p = (MODULE_ROOT / rel).resolve()
    try:
        p.relative_to(MODULE_ROOT)
    except ValueError as e:
        raise HTTPException(status_code=400, detail="Path escapes module root") from e
    return p


@app.put("/api/file")
def write_file(
    rel: str,
    body: FileBody,
    config_path: Path = Depends(active_config_path),
) -> Dict[str, Any]:
    path = _safe_rel_path(rel)
    if path.suffix.lower() not in (".yml", ".yaml"):
        raise HTTPException(status_code=400, detail="Only .yaml/.yml allowed")
    try:
        yaml.safe_load(body.content)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}") from e
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body.content, encoding="utf-8")
    rel_str = str(path.relative_to(MODULE_ROOT))
    out: Dict[str, Any] = {"ok": True, "path": rel_str}
    if path.name.endswith(".Group.yaml"):
        from governance_build.toolkit_sync import upsert_group_source_id_from_group_yaml

        out["source_ids_synced"] = upsert_group_source_id_from_group_yaml(
            default_config_path=config_path,
            group_yaml_text=body.content,
            dry_run=False,
        )
    return out


@app.get("/api/source-id-hint")
def source_id_hint(source_id: str) -> Dict[str, Any]:
    s = source_id.strip()
    if not s:
        return {"valid": True, "empty": True}
    return {"valid": bool(_UUID_RE.match(s)), "empty": False}


def main() -> None:
    import uvicorn

    uvicorn.run(
        "ui.server.main:app",
        host="127.0.0.1",
        port=int(os.environ.get("PORT", "8775")),
        reload=False,
    )


if __name__ == "__main__":
    main()
