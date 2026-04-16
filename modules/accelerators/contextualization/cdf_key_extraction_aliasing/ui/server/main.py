"""Local operator API — trusted workstation only; no authentication."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml
from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

_MODULE_DEFAULT = str(Path(__file__).resolve().parent.parent.parent)
MODULE_ROOT = Path(
    os.environ.get("CDF_KEY_EXTRACTION_ALIASING_ROOT") or _MODULE_DEFAULT
).resolve()

DEFAULT_CONFIG_REL = "default.config.yaml"
DEFAULT_SCOPE_DOCUMENT_REL = "workflow.local.config.yaml"

app = FastAPI(title="Key discovery & aliasing operator API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5173",
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _safe_rel_path(rel: str) -> Path:
    if ".." in rel.split("/") or rel.startswith(("/", "\\")):
        raise HTTPException(status_code=400, detail="Invalid path")
    p = (MODULE_ROOT / rel).resolve()
    try:
        p.relative_to(MODULE_ROOT)
    except ValueError as e:
        raise HTTPException(status_code=400, detail="Path escapes module root") from e
    return p


def _scope_document_path(rel: str | None) -> Path:
    r = (rel or "").strip() or DEFAULT_SCOPE_DOCUMENT_REL
    return _safe_rel_path(r)


class YamlBody(BaseModel):
    content: str = Field(..., description="Full YAML text")


class BuildBody(BaseModel):
    force: bool = False
    dry_run: bool = False


class FileBody(BaseModel):
    content: str


@app.get("/api/health")
def health() -> dict:
    return {"ok": True, "module_root": str(MODULE_ROOT)}


@app.get("/api/default-config")
def get_default_config() -> dict:
    p = MODULE_ROOT / DEFAULT_CONFIG_REL
    if not p.is_file():
        return {"path": DEFAULT_CONFIG_REL, "content": ""}
    return {
        "path": DEFAULT_CONFIG_REL,
        "content": p.read_text(encoding="utf-8"),
    }


@app.put("/api/default-config")
def put_default_config(body: YamlBody) -> dict:
    p = MODULE_ROOT / DEFAULT_CONFIG_REL
    try:
        yaml.safe_load(body.content)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}") from e
    p.write_text(body.content, encoding="utf-8")
    return {"ok": True, "path": DEFAULT_CONFIG_REL}


@app.get("/api/default-config/model")
def get_default_config_model() -> Dict[str, Any]:
    p = MODULE_ROOT / DEFAULT_CONFIG_REL
    if not p.is_file():
        return {}
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="Config root must be a mapping")
    return data


@app.put("/api/default-config/model")
def put_default_config_model(model: Dict[str, Any] = Body(...)) -> dict:
    p = MODULE_ROOT / DEFAULT_CONFIG_REL
    try:
        text = yaml.safe_dump(
            model,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
        yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML structure: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    p.write_text(text, encoding="utf-8")
    return {"ok": True, "path": DEFAULT_CONFIG_REL}


@app.get("/api/scope-document")
def get_scope_document(
    rel: str | None = Query(None, description="Path under module root"),
) -> dict:
    path = _scope_document_path(rel)
    r = str(path.relative_to(MODULE_ROOT)).replace("\\", "/")
    if not path.is_file():
        return {"path": r, "content": ""}
    return {"path": r, "content": path.read_text(encoding="utf-8")}


@app.put("/api/scope-document")
def put_scope_document(
    body: YamlBody,
    rel: str | None = Query(None, description="Path under module root"),
) -> dict:
    path = _scope_document_path(rel)
    try:
        yaml.safe_load(body.content)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}") from e
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body.content, encoding="utf-8")
    r = str(path.relative_to(MODULE_ROOT)).replace("\\", "/")
    return {"ok": True, "path": r}


@app.get("/api/scope-document/model")
def get_scope_document_model(
    rel: str | None = Query(None, description="Path under module root"),
) -> Dict[str, Any]:
    path = _scope_document_path(rel)
    if not path.is_file():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="Scope document root must be a mapping")
    return data


@app.put("/api/scope-document/model")
def put_scope_document_model(
    model: Dict[str, Any] = Body(...),
    rel: str | None = Query(None, description="Path under module root"),
) -> dict:
    path = _scope_document_path(rel)
    try:
        text = yaml.safe_dump(
            model,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
        yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML structure: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    r = str(path.relative_to(MODULE_ROOT)).replace("\\", "/")
    return {"ok": True, "path": r}


@app.post("/api/build")
def run_build(body: BuildBody) -> dict:
    cmd = [
        sys.executable,
        str(MODULE_ROOT / "module.py"),
        "build",
    ]
    if body.force:
        cmd.append("--force")
    if body.dry_run:
        cmd.append("--dry-run")
    env = {**os.environ, "PYTHONPATH": str(MODULE_ROOT / "scripts")}
    proc = subprocess.run(
        cmd,
        cwd=str(MODULE_ROOT),
        capture_output=True,
        text=True,
        env=env,
    )
    return {
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


@app.get("/api/artifacts")
def list_artifacts() -> dict:
    wf = MODULE_ROOT / "workflows"
    paths: List[str] = []
    if wf.is_dir():
        seen: set[str] = set()
        for p in wf.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() not in {".yaml", ".yml"}:
                continue
            rel = str(p.relative_to(MODULE_ROOT)).replace("\\", "/")
            seen.add(rel)
        paths = sorted(seen)
    return {"paths": paths}


@app.get("/api/file")
def read_file(rel: str = Query(..., description="Relative path under module root")) -> dict:
    path = _safe_rel_path(rel)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return {
        "path": str(path.relative_to(MODULE_ROOT)).replace("\\", "/"),
        "content": path.read_text(encoding="utf-8"),
    }


@app.put("/api/file")
def write_file(
    rel: str = Query(..., description="Relative path under module root"),
    body: FileBody = Body(...),
) -> dict:
    path = _safe_rel_path(rel)
    if path.suffix.lower() not in {".yaml", ".yml"}:
        raise HTTPException(status_code=400, detail="Only .yaml/.yml allowed")
    try:
        yaml.safe_load(body.content)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}") from e
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body.content, encoding="utf-8")
    return {
        "ok": True,
        "path": str(path.relative_to(MODULE_ROOT)).replace("\\", "/"),
    }


def main() -> None:
    import uvicorn

    uvicorn.run(
        "ui.server.main:app",
        host="127.0.0.1",
        port=int(os.environ.get("PORT", "8765")),
        reload=False,
    )


if __name__ == "__main__":
    main()
