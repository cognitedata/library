"""Local operator API — trusted workstation only; no authentication."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Literal

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
TEMPLATE_WORKFLOW_CONFIG_REL = "workflow_template/workflow.template.config.yaml"
# Snapshot of input.configuration from the last operator run against a WorkflowTrigger (optional .gitignore).
OPERATOR_RUN_SCOPE_SNAPSHOT = ".operator_run_scope.yaml"

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


def _canvas_document_path(scope_rel: str | None) -> Path:
    """Sibling layout file: workflow.local.config.yaml -> workflow.local.canvas.yaml."""
    scope = _scope_document_path(scope_rel)
    name = scope.name
    parent = scope.parent
    lower = name.lower()
    if lower.endswith(".config.yaml"):
        stem = name[: -len(".config.yaml")]
        return parent / f"{stem}.canvas.yaml"
    if lower.endswith(".config.yml"):
        stem = name[: -len(".config.yml")]
        return parent / f"{stem}.canvas.yml"
    if lower.endswith(".yaml"):
        stem = name[: -len(".yaml")]
        return parent / f"{stem}.canvas.yaml"
    if lower.endswith(".yml"):
        stem = name[: -len(".yml")]
        return parent / f"{stem}.canvas.yml"
    return parent / f"{name}.canvas.yaml"


class YamlBody(BaseModel):
    content: str = Field(..., description="Full YAML text")


class BuildBody(BaseModel):
    force: bool = False
    dry_run: bool = False


RunTarget = Literal["workflow_local", "workflow_template", "workflow_trigger"]


class RunBody(BaseModel):
    run_all: bool = False
    target: RunTarget = "workflow_local"
    """Which scope document to pass to ``module.py run`` (see /api/run handler)."""
    workflow_trigger_rel: str | None = None
    """Module-relative path to a WorkflowTrigger YAML when ``target`` is ``workflow_trigger``."""


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


@app.get("/api/canvas-document")
def get_canvas_document(
    rel: str | None = Query(
        None,
        description="Scope document path under module root (canvas sibling is derived)",
    ),
) -> dict:
    """Layout-only canvas YAML paired with the scope document (e.g. workflow.local.canvas.yaml)."""
    path = _canvas_document_path(rel)
    r = str(path.relative_to(MODULE_ROOT)).replace("\\", "/")
    if not path.is_file():
        return {"path": r, "scope_rel": str(_scope_document_path(rel).relative_to(MODULE_ROOT)).replace("\\", "/"), "content": ""}
    return {
        "path": r,
        "scope_rel": str(_scope_document_path(rel).relative_to(MODULE_ROOT)).replace("\\", "/"),
        "content": path.read_text(encoding="utf-8"),
    }


@app.put("/api/canvas-document")
def put_canvas_document(
    body: YamlBody,
    rel: str | None = Query(
        None,
        description="Scope document path under module root (canvas sibling is derived)",
    ),
) -> dict:
    path = _canvas_document_path(rel)
    try:
        yaml.safe_load(body.content)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}") from e
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body.content, encoding="utf-8")
    r = str(path.relative_to(MODULE_ROOT)).replace("\\", "/")
    return {"ok": True, "path": r}


@app.get("/api/canvas-document/model")
def get_canvas_document_model(
    rel: str | None = Query(
        None,
        description="Scope document path under module root",
    ),
) -> Dict[str, Any]:
    path = _canvas_document_path(rel)
    if not path.is_file():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="Canvas document root must be a mapping")
    return data


@app.put("/api/canvas-document/model")
def put_canvas_document_model(
    model: Dict[str, Any] = Body(...),
    rel: str | None = Query(
        None,
        description="Scope document path under module root",
    ),
) -> dict:
    path = _canvas_document_path(rel)
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


@app.post("/api/template-workflow-config/from-scope")
def copy_template_workflow_config_from_scope(
    scope_rel: str | None = Query(
        None,
        description="Scope document path under module root (default workflow.local.config.yaml)",
    ),
) -> dict:
    """Overwrite workflow.template.config.yaml with the saved scope document (same v1 shape)."""
    scope_path = _scope_document_path(scope_rel)
    if not scope_path.is_file():
        raise HTTPException(status_code=404, detail="Scope document not found")
    content = scope_path.read_text(encoding="utf-8")
    try:
        yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise HTTPException(
            status_code=400, detail=f"Invalid YAML in scope document: {e}"
        ) from e
    template_path = _safe_rel_path(TEMPLATE_WORKFLOW_CONFIG_REL)
    template_path.parent.mkdir(parents=True, exist_ok=True)
    template_path.write_text(content, encoding="utf-8")
    from_rel = str(scope_path.relative_to(MODULE_ROOT)).replace("\\", "/")
    return {
        "ok": True,
        "from": from_rel,
        "to": TEMPLATE_WORKFLOW_CONFIG_REL,
    }


@app.post("/api/template-workflow-config/to-scope")
def copy_template_workflow_config_to_scope(
    scope_rel: str | None = Query(
        None,
        description="Scope document path under module root (default workflow.local.config.yaml)",
    ),
) -> dict:
    """Overwrite the scope document with workflow.template.config.yaml (same v1 shape)."""
    template_path = _safe_rel_path(TEMPLATE_WORKFLOW_CONFIG_REL)
    if not template_path.is_file():
        raise HTTPException(status_code=404, detail="Workflow template config not found")
    content = template_path.read_text(encoding="utf-8")
    try:
        yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise HTTPException(
            status_code=400, detail=f"Invalid YAML in workflow template config: {e}"
        ) from e
    scope_path = _scope_document_path(scope_rel)
    scope_path.parent.mkdir(parents=True, exist_ok=True)
    scope_path.write_text(content, encoding="utf-8")
    to_rel = str(scope_path.relative_to(MODULE_ROOT)).replace("\\", "/")
    return {
        "ok": True,
        "from": TEMPLATE_WORKFLOW_CONFIG_REL,
        "to": to_rel,
    }


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


@app.post("/api/run")
def run_pipeline(body: RunBody) -> dict:
    """Invoke ``python module.py run --config-path …`` (local CDF pipeline; requires credentials in env)."""
    if body.target == "workflow_local":
        config_rel = DEFAULT_SCOPE_DOCUMENT_REL
    elif body.target == "workflow_template":
        config_rel = TEMPLATE_WORKFLOW_CONFIG_REL
    else:
        wr = (body.workflow_trigger_rel or "").strip()
        if not wr:
            raise HTTPException(
                status_code=400,
                detail="workflow_trigger_rel is required when target is workflow_trigger",
            )
        config_rel = _write_scope_yaml_from_workflow_trigger(wr)

    cmd = [
        sys.executable,
        str(MODULE_ROOT / "module.py"),
        "run",
        "--config-path",
        config_rel,
    ]
    if body.run_all:
        cmd.append("--all")
    proc = subprocess.run(
        cmd,
        cwd=str(MODULE_ROOT),
        capture_output=True,
        text=True,
        env={**os.environ},
    )
    return {
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def _is_workflow_trigger_path(rel: str) -> bool:
    return rel.lower().endswith(".workflowtrigger.yaml") or rel.lower().endswith(
        ".workflowtrigger.yml"
    )


def _write_scope_yaml_from_workflow_trigger(rel: str) -> str:
    """Extract ``input.configuration`` to :data:`OPERATOR_RUN_SCOPE_SNAPSHOT`; return relative path."""
    if not _is_workflow_trigger_path(rel):
        raise HTTPException(
            status_code=400,
            detail="workflow_trigger_rel must be a WorkflowTrigger YAML path",
        )
    path = _safe_rel_path(rel)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="WorkflowTrigger file not found")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}") from e
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="WorkflowTrigger root must be a mapping")
    inp = data.get("input")
    if not isinstance(inp, dict):
        raise HTTPException(status_code=400, detail="WorkflowTrigger missing input mapping")
    cfg = inp.get("configuration")
    if not isinstance(cfg, dict):
        raise HTTPException(
            status_code=400,
            detail="WorkflowTrigger missing input.configuration mapping",
        )
    out_path = MODULE_ROOT / OPERATOR_RUN_SCOPE_SNAPSHOT
    out_path.write_text(
        yaml.safe_dump(
            cfg,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return OPERATOR_RUN_SCOPE_SNAPSHOT


@app.get("/api/workflow-trigger-meta")
def workflow_trigger_meta() -> dict:
    """Root `name` field per WorkflowTrigger YAML under workflows/ (operator UI nav labels)."""
    wf = MODULE_ROOT / "workflows"
    entries: List[Dict[str, Any]] = []
    if wf.is_dir():
        for p in sorted(wf.rglob("*")):
            if not p.is_file():
                continue
            if p.suffix.lower() not in {".yaml", ".yml"}:
                continue
            rel = str(p.relative_to(MODULE_ROOT)).replace("\\", "/")
            if not _is_workflow_trigger_path(rel):
                continue
            name: str | None = None
            try:
                data = yaml.safe_load(p.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    raw = data.get("name")
                    if isinstance(raw, str) and raw.strip():
                        name = raw
            except Exception:
                pass
            entries.append({"path": rel, "name": name})
    return {"entries": entries}


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
