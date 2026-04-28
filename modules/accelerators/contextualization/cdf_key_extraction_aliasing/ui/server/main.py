"""Local operator API — trusted workstation only; no authentication."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
import json
from typing import Any, Dict, Iterator, List, Literal, Set

import yaml
from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

_MODULE_DEFAULT = str(Path(__file__).resolve().parent.parent.parent)
MODULE_ROOT = Path(
    os.environ.get("CDF_KEY_EXTRACTION_ALIASING_ROOT") or _MODULE_DEFAULT
).resolve()
# So ``local_runner``, ``functions``, and ``scope_build`` imports work when uvicorn loads this module
# (same layout as ``module.py build`` / ``module.py ui``).
_mod_root_str = str(MODULE_ROOT)
_scripts = str(MODULE_ROOT / "scripts")
_functions = str(MODULE_ROOT / "functions")
for _p in (_scripts, _functions, _mod_root_str):
    if _p not in sys.path:
        sys.path.insert(0, _p)

DEFAULT_CONFIG_REL = "default.config.yaml"
DEFAULT_SCOPE_DOCUMENT_REL = "workflow.local.config.yaml"
TEMPLATE_WORKFLOW_CONFIG_REL = "workflow_template/workflow.template.config.yaml"
TEMPLATE_WORKFLOW_CANVAS_REL = "workflow_template/workflow.template.canvas.yaml"
# Snapshot of input.configuration from the last operator run against a WorkflowTrigger (optional .gitignore).
OPERATOR_RUN_SCOPE_SNAPSHOT = ".operator_run_scope.yaml"

app = FastAPI(title="Key discovery & aliasing operator API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5173",
        "http://localhost:5173",
        "http://127.0.0.1:5174",
        "http://localhost:5174",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _module_pythonpath() -> str:
    """Match ``module.py`` / deploy scripts: ``functions``, ``scripts``, package root."""
    parts = [
        str(MODULE_ROOT / "functions"),
        str(MODULE_ROOT / "scripts"),
        str(MODULE_ROOT),
    ]
    joined = ":".join(parts)
    prev = (os.environ.get("PYTHONPATH") or "").strip()
    return f"{joined}:{prev}" if prev else joined


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


def _embed_compiled_workflow_into_scope_file(rel: str | None) -> None:
    """Merge sibling canvas into the scope YAML on disk, compile the DAG, write root ``compiled_workflow``.

    Called after scope or canvas saves so local runs and operator snapshots stay aligned with CDF
    ``workflow.input`` (embedded IR matches ``compiled_workflow_for_local_run`` fast path).
    """
    from functions.cdf_fn_common.scope_canvas_merge import merge_sibling_canvas_yaml_into_scope
    from functions.cdf_fn_common.workflow_compile.canvas_dag import (
        CanvasCompileError,
        compiled_workflow_for_scope_document,
    )

    path = _scope_document_path(rel)
    if not path.is_file():
        return
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise HTTPException(status_code=400, detail="Scope document root must be a mapping")
    merge_sibling_canvas_yaml_into_scope(raw, path)
    try:
        cw = compiled_workflow_for_scope_document(raw)
    except CanvasCompileError as e:
        raise HTTPException(status_code=400, detail=f"Workflow canvas compile failed: {e}") from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    raw["compiled_workflow"] = cw
    try:
        text = yaml.safe_dump(
            raw,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
        yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=500, detail=f"Invalid YAML after compile: {e}") from e
    path.write_text(text, encoding="utf-8")


class YamlBody(BaseModel):
    content: str = Field(..., description="Full YAML text")


class BuildBody(BaseModel):
    force: bool = False
    dry_run: bool = False
    scope_suffix: str | None = None
    """When set, run ``module.py build --scope-suffix …`` for a single leaf only."""


RunTarget = Literal["workflow_local", "workflow_template", "workflow_trigger"]


class RunBody(BaseModel):
    run_all: bool = False
    target: RunTarget = "workflow_local"
    """Which scope document to pass to ``module.py run`` (see /api/run handler)."""
    workflow_trigger_rel: str | None = None
    """Module-relative path to a WorkflowTrigger YAML when ``target`` is ``workflow_trigger``."""


def _module_run_argv(body: RunBody) -> list[str]:
    """``python module.py run --config-path …`` argv (same semantics as ``/api/run``)."""
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
    return cmd


class DeployScopeBody(BaseModel):
    scope_suffix: str = Field(..., min_length=1)
    workflow_trigger_rel: str = Field(
        ...,
        min_length=1,
        description="Module-relative workflows/<suffix>/…WorkflowTrigger.yaml (scoped leaf only).",
    )
    skip_build: bool = False
    dry_run: bool = False
    """When true, YAML is validated and planned upserts are printed; no CDF calls."""
    allow_unresolved_placeholders: bool = False
    """If true, allow ``{{ ... }}`` Toolkit-style placeholders in YAML (CDF may still reject)."""


class CdfWorkflowRunBody(BaseModel):
    scope_suffix: str = Field(..., min_length=1)
    workflow_trigger_rel: str = Field(
        ...,
        min_length=1,
        description="Module-relative workflows/<suffix>/…WorkflowTrigger.yaml (scoped leaf only).",
    )
    dry_run: bool = False
    timeout_seconds: float = Field(7200.0, ge=30.0, le=86400.0)
    poll_interval: float = Field(5.0, ge=0.5, le=120.0)
    workflow_external_id: str | None = None


class FileBody(BaseModel):
    content: str


@app.get("/api/health")
def health() -> dict:
    return {"ok": True, "module_root": str(MODULE_ROOT)}


def _cdf_client():
    """CDF client from ``ClientConfig`` built from env (``.env`` via ``load_env``, same as ``module.py run``)."""
    try:
        from local_runner.client import create_cognite_client
        from local_runner.env import load_env
    except ImportError as e:
        raise HTTPException(
            status_code=503,
            detail=f"cognite-sdk / local_runner not available: {e}",
        ) from e
    load_env()
    try:
        return create_cognite_client()
    except RuntimeError as e:
        raise HTTPException(
            status_code=503,
            detail=str(e),
        ) from e
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Could not construct CogniteClient (check .env / COGNITE_* / CDF_* / IDP_*): {e}",
        ) from e


@app.get("/api/cdf/data-modeling/spaces")
def cdf_data_modeling_spaces(
    limit: int = Query(2000, ge=1, le=10000),
    include_global: bool = Query(False),
) -> dict:
    """List data modeling space identifiers (optional; UI prefers data-models picker)."""
    from cognite.client.exceptions import CogniteAPIError

    client = _cdf_client()
    seen: Set[str] = set()
    names: List[str] = []
    try:
        space_list = client.data_modeling.spaces.list(
            limit=limit, include_global=include_global
        )
        for s in space_list:
            sid = getattr(s, "space", None) or str(s)
            if sid not in seen:
                seen.add(sid)
                names.append(sid)
            if len(names) >= limit:
                break
    except CogniteAPIError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

    names.sort()
    return {"spaces": names}


@app.get("/api/cdf/data-modeling/data-models")
def cdf_data_modeling_data_models(
    limit: int = Query(2000, ge=1, le=10000),
    space: str | None = Query(
        None,
        max_length=512,
        description="Optional filter: only data models in this space (not the same as view ``schemaSpace``).",
    ),
    include_global: bool = Query(False),
    all_versions: bool = Query(False),
    inline_views: bool = Query(False),
) -> dict:
    """List data models; each row includes ``space`` to use as ``view_space`` for source views."""
    from cognite.client.exceptions import CogniteAPIError

    client = _cdf_client()
    rows: List[Dict[str, str]] = []
    space_f = space.strip() if space and space.strip() else None
    try:
        dm_list = client.data_modeling.data_models.list(
            limit=limit,
            space=space_f,
            include_global=include_global,
            all_versions=all_versions,
            inline_views=inline_views,
        )
        for dm in dm_list:
            rows.append(
                {
                    "space": dm.space,
                    "external_id": dm.external_id,
                    "version": dm.version,
                    "name": (dm.name or "").strip(),
                    "description": ((dm.description or "")[:500]).strip(),
                }
            )
            if len(rows) >= limit:
                break
    except CogniteAPIError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    rows.sort(key=lambda r: (r["space"], r["external_id"], r["version"]))
    return {"data_models": rows}


@app.get("/api/cdf/data-modeling/views")
def cdf_data_modeling_views(
    space: str = Query(..., min_length=1, max_length=512),
    limit: int = Query(8000, ge=1, le=20000),
    include_global: bool = Query(False),
    all_versions: bool = Query(False),
) -> dict:
    """List views in a data modeling space (for view_external_id / view_version pickers)."""
    from cognite.client.exceptions import CogniteAPIError

    client = _cdf_client()
    rows: List[Dict[str, str]] = []
    try:
        chunk = min(2500, limit)
        for view_list in client.data_modeling.views(
            chunk_size=chunk,
            space=space.strip(),
            include_global=include_global,
            all_versions=all_versions,
        ):
            for v in view_list:
                rows.append(
                    {
                        "space": v.space,
                        "external_id": v.external_id,
                        "version": v.version,
                    }
                )
                if len(rows) >= limit:
                    break
            if len(rows) >= limit:
                break
    except CogniteAPIError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    rows.sort(key=lambda r: (r["external_id"], r["version"]))
    return {"views": rows}


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
    _embed_compiled_workflow_into_scope_file(rel)
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
    _embed_compiled_workflow_into_scope_file(rel)
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
    _embed_compiled_workflow_into_scope_file(rel)
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
    _embed_compiled_workflow_into_scope_file(rel)
    r = str(path.relative_to(MODULE_ROOT)).replace("\\", "/")
    return {"ok": True, "path": r}


@app.post("/api/template-workflow-config/from-scope")
def copy_template_workflow_config_from_scope(
    scope_rel: str | None = Query(
        None,
        description="Scope document path under module root (default workflow.local.config.yaml)",
    ),
) -> dict:
    """Overwrite workflow.template.config.yaml with the saved scope document (same v1 shape).

    For promoting both scope and sibling canvas in one step, prefer
    ``POST /api/promote-local-workflow-templates``.
    """
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


@app.post("/api/promote-local-workflow-templates")
def promote_local_workflow_templates(
    scope_rel: str | None = Query(
        None,
        description=(
            "Scope document path under module root (default workflow.local.config.yaml). "
            "Copies that file to workflow.template.config.yaml and the sibling canvas to "
            "workflow.template.canvas.yaml (same as ``python module.py promote-local-templates``)."
        ),
    ),
) -> dict:
    """Overwrite workflow template config + canvas from saved local scope + sibling canvas."""
    scope_path = _scope_document_path(scope_rel)
    if not scope_path.is_file():
        raise HTTPException(status_code=404, detail="Scope document not found")
    canvas_src = _canvas_document_path(scope_rel)
    if not canvas_src.is_file():
        raise HTTPException(
            status_code=404,
            detail=f"Canvas document not found (expected next to scope): {canvas_src.name}",
        )

    scope_text = scope_path.read_text(encoding="utf-8")
    canvas_text = canvas_src.read_text(encoding="utf-8")
    try:
        yaml.safe_load(scope_text)
    except yaml.YAMLError as e:
        raise HTTPException(
            status_code=400, detail=f"Invalid YAML in scope document: {e}"
        ) from e
    try:
        yaml.safe_load(canvas_text)
    except yaml.YAMLError as e:
        raise HTTPException(
            status_code=400, detail=f"Invalid YAML in canvas document: {e}"
        ) from e

    template_cfg = _safe_rel_path(TEMPLATE_WORKFLOW_CONFIG_REL)
    template_canvas = _safe_rel_path(TEMPLATE_WORKFLOW_CANVAS_REL)
    template_cfg.parent.mkdir(parents=True, exist_ok=True)
    template_canvas.parent.mkdir(parents=True, exist_ok=True)
    template_cfg.write_text(scope_text, encoding="utf-8")
    template_canvas.write_text(canvas_text, encoding="utf-8")

    scope_rel_out = str(scope_path.relative_to(MODULE_ROOT)).replace("\\", "/")
    canvas_rel_out = str(canvas_src.relative_to(MODULE_ROOT)).replace("\\", "/")
    return {
        "ok": True,
        "config": {"from": scope_rel_out, "to": TEMPLATE_WORKFLOW_CONFIG_REL},
        "canvas": {"from": canvas_rel_out, "to": TEMPLATE_WORKFLOW_CANVAS_REL},
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
    if body.scope_suffix is not None and str(body.scope_suffix).strip():
        cmd.extend(["--scope-suffix", str(body.scope_suffix).strip()])
    env = {**os.environ, "PYTHONPATH": _module_pythonpath()}
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


@app.post("/api/deploy-scope")
def deploy_scope_cdf(body: DeployScopeBody) -> dict:
    """Run ``scripts/deploy_scope_cdf.py`` (SDK upsert of Workflow / WorkflowVersion / WorkflowTrigger)."""
    from cdf_deploy_scope_guard import (
        assert_scope_suffix_deployable,
        assert_trigger_path_under_module,
        assert_workflow_trigger_rel_matches_suffix,
    )

    suffix = body.scope_suffix.strip()
    wr = body.workflow_trigger_rel.strip()
    try:
        assert_scope_suffix_deployable(suffix)
        assert_workflow_trigger_rel_matches_suffix(wr, suffix)
        trig_path = assert_trigger_path_under_module(MODULE_ROOT, wr)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not trig_path.is_file():
        raise HTTPException(status_code=404, detail=f"WorkflowTrigger not found: {wr}")
    cmd = [
        sys.executable,
        str(MODULE_ROOT / "scripts" / "deploy_scope_cdf.py"),
        "--scope-suffix",
        suffix,
    ]
    if body.skip_build:
        cmd.append("--skip-build")
    if body.dry_run:
        cmd.append("--dry-run")
    if body.allow_unresolved_placeholders:
        cmd.append("--allow-unresolved-placeholders")
    env = {**os.environ, "PYTHONPATH": _module_pythonpath()}
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


@app.post("/api/cdf-workflow-run")
def cdf_workflow_run_endpoint(body: CdfWorkflowRunBody) -> dict:
    """Run ``scripts/cdf_workflow_run.py`` (SDK ``executions.run`` + poll)."""
    from cdf_deploy_scope_guard import (
        assert_scope_suffix_deployable,
        assert_trigger_path_under_module,
        assert_workflow_trigger_rel_matches_suffix,
    )

    suffix = body.scope_suffix.strip()
    wr = body.workflow_trigger_rel.strip()
    try:
        assert_scope_suffix_deployable(suffix)
        assert_workflow_trigger_rel_matches_suffix(wr, suffix)
        trig_path = assert_trigger_path_under_module(MODULE_ROOT, wr)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not trig_path.is_file():
        raise HTTPException(status_code=404, detail=f"WorkflowTrigger not found: {wr}")
    cmd = [
        sys.executable,
        str(MODULE_ROOT / "scripts" / "cdf_workflow_run.py"),
        "--scope-suffix",
        suffix,
        "--timeout-seconds",
        str(body.timeout_seconds),
        "--poll-interval",
        str(body.poll_interval),
    ]
    if body.dry_run:
        cmd.append("--dry-run")
    wfe = (body.workflow_external_id or "").strip()
    if wfe:
        cmd.extend(["--workflow-external-id", wfe])
    env = {**os.environ, "PYTHONPATH": _module_pythonpath()}
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
    cmd = _module_run_argv(body)
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


@app.post("/api/run-stream")
def run_pipeline_stream(body: RunBody) -> StreamingResponse:
    """Run the local pipeline and stream NDJSON lines (``task_start`` / ``task_end``) for canvas preview."""
    if sys.platform == "win32":
        raise HTTPException(
            status_code=501,
            detail="Progress streaming uses a POSIX pipe; use POST /api/run on Windows.",
        )

    cmd = _module_run_argv(body)
    r_fd, w_fd = os.pipe()
    try:
        child_env = {
            **os.environ,
            "KEA_UI_PROGRESS_FD": str(w_fd),
            "PYTHONPATH": _module_pythonpath(),
        }
        proc = subprocess.Popen(
            cmd,
            cwd=str(MODULE_ROOT),
            env=child_env,
            close_fds=True,
            pass_fds=(w_fd,),
        )
    except Exception:
        os.close(r_fd)
        os.close(w_fd)
        raise

    os.close(w_fd)

    def ndjson_iter() -> Iterator[bytes]:
        try:
            with os.fdopen(r_fd, "r", encoding="utf-8", newline="\n") as rf:
                for line in rf:
                    yield line.encode("utf-8")
        finally:
            rc = proc.wait()
            yield (
                json.dumps({"event": "exit", "code": int(rc or 0)}, ensure_ascii=False) + "\n"
            ).encode("utf-8")

    return StreamingResponse(ndjson_iter(), media_type="application/x-ndjson")


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
    snapshot: Dict[str, Any] = dict(cfg)
    cw = inp.get("compiled_workflow")
    if isinstance(cw, dict):
        tasks = cw.get("tasks")
        if isinstance(tasks, list) and tasks:
            snapshot["compiled_workflow"] = cw
    out_path = MODULE_ROOT / OPERATOR_RUN_SCOPE_SNAPSHOT
    out_path.write_text(
        yaml.safe_dump(
            snapshot,
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
