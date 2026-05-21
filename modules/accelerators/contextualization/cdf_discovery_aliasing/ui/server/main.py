"""Local operator API — trusted workstation only; no authentication."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterator, List, Literal, Mapping, Set

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

from scope_build.scope_yaml_io import (  # noqa: E402
    SCOPE_DOCUMENT_DUMP_KWARGS,
    dump_scope_document_yaml_roundtrip,
)
from local_runner.discovery_run_v2 import DISCOVERY_RUN_SUFFIX  # noqa: E402
from ui.server import discovery_run_results as _discovery_api  # noqa: E402

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


_RUN_RESULTS_PREFIX = "local_run_results/"
_CDF_PIPELINE_RESULT_BASENAME = re.compile(
    r"^\d{8}_\d{6}_cdf_(extraction|aliasing)\.json$"
)
_DISCOVERY_RUN_BASENAME = re.compile(rf"^\d{{8}}_\d{{6}}{re.escape(DISCOVERY_RUN_SUFFIX)}$")
_MAX_RUN_RESULT_PREVIEW_BYTES = 50 * 1024 * 1024


def _run_results_root() -> Path:
    return MODULE_ROOT / "local_run_results"


def _norm_run_scope_key(key: str) -> str:
    return key.strip().replace("\\", "/")


def _run_scope_matches_key(run_scope: Any, run_scope_key: str) -> bool:
    """*run_scope_key*: ``workflow_local`` | ``workflow_template`` | ``workflow_trigger:<moduleRel>``."""
    key = _norm_run_scope_key(run_scope_key)
    if not key:
        return True
    if not isinstance(run_scope, dict):
        return False
    if ":" in key:
        prefix, rest = key.split(":", 1)
        prefix = prefix.strip().lower()
        rest = _norm_run_scope_key(rest)
        if prefix != "workflow_trigger":
            return False
        if str(run_scope.get("target") or "").strip().lower() != "workflow_trigger":
            return False
        return _norm_run_scope_key(str(run_scope.get("workflow_trigger_rel") or "")) == rest
    want = key.lower()
    if want not in ("workflow_local", "workflow_template"):
        return False
    return str(run_scope.get("target") or "").strip().lower() == want


def _read_json_run_scope(path: Path) -> Any:
    """Top-level ``run_scope`` from a JSON document (extraction/aliasing or discovery_run)."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if isinstance(data, dict):
        return data.get("run_scope")
    return None


def _read_extraction_run_scope(extraction_path: Path) -> Any:
    return _read_json_run_scope(extraction_path)


def _resolve_run_result_pipeline_file(rel: str) -> Path:
    rel_n = rel.strip().replace("\\", "/")
    if not rel_n.startswith(_RUN_RESULTS_PREFIX):
        raise HTTPException(status_code=400, detail="Path must be under local_run_results/")
    path = _safe_rel_path(rel_n)
    root_resolved = _run_results_root().resolve()
    try:
        path.relative_to(root_resolved)
    except ValueError as e:
        raise HTTPException(status_code=400, detail="Path escapes local_run_results") from e
    if not _CDF_PIPELINE_RESULT_BASENAME.match(path.name):
        raise HTTPException(
            status_code=400,
            detail="Only YYYYMMDD_HHMMSS_cdf_extraction.json or _cdf_aliasing.json",
        )
    return path


def _rel_under_module(path: Path) -> str:
    return str(path.relative_to(MODULE_ROOT)).replace("\\", "/")


def _resolve_discovery_run_path(rel: str) -> Path:
    return _discovery_api.resolve_discovery_run_path(
        rel,
        run_results_prefix=_RUN_RESULTS_PREFIX,
        run_results_root=_run_results_root(),
        safe_rel_path=_safe_rel_path,
        rel_under_module=_rel_under_module,
    )


def _load_json_object(path: Path, *, max_bytes: int = _MAX_RUN_RESULT_PREVIEW_BYTES) -> Dict[str, Any]:
    if not path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    sz = path.stat().st_size
    if sz > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"File too large for preview ({sz} bytes; max {max_bytes})",
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}") from e
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Expected JSON object")
    return data


def _load_discovery_run(rel: str) -> Dict[str, Any]:
    path = _resolve_discovery_run_path(rel)
    return _discovery_api.load_discovery_run_json(path, _load_json_object)


def _scope_document_path(rel: str | None) -> Path:
    r = (rel or "").strip() or DEFAULT_SCOPE_DOCUMENT_REL
    return _safe_rel_path(r)


def _persist_canvas_model_to_scope(rel: str | None, canvas_model: Dict[str, Any]) -> str:
    """Write ``canvas`` inside the scope YAML at *rel* and validate compile."""
    from functions.cdf_fn_common.workflow_compile.canvas_dag import CanvasCompileError
    from scope_build.scope_yaml_io import (
        compile_validate_scope_document,
        dump_scope_document_yaml_roundtrip,
        load_scope_document_dict_normalized,
    )

    scope_path = _scope_document_path(rel)
    if not scope_path.is_file():
        raise HTTPException(status_code=404, detail="Scope document not found")
    try:
        raw = load_scope_document_dict_normalized(scope_path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    raw["canvas"] = deepcopy(canvas_model)
    raw.pop("compiled_workflow", None)
    try:
        compile_validate_scope_document(raw)
    except CanvasCompileError as e:
        raise HTTPException(status_code=400, detail=f"Workflow canvas compile failed: {e}") from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    try:
        text = dump_scope_document_yaml_roundtrip(raw)
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    scope_path.write_text(text, encoding="utf-8")
    return str(scope_path.relative_to(MODULE_ROOT)).replace("\\", "/")


def _embed_compiled_workflow_into_scope_file(rel: str | None) -> None:
    """Normalize scope graph keys and validate compile.

    Called after scope or canvas saves. CDF uses WorkflowVersion for task IR; trigger carries trimmed
    ``configuration`` only.
    """
    from functions.cdf_fn_common.workflow_compile.canvas_dag import CanvasCompileError
    from scope_build.scope_yaml_io import (
        compile_validate_scope_document,
        dump_scope_document_yaml_roundtrip,
        load_scope_document_dict_normalized,
    )

    path = _scope_document_path(rel)
    if not path.is_file():
        return
    try:
        raw = load_scope_document_dict_normalized(path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    raw.pop("compiled_workflow", None)
    try:
        compile_validate_scope_document(raw)
    except CanvasCompileError as e:
        raise HTTPException(status_code=400, detail=f"Workflow canvas compile failed: {e}") from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    try:
        text = dump_scope_document_yaml_roundtrip(raw)
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
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
    deploy_functions: Literal["never", "if-missing", "if-stale", "always"] = "if-stale"
    """Passed to ``deploy_scope_cdf.py --deploy-functions`` (Cognite Functions from ``functions.Function.yaml``)."""


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
    instance_space: str | None = Field(
        default=None,
        description="Passed as ``--instance-space`` to substitute ``{{instance_space}}`` in trigger input.",
    )


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
        text = dump_scope_document_yaml_roundtrip(model)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
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
        text = dump_scope_document_yaml_roundtrip(model)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
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
        description="Scope document path under module root",
    ),
) -> dict:
    """Return embedded ``canvas`` from the scope YAML, or empty content."""
    scope_path = _scope_document_path(rel)
    scope_r = str(scope_path.relative_to(MODULE_ROOT)).replace("\\", "/")
    if scope_path.is_file():
        try:
            doc = yaml.safe_load(scope_path.read_text(encoding="utf-8"))
        except yaml.YAMLError:
            doc = None
        if isinstance(doc, dict):
            c = doc.get("canvas")
            if isinstance(c, dict) and isinstance(c.get("nodes"), list) and c.get("nodes"):
                text = yaml.safe_dump(c, **SCOPE_DOCUMENT_DUMP_KWARGS)
                return {
                    "path": scope_r,
                    "scope_rel": scope_r,
                    "content": text,
                    "storage": "scope",
                }
    return {"path": scope_r, "scope_rel": scope_r, "content": "", "storage": "scope"}


@app.put("/api/canvas-document")
def put_canvas_document(
    body: YamlBody,
    rel: str | None = Query(
        None,
        description="Scope document path under module root (canvas is stored under ``canvas`` in this file)",
    ),
) -> dict:
    try:
        loaded = yaml.safe_load(body.content)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}") from e
    if not isinstance(loaded, dict):
        raise HTTPException(status_code=400, detail="Canvas YAML must be a mapping")
    from functions.cdf_fn_common.scope_canvas_merge import canvas_dict_from_layout_yaml

    cd = canvas_dict_from_layout_yaml(loaded)
    if cd is None:
        raise HTTPException(
            status_code=400,
            detail="Canvas YAML must define non-empty nodes (root or under canvas:)",
        )
    r = _persist_canvas_model_to_scope(rel, cd)
    return {"ok": True, "path": r, "scope_rel": r}


@app.get("/api/canvas-document/model")
def get_canvas_document_model(
    rel: str | None = Query(
        None,
        description="Scope document path under module root",
    ),
) -> Dict[str, Any]:
    scope_path = _scope_document_path(rel)
    if scope_path.is_file():
        doc = yaml.safe_load(scope_path.read_text(encoding="utf-8"))
        if isinstance(doc, dict):
            c = doc.get("canvas")
            if isinstance(c, dict) and isinstance(c.get("nodes"), list) and c.get("nodes"):
                return c
    return {}


@app.put("/api/canvas-document/model")
def put_canvas_document_model(
    model: Dict[str, Any] = Body(...),
    rel: str | None = Query(
        None,
        description="Scope document path under module root",
    ),
) -> dict:
    try:
        dump_scope_document_yaml_roundtrip(model)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    r = _persist_canvas_model_to_scope(rel, model)
    return {"ok": True, "path": r, "scope_rel": r}


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


@app.post("/api/promote-local-workflow-templates")
def promote_local_workflow_templates(
    scope_rel: str | None = Query(
        None,
        description=(
            "Scope document path under module root (default workflow.local.config.yaml). "
            "Writes ``workflow.template.config.yaml`` (same as ``python module.py promote-local-templates``)."
        ),
    ),
) -> dict:
    """Overwrite workflow template config from local scope (unified document including ``canvas``)."""
    from scope_build.scope_yaml_io import promote_unified_scope_file_to_template_config

    scope_path = _scope_document_path(scope_rel)
    if not scope_path.is_file():
        raise HTTPException(status_code=404, detail="Scope document not found")
    template_cfg = _safe_rel_path(TEMPLATE_WORKFLOW_CONFIG_REL)
    try:
        promote_unified_scope_file_to_template_config(source=scope_path, destination=template_cfg)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    scope_rel_out = str(scope_path.relative_to(MODULE_ROOT)).replace("\\", "/")
    return {
        "ok": True,
        "config": {"from": scope_rel_out, "to": TEMPLATE_WORKFLOW_CONFIG_REL},
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
    cmd.extend(["--deploy-functions", body.deploy_functions])
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
    ins = (body.instance_space or "").strip()
    if ins:
        cmd.extend(["--instance-space", ins])
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
        env={
            **os.environ,
            "KEA_OPERATOR_RUN_TARGET": body.target,
            "KEA_OPERATOR_WORKFLOW_TRIGGER_REL": (body.workflow_trigger_rel or "").strip(),
        },
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
            "KEA_OPERATOR_RUN_TARGET": body.target,
            "KEA_OPERATOR_WORKFLOW_TRIGGER_REL": (body.workflow_trigger_rel or "").strip(),
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


def _scope_config_rel_from_workflow_trigger_rel(trigger_rel: str) -> str:
    """Sibling ``*.config.yaml`` path for a scoped WorkflowTrigger (same stem, ASCII case preserved on prefix)."""
    tr = trigger_rel.strip().replace("\\", "/")
    lower = tr.lower()
    if lower.endswith(".workflowtrigger.yaml"):
        return f"{tr[: -len('.workflowtrigger.yaml')]}.config.yaml"
    if lower.endswith(".workflowtrigger.yml"):
        return f"{tr[: -len('.workflowtrigger.yml')]}.config.yml"
    raise HTTPException(
        status_code=400,
        detail="workflow_trigger_rel must end with .workflowtrigger.yaml or .workflowtrigger.yml",
    )


class ScopedWorkflowPublishBody(BaseModel):
    workflow_trigger_rel: str = Field(..., min_length=1)
    full_scope_document: Dict[str, Any] = Field(
        ...,
        description="Unified v1 scope document (full); persisted to sibling *.config.yaml",
    )
    workflow_trigger_yaml: str | None = Field(
        None,
        description="Optional full WorkflowTrigger YAML from the editor; when set, replaces disk "
        "shell (except input.configuration, which is set from trimmed full scope).",
    )


@app.post("/api/scoped-workflow-publish")
def scoped_workflow_publish(body: ScopedWorkflowPublishBody) -> dict:
    """Write full scope to ``*.config.yaml`` and trimmed ``input.configuration`` into the WorkflowTrigger."""
    from scope_build.scope_document_limits import (  # noqa: PLC0415
        assert_scope_document_within_limit,
        assert_workflow_trigger_input_within_limit,
    )
    from scope_build.scope_document_patch import scope_configuration_for_workflow_trigger  # noqa: PLC0415
    from scope_build.scope_yaml_io import (  # noqa: PLC0415
        compile_validate_scope_document,
        dump_scope_document_yaml_roundtrip,
        load_scope_document_dict_normalized,
    )

    rel = body.workflow_trigger_rel.strip().replace("\\", "/")
    if not _is_workflow_trigger_path(rel):
        raise HTTPException(
            status_code=400,
            detail="workflow_trigger_rel must be a WorkflowTrigger YAML path",
        )
    trig_path = _safe_rel_path(rel)
    if not trig_path.is_file():
        raise HTTPException(status_code=404, detail="WorkflowTrigger file not found")
    scope_rel = _scope_config_rel_from_workflow_trigger_rel(rel)
    scope_path = _safe_rel_path(scope_rel)
    doc_in = deepcopy(body.full_scope_document)
    if not isinstance(doc_in, dict):
        raise HTTPException(status_code=400, detail="full_scope_document must be a JSON object")
    try:
        compile_validate_scope_document(doc_in)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    scope_path.parent.mkdir(parents=True, exist_ok=True)
    scope_path.write_text(dump_scope_document_yaml_roundtrip(doc_in), encoding="utf-8")
    _embed_compiled_workflow_into_scope_file(scope_rel)
    try:
        final_scope = load_scope_document_dict_normalized(scope_path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    trimmed = scope_configuration_for_workflow_trigger(final_scope)
    scope_id = scope_path.parent.name
    assert_scope_document_within_limit(trimmed, scope_id=scope_id)
    raw_yaml = (body.workflow_trigger_yaml or "").strip()
    try:
        if raw_yaml:
            trig_data = yaml.safe_load(raw_yaml)
        else:
            trig_data = yaml.safe_load(trig_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise HTTPException(status_code=500, detail=f"Invalid WorkflowTrigger YAML: {e}") from e
    if not isinstance(trig_data, dict):
        raise HTTPException(status_code=500, detail="WorkflowTrigger root must be a mapping")
    inp = trig_data.setdefault("input", {})
    if not isinstance(inp, dict):
        raise HTTPException(status_code=500, detail="WorkflowTrigger input must be a mapping")
    inp["configuration"] = trimmed
    assert_workflow_trigger_input_within_limit(inp, scope_id=scope_id)
    trig_path.write_text(
        yaml.safe_dump(trig_data, **SCOPE_DOCUMENT_DUMP_KWARGS),
        encoding="utf-8",
    )
    return {
        "ok": True,
        "workflow_trigger_rel": rel.replace("\\", "/"),
        "scope_document_rel": str(scope_path.relative_to(MODULE_ROOT)).replace("\\", "/"),
    }


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
    out_path = MODULE_ROOT / OPERATOR_RUN_SCOPE_SNAPSHOT
    out_path.write_text(yaml.safe_dump(snapshot, **SCOPE_DOCUMENT_DUMP_KWARGS), encoding="utf-8")
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


@app.get("/api/run-results")
def list_run_results(
    run_scope_key: str | None = Query(
        None,
        description="Filter: workflow_local | workflow_template | workflow_trigger:<WorkflowTriggerRel>",
    ),
) -> dict:
    """Paired ``module.py run`` JSON under ``local_run_results/`` (timestamped *_cdf_*.json)."""
    root = _run_results_root()
    root.mkdir(parents=True, exist_ok=True)
    runs: List[Dict[str, Any]] = []
    if not root.is_dir():
        return {"runs": []}
    scope_filter = (run_scope_key or "").strip()
    for p in sorted(root.glob("*_cdf_extraction.json"), key=lambda x: x.stat().st_mtime, reverse=True):
        if not _CDF_PIPELINE_RESULT_BASENAME.match(p.name):
            continue
        stem_ts = p.stem.replace("_cdf_extraction", "")
        alias = root / f"{stem_ts}_cdf_aliasing.json"
        if not alias.is_file() or not _CDF_PIPELINE_RESULT_BASENAME.match(alias.name):
            continue
        if scope_filter:
            rs = _read_extraction_run_scope(p)
            if not _run_scope_matches_key(rs, scope_filter):
                continue
        runs.append(
            {
                "stem": stem_ts,
                "extraction_rel": str(p.relative_to(MODULE_ROOT)).replace("\\", "/"),
                "aliasing_rel": str(alias.relative_to(MODULE_ROOT)).replace("\\", "/"),
                "mtime_ms": int(p.stat().st_mtime * 1000),
            }
        )
    return {"runs": runs}


@app.get("/api/run-results/preview")
def preview_run_result(
    rel: str = Query(..., description="Module-relative path under local_run_results/"),
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=500),
) -> dict:
    path = _resolve_run_result_pipeline_file(rel)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    sz = path.stat().st_size
    if sz > _MAX_RUN_RESULT_PREVIEW_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large for preview ({sz} bytes; max {_MAX_RUN_RESULT_PREVIEW_BYTES})",
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}") from e
    if not isinstance(data, dict) or not isinstance(data.get("results"), list):
        raise HTTPException(status_code=400, detail="Expected object with results array")
    results = data["results"]
    total = len(results)
    chunk = results[offset : offset + limit]
    return {"total": total, "offset": offset, "limit": limit, "items": chunk}


@app.get("/api/run-results/discovery")
def list_discovery_run_results(
    run_scope_key: str | None = Query(
        None,
        description="Filter: workflow_local | workflow_template | workflow_trigger:<WorkflowTriggerRel>",
    ),
) -> dict:
    """Timestamped ``*_discovery_run.json`` under ``local_run_results/`` (schema v2)."""
    return _discovery_api.list_discovery_runs(
        run_results_root=_run_results_root(),
        run_results_prefix=_RUN_RESULTS_PREFIX,
        rel_under_module=_rel_under_module,
        read_json_run_scope=_read_json_run_scope,
        run_scope_matches_key=_run_scope_matches_key,
        load_json_object=_load_json_object,
        summary_from_run=_discovery_api.summary_from_discovery_run,
        run_scope_key=run_scope_key,
    )


@app.get("/api/run-results/discovery-detail")
def discovery_run_detail(
    rel: str = Query(..., description="Module-relative path to *_discovery_run.json"),
) -> dict:
    """Run summary and scope for a discovery local run (v2)."""
    path = _resolve_discovery_run_path(rel)
    data = _load_discovery_run(rel)
    return _discovery_api.discovery_run_detail_payload(path, data, _rel_under_module)


@app.get("/api/run-results/discovery-pipeline-tasks")
def preview_discovery_pipeline_tasks(
    rel: str = Query(..., description="Module-relative *_discovery_run.json"),
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=500),
    category: str | None = Query(None, description="Filter pipeline.tasks by category"),
) -> dict:
    """Paginate ``pipeline.tasks`` from a discovery run document."""
    data = _load_discovery_run(rel)
    if category and str(category).strip():
        cat = str(category).strip().lower()
        pipeline = data.get("pipeline") if isinstance(data.get("pipeline"), dict) else {}
        tasks = pipeline.get("tasks") if isinstance(pipeline.get("tasks"), list) else []
        filtered = [
            t for t in tasks if isinstance(t, dict) and str(t.get("category") or "").lower() == cat
        ]
        data = {**data, "pipeline": {"tasks": filtered, "task_count": len(filtered)}}
    page = _discovery_api.paginate_pipeline_tasks(data, offset=offset, limit=limit)
    page["run_rel"] = rel.strip().replace("\\", "/")
    return page


@app.get("/api/run-results/discovery-persistence-nodes")
def list_discovery_persistence_nodes(
    rel: str = Query(..., description="Module-relative *_discovery_run.json"),
) -> dict:
    """All persistence / save / indexing nodes from ``persistence.nodes``."""
    data = _load_discovery_run(rel)
    nodes = _discovery_api.persistence_nodes_from_data(data)
    return {"run_rel": rel.strip().replace("\\", "/"), "items": nodes, "total": len(nodes)}


@app.get("/api/run-results/discovery-persistence-node")
def discovery_persistence_node_detail(
    rel: str = Query(..., description="Module-relative *_discovery_run.json"),
    task_id: str = Query(..., description="Compiled workflow task id for the persistence node"),
) -> dict:
    """One persistence node (input cohort + output sink / handler result)."""
    data = _load_discovery_run(rel)
    node = _discovery_api.persistence_node_by_task_id(data, task_id)
    return {"run_rel": rel.strip().replace("\\", "/"), "node": node}


@app.get("/api/run-results/discovery-persistence-merged")
def discovery_persistence_merged(
    rel: str = Query(..., description="Module-relative *_discovery_run.json"),
) -> dict:
    """Merged entities across all persistence nodes."""
    data = _load_discovery_run(rel)
    merged = _discovery_api.merged_entities_from_data(data)
    return {
        "run_rel": rel.strip().replace("\\", "/"),
        "merged_entities": merged,
        "instance_count": merged.get("instance_count"),
        "inverted_index_sink_row_count": merged.get("inverted_index_sink_row_count"),
    }


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
