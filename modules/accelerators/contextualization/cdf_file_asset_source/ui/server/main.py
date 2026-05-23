"""Local operator API — trusted workstation only; no authentication."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal

import yaml
from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

_MODULE_DEFAULT = str(Path(__file__).resolve().parent.parent.parent)
MODULE_ROOT = Path(os.environ.get("CDF_FILE_ASSET_SOURCE_ROOT") or _MODULE_DEFAULT).resolve()

_mod_root = str(MODULE_ROOT)
_functions = str(MODULE_ROOT / "functions")
for _p in (_functions, _mod_root):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from local_runner.paths import DEFAULT_CONFIG_REL  # noqa: E402
from local_runner.validate import validate_default_config  # noqa: E402

_RUN_RESULTS_PREFIX = "local_run_results/"
_ARTIFACT_PREFIXES = ("workflows/", "patterns/")
_MAX_RUN_RESULT_PREVIEW_BYTES = 50 * 1024 * 1024
_LIST_RUN_SCOPE_MAX_BYTES = 256 * 1024

app = FastAPI(title="CDF file asset source operator API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5188",
        "http://localhost:5188",
        "http://127.0.0.1:5173",
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _module_pythonpath() -> str:
    repo = MODULE_ROOT.parent.parent.parent.parent
    parts = [_functions, _mod_root, str(repo)]
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


class YamlBody(BaseModel):
    content: str


class ValidateBody(BaseModel):
    steps: list[str] | None = None


class RunBody(BaseModel):
    step: Literal["extract", "create", "write", "all"] = "all"


def _module_run_argv(step: str) -> list[str]:
    return [
        sys.executable,
        str(MODULE_ROOT / "module.py"),
        "run",
        "--step",
        step,
    ]


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"ok": True, "status": "ok", "module_root": str(MODULE_ROOT)}


@app.get("/api/config-steps")
def list_config_steps() -> dict[str, Any]:
    """Configure-tab steps backed by ``default.config.yaml``."""
    return {
        "default_config": DEFAULT_CONFIG_REL,
        "steps": [
            {"id": "scope", "label": "Scope"},
            {"id": "extract", "label": "Extract"},
        ],
    }


@app.get("/api/default-config")
def get_default_config() -> dict:
    p = MODULE_ROOT / DEFAULT_CONFIG_REL
    if not p.is_file():
        return {"path": DEFAULT_CONFIG_REL, "content": ""}
    return {"path": DEFAULT_CONFIG_REL, "content": p.read_text(encoding="utf-8")}


@app.put("/api/default-config")
def put_default_config(body: YamlBody) -> dict:
    p = MODULE_ROOT / DEFAULT_CONFIG_REL
    try:
        yaml.safe_load(body.content)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}") from e
    p.write_text(body.content, encoding="utf-8")
    return {"ok": True, "path": DEFAULT_CONFIG_REL}


@app.post("/api/validate")
def post_validate(body: ValidateBody | None = None) -> dict:
    steps = body.steps if body and body.steps else None
    return validate_default_config(steps)


@app.post("/api/run")
def post_run(body: RunBody) -> dict:
    cmd = _module_run_argv(body.step)
    proc = subprocess.run(
        cmd,
        cwd=str(MODULE_ROOT),
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": _module_pythonpath()},
    )
    run_json: Any = None
    if proc.stdout.strip():
        try:
            run_json = json.loads(proc.stdout)
        except json.JSONDecodeError:
            run_json = None
    return {
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "run": run_json,
    }


@app.post("/api/run-stream")
def post_run_stream(body: RunBody) -> StreamingResponse:
    """Run the local pipeline and stream NDJSON progress (``task_start`` / ``task_end`` / ``log``)."""
    if sys.platform == "win32":
        raise HTTPException(
            status_code=501,
            detail="Progress streaming uses a POSIX pipe; use POST /api/run on Windows.",
        )

    cmd = _module_run_argv(body.step)
    r_fd, w_fd = os.pipe()
    try:
        child_env = {
            **os.environ,
            "FAS_UI_PROGRESS_FD": str(w_fd),
            "PYTHONPATH": _module_pythonpath(),
            "PYTHONUNBUFFERED": "1",
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

    return StreamingResponse(
        ndjson_iter(),
        media_type="application/x-ndjson",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _peek_run_scope(path: Path) -> Any:
    """Extract run_scope without loading huge JSON payloads."""
    try:
        sz = path.stat().st_size
        if sz > _LIST_RUN_SCOPE_MAX_BYTES:
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data.get("run_scope")
    except (OSError, json.JSONDecodeError):
        pass
    return None


@app.get("/api/run-results")
def list_run_results() -> dict:
    root = MODULE_ROOT / "local_run_results"
    if not root.is_dir():
        return {"items": []}
    candidates = sorted(root.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    items = []
    for p in candidates[:50]:
        rel = str(p.relative_to(MODULE_ROOT)).replace("\\", "/")
        items.append(
            {
                "path": rel,
                "run_scope": _peek_run_scope(p),
                "mtime_ms": int(p.stat().st_mtime * 1000),
            }
        )
    return {"items": items}


@app.get("/api/run-results/preview")
def preview_run_result(
    rel: str = Query(...),
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=500),
) -> dict:
    rel_n = rel.strip().replace("\\", "/")
    if not rel_n.startswith(_RUN_RESULTS_PREFIX):
        raise HTTPException(status_code=400, detail="Path must be under local_run_results/")
    path = _safe_rel_path(rel_n)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    sz = path.stat().st_size
    if sz > _MAX_RUN_RESULT_PREVIEW_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large for preview ({sz} bytes; max {_MAX_RUN_RESULT_PREVIEW_BYTES})",
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        results = data["results"]
        total = len(results)
        chunk = results[offset : offset + limit]
        return {
            "path": rel_n,
            "total": total,
            "offset": offset,
            "limit": limit,
            "items": chunk,
            "data": data,
        }
    return {"path": rel_n, "data": data}


@app.get("/api/artifacts")
def list_artifacts(prefix: str = Query("workflows/", description="workflows/ | patterns/")) -> dict:
    if prefix not in _ARTIFACT_PREFIXES:
        raise HTTPException(status_code=400, detail=f"prefix must be one of {_ARTIFACT_PREFIXES}")
    root = MODULE_ROOT / prefix.rstrip("/")
    if not root.is_dir():
        return {"prefix": prefix, "items": []}
    items = []
    for p in sorted(root.rglob("*")):
        if p.is_file() and p.suffix in (".yaml", ".yml"):
            items.append(str(p.relative_to(MODULE_ROOT)).replace("\\", "/"))
    return {"prefix": prefix, "items": items}


@app.get("/api/file")
def read_module_file(rel: str = Query(...)) -> dict:
    path = _safe_rel_path(rel)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    return {
        "path": str(path.relative_to(MODULE_ROOT)).replace("\\", "/"),
        "content": path.read_text(encoding="utf-8"),
    }


@app.get("/api/industry-templates")
def list_industry_templates() -> dict:
    items = []
    for p in sorted(MODULE_ROOT.glob("config.template.*.yaml")):
        items.append(str(p.relative_to(MODULE_ROOT)).replace("\\", "/"))
    return {"items": items}


