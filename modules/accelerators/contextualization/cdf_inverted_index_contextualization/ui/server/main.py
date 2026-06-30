"""Inverted index operator API — trusted workstation only; no authentication."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from ui.server.paths import MODULE_ROOT  # noqa: F401

from ui.server.config_api import router as config_router  # noqa: E402
from ui.server.index_api import router as index_router  # noqa: E402
from ui.server.workspace_api import router as workspace_router  # noqa: E402

app = FastAPI(title="Inverted Index operator API", version="1.0.0")
app.include_router(config_router)
app.include_router(index_router)
app.include_router(workspace_router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5195",
        "http://localhost:5195",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _load_env() -> None:
    from local_runner.env import load_env

    load_env(MODULE_ROOT)


def _connection_info() -> dict:
    import os as _os

    project = (
        _os.getenv("COGNITE_PROJECT") or _os.getenv("PROJECT") or _os.getenv("CDF_PROJECT") or ""
    )
    base_url = (
        _os.getenv("COGNITE_BASE_URL")
        or _os.getenv("BASE_URL")
        or _os.getenv("CDF_BASE_URL")
        or _os.getenv("CDF_URL")
        or ""
    )
    cluster = _os.getenv("CDF_CLUSTER") or ""
    if not base_url and cluster:
        base_url = f"https://{cluster}.cognitedata.com"
    return {
        "project": project,
        "base_url": base_url,
        "cluster": cluster,
        "auth_mode": _auth_mode(),
    }


def _auth_mode() -> str:
    import os as _os

    if _os.getenv("COGNITE_API_KEY") or _os.getenv("API_KEY") or _os.getenv("CDF_API_KEY"):
        return "api_key"
    if (_os.getenv("COGNITE_CLIENT_ID") or _os.getenv("CLIENT_ID")) and (
        _os.getenv("COGNITE_CLIENT_SECRET") or _os.getenv("CLIENT_SECRET")
    ):
        return "oauth"
    return "unknown"


@app.get("/api/health")
def health() -> dict:
    return {"ok": True, "module_root": str(MODULE_ROOT)}


@app.get("/api/connection")
def connection() -> dict:
    try:
        _load_env()
    except Exception:
        pass
    info = _connection_info()
    if not info.get("project") or not info.get("base_url"):
        raise HTTPException(
            status_code=503,
            detail="Missing CDF connection environment (set COGNITE_PROJECT and CDF_CLUSTER/COGNITE_BASE_URL)",
        )
    return info
