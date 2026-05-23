"""CDF Explorer operator API — trusted workstation only; no authentication."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

_MODULE_DEFAULT = str(Path(__file__).resolve().parent.parent.parent)
MODULE_ROOT = Path(os.environ.get("CDF_EXPLORER_ROOT") or _MODULE_DEFAULT).resolve()
_mod_root_str = str(MODULE_ROOT)
if _mod_root_str not in sys.path:
    sys.path.insert(0, _mod_root_str)

from ui.server import cdf_browse, explorer_config, explorer_tree, file_content_query  # noqa: E402

app = FastAPI(title="CDF Explorer operator API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5193",
        "http://localhost:5193",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _cdf_client():
    try:
        from local_runner.client import create_cognite_client
        from local_runner.env import load_env
    except ImportError as e:
        raise HTTPException(status_code=503, detail=f"local_runner not available: {e}") from e
    load_env()
    try:
        return create_cognite_client()
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Could not construct CogniteClient (check .env): {e}",
        ) from e


def _api_error(e: Exception) -> HTTPException:
    from cognite.client.exceptions import CogniteAPIError

    if isinstance(e, ValueError):
        return HTTPException(status_code=400, detail=str(e))
    if isinstance(e, CogniteAPIError):
        return HTTPException(status_code=502, detail=str(e))
    return HTTPException(status_code=500, detail=str(e))


@app.get("/api/health")
def health() -> dict:
    return {"ok": True, "module_root": str(MODULE_ROOT)}


@app.get("/api/connection")
def connection() -> dict:
    client = _cdf_client()
    return cdf_browse.connection_info(client)


class ExplorerStarsBody(BaseModel):
    node_ids: list[str] = Field(default_factory=list, max_length=10_000)


class ExplorerWorkspaceBody(BaseModel):
    active_tab_id: str | None = None
    tabs: list[dict] = Field(default_factory=list, max_length=50)


class ExplorerSavedQueriesBody(BaseModel):
    queries: list[dict] = Field(default_factory=list, max_length=200)


@app.get("/api/cdf/explorer/config")
def explorer_config_get() -> dict:
    return explorer_config.public_config()


@app.put("/api/cdf/explorer/config/stars")
def explorer_config_set_stars(body: ExplorerStarsBody) -> dict:
    try:
        node_ids = explorer_config.set_starred_node_ids(body.node_ids)
    except Exception as e:
        raise _api_error(e) from e
    return {"stars": {"node_ids": node_ids}}


@app.put("/api/cdf/explorer/config/workspace")
def explorer_config_set_workspace(body: ExplorerWorkspaceBody) -> dict:
    try:
        workspace = explorer_config.set_workspace(
            {"active_tab_id": body.active_tab_id, "tabs": body.tabs}
        )
    except Exception as e:
        raise _api_error(e) from e
    return {"workspace": workspace}


@app.put("/api/cdf/explorer/config/saved-queries")
def explorer_config_set_saved_queries(body: ExplorerSavedQueriesBody) -> dict:
    try:
        queries = explorer_config.set_saved_queries(body.queries)
    except Exception as e:
        raise _api_error(e) from e
    return {"saved_queries": {"queries": queries}}


@app.get("/api/cdf/explorer/children")
def explorer_children(node_id: str = Query("connection", max_length=2048)) -> dict:
    client = _cdf_client()
    try:
        nodes = explorer_tree.list_children(client, node_id)
    except Exception as e:
        raise _api_error(e) from e
    return {"nodes": nodes, "stars": explorer_config.get_starred_node_ids()}


@app.get("/api/cdf/data-modeling/data-model/graph")
def dm_data_model_graph(
    space: str = Query(..., min_length=1, max_length=512),
    external_id: str = Query(..., min_length=1, max_length=512),
    version: str = Query(..., min_length=1, max_length=64),
) -> dict:
    client = _cdf_client()
    try:
        return cdf_browse.dm_data_model_graph(
            client, space=space, external_id=external_id, version=version
        )
    except Exception as e:
        raise _api_error(e) from e


class SqlRunRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=500_000)
    limit: int = Field(100, ge=1, le=10_000)
    source_limit: Optional[int] = Field(default=None, ge=1, le=100_000)
    convert_to_string: bool = True
    timeout: Optional[int] = Field(None, ge=1, le=240)


class FileContentSqlRunRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=500_000)
    limit: int = Field(100, ge=1, le=10_000)
    format: Optional[str] = Field(default=None, pattern="^(parquet|csv|json)$")
    file_id: Optional[int] = Field(default=None, ge=1)
    file_external_id: Optional[str] = Field(default=None, min_length=1, max_length=512)
    convert_to_string: bool = True


@app.get("/api/cdf/transformations/detail")
def transformation_detail(
    id: int = Query(..., ge=1, description="Transformation numeric id"),
) -> dict:
    client = _cdf_client()
    try:
        return cdf_browse.get_transformation_detail(client, transformation_id=id)
    except Exception as e:
        raise _api_error(e) from e


@app.get("/api/cdf/functions/detail")
def function_detail(
    id: str = Query(..., min_length=1, max_length=256, description="Function id or external id"),
) -> dict:
    client = _cdf_client()
    try:
        return cdf_browse.get_function_detail(client, function_id=id)
    except Exception as e:
        raise _api_error(e) from e


@app.get("/api/cdf/workflows/graph")
def workflow_graph(
    external_id: str = Query(..., min_length=1, max_length=512),
    version: Optional[str] = Query(None, max_length=64),
) -> dict:
    client = _cdf_client()
    try:
        return cdf_browse.workflow_graph(
            client, workflow_external_id=external_id, version=version
        )
    except Exception as e:
        raise _api_error(e) from e


@app.post("/api/cdf/sql/run")
def sql_run(body: SqlRunRequest) -> dict:
    """Preview SQL using CDF transformations query/run (20230101-beta)."""
    client = _cdf_client()
    try:
        return cdf_browse.run_sql_preview(
            client,
            query=body.query,
            limit=body.limit,
            source_limit=body.source_limit,
            convert_to_string=body.convert_to_string,
            timeout=body.timeout,
        )
    except Exception as e:
        raise _api_error(e) from e


@app.post("/api/cdf/file-content/sql/run")
def file_content_sql_run(body: FileContentSqlRunRequest) -> dict:
    """Run SELECT-only SQL against a downloaded CDF File (parquet, CSV, JSON) via DuckDB."""
    if body.file_id is None and not (body.file_external_id or "").strip():
        raise HTTPException(status_code=400, detail="file_id or file_external_id is required")
    client = _cdf_client()
    try:
        return file_content_query.run_file_content_sql(
            client,
            query=body.query,
            limit=body.limit,
            file_id=body.file_id,
            file_external_id=(body.file_external_id or "").strip() or None,
            fmt=body.format,  # type: ignore[arg-type]
            convert_to_string=body.convert_to_string,
        )
    except Exception as e:
        raise _api_error(e) from e
