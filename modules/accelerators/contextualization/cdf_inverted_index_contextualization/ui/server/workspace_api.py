"""Persist operator workspace tabs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ui.server.paths import WORKSPACE_PATH

router = APIRouter(prefix="/api/inverted-index", tags=["workspace"])

_WORKSPACE_FILE = WORKSPACE_PATH
_MAX_TABS = 50


class WorkspaceBody(BaseModel):
    active_tab_id: str | None = None
    tabs: list[dict[str, Any]] = Field(default_factory=list, max_length=_MAX_TABS)


def _read_workspace() -> dict:
    if not _WORKSPACE_FILE.is_file():
        return {"active_tab_id": None, "tabs": []}
    try:
        data = json.loads(_WORKSPACE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise HTTPException(status_code=500, detail=f"Failed to read workspace: {e}") from e
    if not isinstance(data, dict):
        return {"active_tab_id": None, "tabs": []}
    tabs = data.get("tabs")
    if not isinstance(tabs, list):
        tabs = []
    active = data.get("active_tab_id")
    if active is not None and not isinstance(active, str):
        active = None
    return {"active_tab_id": active, "tabs": tabs[:_MAX_TABS]}


def _write_workspace(workspace: dict) -> dict:
    tabs = workspace.get("tabs")
    if not isinstance(tabs, list):
        tabs = []
    normalized = {
        "active_tab_id": workspace.get("active_tab_id"),
        "tabs": tabs[:_MAX_TABS],
    }
    try:
        _WORKSPACE_FILE.write_text(json.dumps(normalized, indent=2), encoding="utf-8")
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Failed to write workspace: {e}") from e
    return normalized


@router.get("/workspace")
def get_workspace() -> dict:
    return {"workspace": _read_workspace()}


@router.put("/workspace")
def put_workspace(body: WorkspaceBody) -> dict:
    workspace = _write_workspace(body.model_dump())
    return {"workspace": workspace}
