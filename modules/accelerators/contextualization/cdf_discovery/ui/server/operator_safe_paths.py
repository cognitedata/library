"""Path guards for operator FastAPI (vendored from operator_kit template)."""

from __future__ import annotations

from pathlib import Path

from fastapi import HTTPException


def safe_rel_path(module_root: Path, rel: str) -> Path:
    if ".." in rel.split("/") or rel.startswith(("/", "\\")):
        raise HTTPException(status_code=400, detail="Invalid path")
    p = (module_root / rel).resolve()
    try:
        p.relative_to(module_root)
    except ValueError as e:
        raise HTTPException(status_code=400, detail="Path escapes module root") from e
    return p
