"""Load v1 scope YAML for discovery workflow local runs (canvas ``compiled_workflow``)."""

from __future__ import annotations

import copy
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from .paths import SCRIPT_DIR

DEFAULT_SCOPE = "default"

# v1 scope document at module root (local runs only; CDF uses trigger-embedded configuration).
WORKFLOW_LOCAL_CONFIG_FILENAME = "workflow.local.config.yaml"
DEFAULT_SCOPE_DOCUMENT_PATH = SCRIPT_DIR / WORKFLOW_LOCAL_CONFIG_FILENAME


def resolve_scope_document_path(scope: Optional[str] = None) -> Path:
    """Resolve the default v1 scope YAML at the module root.

    Only ``scope='default'`` (or omitted) is supported without ``--config-path``.
    Other scope names raise: use ``--config-path`` to point at a v1 scope file.
    """
    sc = (scope or DEFAULT_SCOPE).strip() or DEFAULT_SCOPE
    if sc != DEFAULT_SCOPE:
        raise FileNotFoundError(
            f"Per-scope directory layout was removed. For scope {sc!r} pass --config-path "
            f"to a v1 scope YAML file, or use scope {DEFAULT_SCOPE!r} to load "
            f"{DEFAULT_SCOPE_DOCUMENT_PATH.name} at the module root."
        )
    if not DEFAULT_SCOPE_DOCUMENT_PATH.is_file():
        raise FileNotFoundError(
            f"Missing default scope document {DEFAULT_SCOPE_DOCUMENT_PATH}. "
            f"Add {WORKFLOW_LOCAL_CONFIG_FILENAME} at the module root or pass --config-path."
        )
    return DEFAULT_SCOPE_DOCUMENT_PATH


def _load_scope_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        raise ValueError("Scope YAML root must be a mapping")
    return doc


def source_views_from_scope_document(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return a deep copy of root ``source_views`` when present; canvas-only scopes may omit it."""
    raw = doc.get("source_views")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError("source_views must be a list when present")
    out: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict):
            out.append(copy.deepcopy(item))
    return out


def load_discovery_scope(
    logger: logging.Logger,
    scope: Optional[str] = None,
    config_path: Optional[str] = None,
) -> Tuple[Path, List[Dict[str, Any]]]:
    """Load scope YAML and return ``(path, source_views)``.

    ``source_views`` may be empty when the scope is canvas-only (views are carried on
    individual canvas nodes / task payloads). Optional ``--instance-space`` filtering
    is applied in ``module.py`` before the runner merges the list into the scope document.
    """
    if config_path:
        p = Path(config_path).expanduser()
        if not p.is_absolute():
            p = Path.cwd() / p
        if not p.is_file():
            raise FileNotFoundError(f"Config file not found: {p}")
        logger.info("Loading scope from --config-path: %s", p)
        doc = _load_scope_yaml(p)
        return p.resolve(), source_views_from_scope_document(doc)

    sc = (scope or DEFAULT_SCOPE).strip() or DEFAULT_SCOPE
    p = resolve_scope_document_path(sc)
    logger.info("Loading scope %r from %s", sc, p)
    doc = _load_scope_yaml(p)
    return p.resolve(), source_views_from_scope_document(doc)
