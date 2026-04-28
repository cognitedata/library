"""Merge sibling ``*.canvas.yaml`` into a v1 scope document (local runs, UI, ``build_scopes``)."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict

import yaml


def canvas_sibling_path(scope_yaml_path: Path) -> Path:
    """Sibling layout file for a v1 scope path (same rules as ``ui/server/main._canvas_document_path``).

    Examples: ``workflow.local.config.yaml`` → ``workflow.local.canvas.yaml``;
    ``foo.yaml`` → ``foo.canvas.yaml``.
    """
    name = scope_yaml_path.name
    parent = scope_yaml_path.parent
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


def _canvas_from_sibling_yaml_dict(raw: Dict[str, Any]) -> Dict[str, Any] | None:
    """Return a deep-copied canvas document from a parsed sibling YAML, or None if it has no graph."""
    nested = raw.get("canvas")
    if isinstance(nested, dict) and isinstance(nested.get("nodes"), list) and nested.get("nodes"):
        return copy.deepcopy(nested)
    nodes = raw.get("nodes")
    if isinstance(nodes, list) and nodes:
        # Root layout export: copy the whole mapping so handle_orientation and future keys survive.
        return copy.deepcopy({k: v for k, v in raw.items() if k != "canvas"})
    return None


def merge_sibling_canvas_yaml_into_scope(doc: Dict[str, Any], scope_yaml_path: Path) -> None:
    """Mutate *doc* in place: overlay ``canvas`` from sibling ``*.canvas.yaml`` when that file defines nodes.

    When a sibling layout file exists with a non-empty graph, it wins over any inline ``canvas`` in the
    scope YAML (same source as the flow UI split save). That keeps nested subgraph ``inner_canvas`` and
    all editor-only node data in sync for ``build_scopes`` / local runs.

    If the sibling is missing or has no nodes, an existing non-empty ``doc['canvas']`` is kept (inline-only
    scope documents without a layout file).
    """
    path = canvas_sibling_path(scope_yaml_path)
    if path.is_file():
        with path.open(encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        if isinstance(raw, dict):
            merged = _canvas_from_sibling_yaml_dict(raw)
            if merged is not None:
                doc["canvas"] = merged
                return
    c = doc.get("canvas")
    if isinstance(c, dict) and isinstance(c.get("nodes"), list) and c.get("nodes"):
        return
