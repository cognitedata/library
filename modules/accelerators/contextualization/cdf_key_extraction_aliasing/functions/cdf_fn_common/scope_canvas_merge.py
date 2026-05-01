"""Unified scope / canvas YAML layout: read ``canvas`` from YAML or hoist root graph into ``canvas``.

* :func:`canvas_dict_from_layout_yaml` — read-only extract (embedded ``canvas`` or root ``nodes``).
* :func:`normalize_root_graph_into_canvas` — mutate a full scope document so the graph lives under ``canvas``.
"""

from __future__ import annotations

import copy
from typing import Any, Dict

# Keys promoted from document root into ``canvas`` when authoring uses a flat graph.
_ROOT_GRAPH_KEYS = ("nodes", "edges", "handle_orientation", "viewport", "schemaVersion")


def normalize_root_graph_into_canvas(doc: Dict[str, Any]) -> None:
    """Mutate *doc* in place: if graph lives at root (``nodes``) and ``canvas`` is empty, hoist into ``canvas``."""
    c = doc.get("canvas")
    if isinstance(c, dict) and isinstance(c.get("nodes"), list) and c.get("nodes"):
        return
    nodes = doc.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        return
    canvas: Dict[str, Any] = {}
    for k in _ROOT_GRAPH_KEYS:
        if k not in doc:
            continue
        canvas[k] = copy.deepcopy(doc[k])
    for k in _ROOT_GRAPH_KEYS:
        doc.pop(k, None)
    doc["canvas"] = canvas


def canvas_dict_from_layout_yaml(raw: Dict[str, Any]) -> Dict[str, Any] | None:
    """Return a deep-copied canvas document from parsed YAML, or None if it has no graph."""
    nested = raw.get("canvas")
    if isinstance(nested, dict) and isinstance(nested.get("nodes"), list) and nested.get("nodes"):
        return copy.deepcopy(nested)
    nodes = raw.get("nodes")
    if isinstance(nodes, list) and nodes:
        return copy.deepcopy({k: v for k, v in raw.items() if k != "canvas"})
    return None
