"""Trim workflow definition documents for deploy payloads."""

from __future__ import annotations

import copy
from typing import Any, Dict, Mapping, MutableMapping


def _strip_positions_from_node(n: MutableMapping[str, Any]) -> None:
    n.pop("position", None)
    n.pop("selected", None)
    n.pop("dragging", None)


def _strip_positions_from_canvas(canvas: MutableMapping[str, Any]) -> None:
    nodes = canvas.get("nodes")
    if isinstance(nodes, list):
        for n in nodes:
            if isinstance(n, dict):
                _strip_positions_from_node(n)


def trim_workflow_document_for_deploy(workflow_document: Mapping[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(dict(workflow_document))
    canvas = out.get("canvas")
    if isinstance(canvas, dict):
        _strip_positions_from_canvas(canvas)
    return out


def extract_trigger_configuration(workflow_document: Mapping[str, Any]) -> Dict[str, Any]:
    doc = dict(workflow_document)
    params = doc.get("parameters") if isinstance(doc.get("parameters"), dict) else {}
    scope = doc.get("scope") if isinstance(doc.get("scope"), dict) else None
    return {
        "schemaVersion": int(doc.get("schemaVersion") or 1),
        "id": str(doc.get("id") or ""),
        "scope": scope,
        "parameters": dict(params),
    }


def build_trigger_input(
    workflow_document: Mapping[str, Any],
    *,
    default_cron: str = "0 2 * * *",
) -> Dict[str, Any]:
    from workflow_build.trigger_from_canvas import read_start_trigger_config

    canvas = workflow_document.get("canvas") if isinstance(workflow_document.get("canvas"), dict) else {}
    trigger_cfg = read_start_trigger_config(canvas, default_cron=default_cron)
    return {
        "incremental_change_processing": bool(
            trigger_cfg.get("incremental_change_processing", True)
        ),
        "run_id": str(trigger_cfg.get("run_id") or ""),
        "configuration": extract_trigger_configuration(workflow_document),
    }
