"""Shared context helpers for local ETL pipeline runs."""

from __future__ import annotations

import uuid
from typing import Any, Dict, Mapping, MutableMapping, Set


def disabled_canvas_task_ids(configuration: Mapping[str, Any]) -> Set[str]:
    """Canvas node ids explicitly disabled (``enabled: false`` on the node)."""
    canvas = configuration.get("canvas")
    if not isinstance(canvas, dict):
        return set()
    out: Set[str] = set()
    for node in canvas.get("nodes") or []:
        if not isinstance(node, dict):
            continue
        if node.get("enabled") is False:
            node_id = str(node.get("id") or "").strip()
            if node_id:
                out.add(node_id)
    return out


def ensure_shared_run_id(shared_data: MutableMapping[str, Any]) -> str:
    from cdf_fn_common.etl_common import require_pipeline_run_key

    configuration = shared_data.get("configuration")
    cfg = dict(configuration) if isinstance(configuration, dict) else {}
    params = cfg.get("parameters")
    params_dict = dict(params) if isinstance(params, dict) else {}
    run_id = str(params_dict.get("run_id") or "").strip() or str(uuid.uuid4())
    params_dict["run_id"] = run_id
    cfg["parameters"] = params_dict
    shared_data["configuration"] = cfg
    rid = require_pipeline_run_key(shared_data)
    shared_data["run_id"] = rid
    shared_data["id"] = rid
    return rid


def pipeline_parameters(configuration: Mapping[str, Any]) -> Dict[str, Any]:
    params = configuration.get("parameters")
    return dict(params) if isinstance(params, dict) else {}


def merge_pipeline_runtime(shared_data: MutableMapping[str, Any], configuration: Mapping[str, Any]) -> None:
    """Seed handler ``data`` with pipeline-level fields used by ETL functions."""
    params = pipeline_parameters(configuration)
    if params and "parameters" not in shared_data:
        shared_data["parameters"] = params
    inst_space = params.get("instance_space")
    if inst_space and not shared_data.get("instance_space"):
        shared_data["instance_space"] = inst_space


def apply_incremental_run_scope(
    document: MutableMapping[str, Any], *, incremental_change_processing: bool
) -> None:
    """
    Align pipeline parameters with the selected run scope.

    Incremental runs enable watermark/hash processing; full-scope runs disable it.
    """
    params = document.get("parameters")
    merged = dict(params) if isinstance(params, dict) else {}
    merged["incremental_change_processing"] = incremental_change_processing
    if not incremental_change_processing:
        merged["incremental"] = False
    elif "incremental_skip_unchanged" not in merged:
        merged["incremental_skip_unchanged"] = True
    document["parameters"] = merged
