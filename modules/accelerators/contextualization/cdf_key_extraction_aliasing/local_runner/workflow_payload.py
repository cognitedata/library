"""Build workflow-aligned task payloads for local incremental (workflow-parity) runs."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


def merged_scope_document_for_local_run(
    scope_yaml_path: Path,
    source_views: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Load v1 scope YAML and set ``key_extraction.config.data.source_views`` to ``source_views``.

    Deployed workflows pass ``workflow.input.scope_document`` with the leaf-filtered view list.
    The local runner filters views (e.g. ``--instance-space``) before calling this so task dicts
    match CDF function inputs.
    """
    with scope_yaml_path.open(encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        raise ValueError(f"Scope YAML must be a mapping: {scope_yaml_path}")
    out = copy.deepcopy(doc)
    ke = out.get("key_extraction")
    if not isinstance(ke, dict):
        raise ValueError("Scope YAML requires key_extraction mapping")
    cfg = ke.get("config")
    if not isinstance(cfg, dict):
        raise ValueError("key_extraction.config must be a mapping")
    data = cfg.get("data")
    if not isinstance(data, dict):
        cfg["data"] = {}
        data = cfg["data"]
    data["source_views"] = copy.deepcopy(source_views)
    return out


def workflow_instance_space_for_local(
    source_views: List[Dict[str, Any]],
    cli_instance_space: Optional[str],
) -> str:
    """Single ``instance_space`` string for local task payloads (optional override for functions).

    When ``--instance-space`` is set, the CLI already filtered ``source_views``; use that value.
    Otherwise prefer ``instance_space`` on the first view, then a single-value node ``space`` filter
    (same derivation as ``ensure_instance_space_from_scope_document`` on CDF).
    """
    if cli_instance_space and str(cli_instance_space).strip():
        return str(cli_instance_space).strip()
    for v in source_views:
        ins = v.get("instance_space")
        if ins is not None and str(ins).strip():
            return str(ins).strip()
    for v in source_views:
        for f in v.get("filters") or []:
            if str(f.get("property_scope", "view")).lower() != "node":
                continue
            if f.get("target_property") != "space":
                continue
            op = str(f.get("operator", "")).upper()
            vals = f.get("values")
            if op == "EQUALS":
                if isinstance(vals, list) and len(vals) == 1 and vals[0] is not None:
                    return str(vals[0]).strip()
                if vals is not None and not isinstance(vals, list):
                    return str(vals).strip()
            elif op == "IN" and isinstance(vals, list) and len(vals) == 1:
                if vals[0] is not None:
                    return str(vals[0]).strip()
    return "all_spaces"
