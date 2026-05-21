"""Build workflow-aligned task payloads for local incremental (workflow-parity) runs."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import yaml

from cdf_fn_common.scope_document_dm import resolve_instance_space_from_canvas_configuration
from cdf_fn_common.workflow_compile.canvas_dag import compiled_workflow_for_scope_document
from cdf_fn_common.scope_canvas_merge import normalize_root_graph_into_canvas


def merged_scope_document_for_local_run(
    scope_yaml_path: Path,
    source_views: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Load v1 scope YAML and set top-level ``source_views`` to the filtered list.

    Deployed workflows pass ``workflow.input.configuration`` with the leaf-filtered view list
    at the document root. The local runner filters views (e.g. ``--instance-space``) before
    calling this so task dicts match CDF function inputs.
    """
    with scope_yaml_path.open(encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        raise ValueError(f"Scope YAML must be a mapping: {scope_yaml_path}")
    out = copy.deepcopy(doc)
    normalize_root_graph_into_canvas(out)
    out.pop("associations", None)
    out["source_views"] = copy.deepcopy(source_views)
    return out


def compiled_workflow_for_merged_scope_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Return ``compiled_workflow`` IR for a merged v1 scope dict (local / trigger parity)."""
    return compiled_workflow_for_scope_document(doc)


def _embedded_compiled_workflow_tasks(doc: Mapping[str, Any]) -> Optional[List[Any]]:
    """Return tasks list when *doc* carries a usable pre-built ``compiled_workflow`` (e.g. from trigger)."""
    cw = doc.get("compiled_workflow")
    if not isinstance(cw, dict):
        return None
    tasks = cw.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        return None
    for t in tasks:
        if not isinstance(t, dict) or not t.get("id"):
            return None
        if not str(t.get("function_external_id") or "").strip():
            return None
    return tasks


def scope_document_has_embedded_compiled_workflow(doc: Mapping[str, Any]) -> bool:
    """True when *doc* includes a root ``compiled_workflow`` with executable tasks (trigger snapshot)."""
    return _embedded_compiled_workflow_tasks(doc) is not None


def compiled_workflow_for_local_run(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Resolve ``compiled_workflow`` for ``module.py run`` / Kahn executor.

    Prefer a non-empty root ``compiled_workflow`` on the scope document (WorkflowTrigger snapshot
    or hand-edited parity) so local execution matches deployed ``workflow.input``; otherwise compile
    from the flow canvas like CDF build.
    """
    if _embedded_compiled_workflow_tasks(doc) is not None:
        return copy.deepcopy(doc["compiled_workflow"])
    return compiled_workflow_for_scope_document(doc)


def workflow_instance_space_for_local(
    source_views: List[Dict[str, Any]],
    cli_instance_space: Optional[str],
    scope_document: Optional[Mapping[str, Any]] = None,
) -> str:
    """Single ``instance_space`` string for local task payloads (optional override for functions).

    When ``--instance-space`` is set, the CLI already filtered ``source_views``; use that value.
    Otherwise prefer ``instance_space`` on the first view, then a single-value node ``space`` filter
    (same derivation as ``ensure_instance_space_from_scope_document`` on CDF).

    When nothing resolves to a concrete space, returns an empty string (not a CDF space name).
    View-query handlers map that to ``instances.list(space=None)`` so listing is unrestricted across spaces.
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
    if scope_document is not None:
        from_canvas = resolve_instance_space_from_canvas_configuration(scope_document)
        if from_canvas:
            return from_canvas
    return ""
