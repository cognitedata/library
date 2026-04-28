"""Build workflow-aligned task payloads for local incremental (workflow-parity) runs."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import yaml

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.workflow_associations import (
    KIND_SOURCE_VIEW_TO_EXTRACTION,
    coerce_association_source_view_index,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.scope_canvas_merge import (
    canvas_sibling_path,
    merge_sibling_canvas_yaml_into_scope,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.workflow_compile.canvas_dag import (
    compiled_workflow_for_scope_document,
)


def _view_identity(v: Dict[str, Any]) -> tuple:
    return (
        str(v.get("view_space") or "").strip(),
        str(v.get("view_external_id") or "").strip(),
        str(v.get("view_version") or "").strip(),
    )


def remap_associations_for_filtered_source_views(
    associations: List[Dict[str, Any]],
    original_source_views: List[Dict[str, Any]],
    filtered_source_views: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Rewrite ``source_view_index`` on association rows so they still refer to the same
    logical view after ``source_views`` is replaced (e.g. CLI filter / reorder).

    Rows whose view was dropped from the filtered list are omitted.
    """
    if not isinstance(associations, list) or not associations:
        return []
    if not isinstance(original_source_views, list) or not original_source_views:
        return copy.deepcopy(associations)
    if not isinstance(filtered_source_views, list):
        return copy.deepcopy(associations)

    orig_keys = [_view_identity(v) for v in original_source_views if isinstance(v, dict)]
    new_index = {
        _view_identity(v): i for i, v in enumerate(filtered_source_views) if isinstance(v, dict)
    }
    out: List[Dict[str, Any]] = []
    for row in associations:
        if not isinstance(row, dict):
            continue
        if str(row.get("kind") or "").strip() != KIND_SOURCE_VIEW_TO_EXTRACTION:
            out.append(copy.deepcopy(row))
            continue
        ii = coerce_association_source_view_index(row.get("source_view_index"))
        if ii is None:
            continue
        if ii < 0 or ii >= len(orig_keys):
            continue
        vk = orig_keys[ii]
        ni = new_index.get(vk)
        if ni is None:
            continue
        r2 = copy.deepcopy(row)
        r2["source_view_index"] = ni
        out.append(r2)
    return out


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
    if not isinstance(out.get("key_extraction"), dict):
        raise ValueError("Scope YAML requires key_extraction mapping")
    merge_sibling_canvas_yaml_into_scope(out, scope_yaml_path)
    original_svs = out.get("source_views")
    filtered_svs = copy.deepcopy(source_views)
    raw_assoc = out.get("associations")
    if (
        isinstance(raw_assoc, list)
        and raw_assoc
        and isinstance(original_svs, list)
        and original_svs
    ):
        out["associations"] = remap_associations_for_filtered_source_views(
            raw_assoc, original_svs, filtered_svs
        )
    out["source_views"] = filtered_svs
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
