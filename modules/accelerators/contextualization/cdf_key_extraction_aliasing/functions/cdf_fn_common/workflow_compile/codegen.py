"""Build Cognite ``WorkflowVersion``-shaped YAML documents from compiled_workflow IR."""

from __future__ import annotations

import copy
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


def _function_task(
    *,
    task_external_id: str,
    function_external_id: str,
    depends_on: List[str],
    description: str,
    name: str,
    timeout: int,
    extra_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "logLevel": "INFO",
        "run_all": "${workflow.input.run_all}",
        "configuration": "${workflow.input.configuration}",
        "compiled_workflow": "${workflow.input.compiled_workflow}",
        "task_id": task_external_id,
    }
    if extra_data:
        for k, v in extra_data.items():
            if v is not None:
                data[k] = v
    task: Dict[str, Any] = {
        "externalId": task_external_id,
        "type": "function",
        "parameters": {
            "function": {
                "externalId": function_external_id,
                "data": data,
            }
        },
        "name": name,
        "description": description,
        "retries": 3,
        "timeout": timeout,
        "onFailure": "abortWorkflow",
    }
    if depends_on:
        task["dependsOn"] = [{"externalId": ext} for ext in depends_on]
    return task


DEFAULT_WORKFLOW_VERSION_TEMPLATE_REL = Path("workflow_template") / "workflow.template.WorkflowVersion.yaml"


def load_function_display_meta_from_workflow_version_template(
    path: Path,
) -> tuple[dict[str, tuple[str, str, int]], str | None]:
    """
    Parse ``workflow.template.WorkflowVersion.yaml``-shaped YAML: map Cognite function external id
    (e.g. ``fn_dm_key_extraction``) to (name, description, timeout) and return optional top-level
    ``workflowDefinition.description`` so scoped :class:`WorkflowVersion` text matches the template.
    """
    if not path.is_file():
        return {}, None
    try:
        raw = path.read_text(encoding="utf-8")
        data = yaml.safe_load(raw)
    except (OSError, yaml.YAMLError) as e:
        logger.warning("Could not read workflow version template %s: %s", path, e)
        return {}, None
    if not isinstance(data, dict):
        return {}, None
    wd = data.get("workflowDefinition")
    if not isinstance(wd, dict):
        return {}, None
    wfd = wd.get("description")
    top_desc: str | None = None
    if wfd is not None:
        top_desc = str(wfd).strip() or None
    out: dict[str, tuple[str, str, int]] = {}
    tasks = wd.get("tasks")
    if not isinstance(tasks, list):
        return out, top_desc
    for t in tasks:
        if not isinstance(t, dict):
            continue
        params = t.get("parameters")
        if not isinstance(params, dict):
            continue
        fnb = params.get("function")
        if not isinstance(fnb, dict):
            continue
        fext = str(fnb.get("externalId") or "").strip()
        if not fext:
            continue
        name = str(t.get("name") or "").strip() or "Workflow step"
        dsc = t.get("description")
        desc = str(dsc).strip() if dsc is not None else f"Execute {fext}"
        to = t.get("timeout")
        tmo = int(to) if isinstance(to, int) and to > 0 else 7200
        out[fext] = (name, desc, tmo)
    return out, top_desc


def build_workflow_version_document(
    *,
    workflow_external_id: str,
    version: str,
    compiled_workflow: Dict[str, Any],
    description: str | None = None,
    module_root: Path | str | None = None,
    workflow_version_template_path: Path | str | None = None,
) -> Dict[str, Any]:
    """
    Return a mapping suitable for ``yaml.safe_dump`` as ``*WorkflowVersion.yaml``.

    ``compiled_workflow`` is used only to list tasks (ids, function_external_id, depends_on);
    runtime still receives the trigger-supplied ``workflow.input.compiled_workflow``.

    When ``module_root`` is set, per-function ``name`` / ``description`` / ``timeout`` and the top-level
    ``workflowDefinition.description`` are taken from ``workflow_version_template_path`` (default
    ``<module_root>/workflow_template/workflow.template.WorkflowVersion.yaml``) so a fresh scoped
    manifest matches the workflow template’s wording for each function type.
    """
    tasks_ir = compiled_workflow.get("tasks") if isinstance(compiled_workflow, dict) else None
    if not isinstance(tasks_ir, list) or not tasks_ir:
        tasks_ir = []

    tpl_by_fn: dict[str, tuple[str, str, int]] = {}
    top_from_tpl: str | None = None
    if module_root is not None:
        root = Path(module_root)
        wv_tmpl: Path
        if workflow_version_template_path is not None:
            wv_tmpl = Path(workflow_version_template_path)
        else:
            wv_tmpl = root / DEFAULT_WORKFLOW_VERSION_TEMPLATE_REL
        tpl_by_fn, top_from_tpl = load_function_display_meta_from_workflow_version_template(wv_tmpl)

    desc = (
        description
        or top_from_tpl
        or (
            "Canvas-driven key extraction and aliasing (node-per-task). "
            "Each task passes task_id + compiled_workflow in function data."
        )
    )

    wf_input: Dict[str, Any] = {
        "run_all": False,
        "run_id": "",
        "configuration": {},
        "compiled_workflow": {},
    }

    tasks_out: List[Dict[str, Any]] = []
    meta = {
        "fn_dm_incremental_state_update": (
            "Incremental state (cohort)",
            "Detect changed CDM instances per source_views and write cohort rows to RAW.",
            7200,
        ),
        "fn_dm_key_extraction": (
            "Key extraction",
            "Extract candidate keys and FK/document references; write key-extraction RAW.",
            7200,
        ),
        "fn_dm_reference_index": (
            "Reference index",
            "Build RAW inverted index for FK and document references.",
            7200,
        ),
        "fn_dm_aliasing": (
            "Aliasing",
            "Apply aliasing rules to extracted keys; write tag-aliasing RAW.",
            7200,
        ),
        "fn_dm_alias_persistence": (
            "Alias persistence",
            "Read aliasing RAW and write aliases onto source instances.",
            3600,
        ),
    }

    for t in tasks_ir:
        if not isinstance(t, dict):
            continue
        tid = str(t.get("id") or "").strip()
        fn = str(t.get("function_external_id") or "").strip()
        if not tid or not fn:
            continue
        deps_raw = t.get("depends_on")
        deps = [str(x) for x in deps_raw] if isinstance(deps_raw, list) else []
        if fn in tpl_by_fn:
            nm, dsc, tmo = tpl_by_fn[fn]
        else:
            nm, dsc, tmo = meta.get(
                fn,
                ("Workflow step", f"Execute {fn}", 7200),
            )
        tasks_out.append(
            _function_task(
                task_external_id=tid,
                function_external_id=fn,
                depends_on=deps,
                description=dsc,
                name=nm,
                timeout=tmo,
                extra_data=None,
            )
        )

    return {
        "workflowExternalId": workflow_external_id,
        "version": version,
        "workflowDefinition": {
            "description": desc,
            "input": wf_input,
            "tasks": tasks_out,
        },
    }


def strip_template_placeholders_for_compare(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Deep copy and normalize dynamic refs for tests (optional)."""
    return copy.deepcopy(doc)
