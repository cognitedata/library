"""Build Cognite ``WorkflowVersion``-shaped YAML documents from compiled_workflow IR."""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional


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


def build_workflow_version_document(
    *,
    workflow_external_id: str,
    version: str,
    compiled_workflow: Dict[str, Any],
    description: str | None = None,
) -> Dict[str, Any]:
    """
    Return a mapping suitable for ``yaml.safe_dump`` as ``*WorkflowVersion.yaml``.

    ``compiled_workflow`` is used only to list tasks (ids, function_external_id, depends_on);
    runtime still receives the trigger-supplied ``workflow.input.compiled_workflow``.
    """
    tasks_ir = compiled_workflow.get("tasks") if isinstance(compiled_workflow, dict) else None
    if not isinstance(tasks_ir, list) or not tasks_ir:
        tasks_ir = []

    desc = description or (
        "Canvas-driven key extraction and aliasing (node-per-task). "
        "Each task passes task_id + compiled_workflow in function data."
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
