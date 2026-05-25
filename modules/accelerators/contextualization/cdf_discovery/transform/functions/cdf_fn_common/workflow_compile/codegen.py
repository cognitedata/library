"""Workflow YAML codegen helpers for ETL workflows."""

from __future__ import annotations

import copy
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import yaml

logger = logging.getLogger(__name__)

_WORKFLOW_INPUT_REF_RE = re.compile(r"^\$\{workflow\.input\.[^}]+\}$")
DEFAULT_WORKFLOW_VERSION_TEMPLATE_REL = Path("workflow_template") / "workflow.template.WorkflowVersion.yaml"


def ir_task_inline_function_data(ir_task: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in ("pipeline_node_id", "canvas_node_id"):
        val = ir_task.get(key)
        if val is not None and str(val).strip():
            out[key] = str(val).strip()
    payload = ir_task.get("payload")
    if isinstance(payload, dict):
        for pk, pv in payload.items():
            out[pk] = copy.deepcopy(pv)
    pers = ir_task.get("persistence")
    if isinstance(pers, dict):
        for pk, pv in pers.items():
            out[pk] = copy.deepcopy(pv)
    return out


def _function_task(
    *,
    task_external_id: str,
    function_external_id: str,
    depends_on: List[str],
    description: str,
    name: str,
    timeout: int,
    extra_data: Optional[Dict[str, Any]] = None,
    retries: Optional[int] = None,
    on_failure: Optional[str] = None,
) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "logLevel": "INFO",
        "incremental_change_processing": "${workflow.input.incremental_change_processing}",
        "run_id": "${workflow.input.run_id}",
        "configuration": "${workflow.input.configuration}",
        "task_id": task_external_id,
    }
    if extra_data:
        for k, v in extra_data.items():
            if v is not None:
                data[k] = v
    from cdf_fn_common.workflow_task_policy import discovery_task_workflow_policy

    policy = discovery_task_workflow_policy(function_external_id)
    task_retries = int(retries) if retries is not None else int(policy["retries"])
    task_on_failure = (
        str(on_failure).strip() if on_failure is not None else str(policy["onFailure"])
    )
    task: Dict[str, Any] = {
        "externalId": task_external_id,
        "type": "function",
        "dependsOn": [{"externalId": ext} for ext in depends_on] if depends_on else [],
        "parameters": {
            "function": {
                "externalId": function_external_id,
                "data": data,
            }
        },
        "name": name,
        "description": description,
        "retries": task_retries,
        "timeout": timeout,
        "onFailure": task_on_failure,
    }
    return task


def load_function_display_meta_from_workflow_version_template(
    path: Path,
) -> tuple[dict[str, tuple[str, str, int]], str | None]:
    if not path.is_file():
        return {}, None
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as e:
        logger.warning("Could not read workflow version template %s: %s", path, e)
        return {}, None
    if not isinstance(data, dict):
        return {}, None
    wd = data.get("workflowDefinition")
    if not isinstance(wd, dict):
        return {}, None
    wfd = wd.get("description")
    top_desc = str(wfd).strip() or None if wfd is not None else None
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
    tasks_ir = compiled_workflow.get("tasks") if isinstance(compiled_workflow, dict) else None
    if not isinstance(tasks_ir, list):
        tasks_ir = []

    tpl_by_fn: dict[str, tuple[str, str, int]] = {}
    top_from_tpl: str | None = None
    if module_root is not None:
        root = Path(module_root)
        wv_tmpl = (
            Path(workflow_version_template_path)
            if workflow_version_template_path is not None
            else root / DEFAULT_WORKFLOW_VERSION_TEMPLATE_REL
        )
        tpl_by_fn, top_from_tpl = load_function_display_meta_from_workflow_version_template(wv_tmpl)

    desc = description or top_from_tpl or "CDF Discovery ETL workflow (canvas DAG)."
    tasks_out: List[Dict[str, Any]] = []
    for t in tasks_ir:
        if not isinstance(t, dict):
            continue
        tid = str(t.get("id") or "").strip()
        fn = str(t.get("function_external_id") or "").strip()
        if not tid:
            continue
        task_type = str(t.get("task_type") or "function").strip()
        deps_raw = t.get("depends_on")
        deps = [str(x) for x in deps_raw] if isinstance(deps_raw, list) else []

        if task_type == "function" and fn:
            if fn in tpl_by_fn:
                nm, dsc, tmo = tpl_by_fn[fn]
            else:
                nm, dsc, tmo = ("Workflow step", f"Execute {fn}", 7200)
            node_label = str(t.get("label") or "").strip()
            if node_label:
                nm = node_label
            tasks_out.append(
                _function_task(
                    task_external_id=tid,
                    function_external_id=fn,
                    depends_on=deps,
                    description=dsc,
                    name=nm,
                    timeout=tmo,
                    extra_data=ir_task_inline_function_data(t),
                )
            )
        else:
            tasks_out.append(
                {
                    "externalId": tid,
                    "type": task_type,
                    "dependsOn": [{"externalId": d} for d in deps if d],
                    "parameters": _task_parameters(t),
                    "retries": 3,
                    "timeout": 7200,
                    "onFailure": "abortWorkflow",
                }
            )

    return {
        "externalId": f"{workflow_external_id}:{version}",
        "workflowExternalId": workflow_external_id,
        "version": version,
        "description": desc,
        "workflowDefinition": {"tasks": tasks_out},
    }


def _task_parameters(ir_task: Mapping[str, Any]) -> Dict[str, Any]:
    task_type = str(ir_task.get("task_type") or "function").strip()
    cfg = {}
    payload = ir_task.get("payload")
    if isinstance(payload, dict):
        cfg = payload.get("config") if isinstance(payload.get("config"), dict) else {}

    if task_type == "transformation":
        ext = str(cfg.get("transformation_external_id") or ir_task.get("id") or "").strip()
        return {
            "transformation": {
                "externalId": ext or f"tr_etl_{ir_task.get('id')}",
                "concurrencyPolicy": "fail",
            }
        }
    if task_type == "subworkflow":
        return {
            "subworkflow": {
                "externalId": str(cfg.get("workflow_external_id") or ""),
                "version": str(cfg.get("workflow_version") or "1"),
            }
        }
    if task_type == "dynamic":
        gen = str(cfg.get("generator_task_id") or "").strip()
        return {"dynamic": {"tasks": f"${{{gen}.output.tasks}}" if gen else []}}
    if task_type == "simulation":
        return {"simulation": {"externalId": str(cfg.get("simulation_external_id") or "")}}
    if task_type == "cdf":
        cdf_params = cfg.get("cdf")
        return {"cdf": cdf_params if isinstance(cdf_params, dict) else {}}

    fn_ext = str(ir_task.get("function_external_id") or "").strip()
    task_id = str(ir_task.get("id") or "").strip()
    extra = ir_task_inline_function_data(ir_task)
    return {
        "function": {
            "externalId": fn_ext,
            "data": {
                "task_id": task_id,
                "incremental_change_processing": "${workflow.input.incremental_change_processing}",
                "run_id": "${workflow.input.run_id}",
                "configuration": "${workflow.input.configuration}",
                **extra,
            },
        }
    }


def build_workflow_version_from_ir(
    *,
    workflow_external_id: str,
    version: str,
    compiled: Mapping[str, Any],
    description: str = "",
) -> Dict[str, Any]:
    """Build a WorkflowVersion-shaped document from compiled IR."""
    tasks_out: List[Dict[str, Any]] = []
    for t in compiled.get("tasks") or []:
        if not isinstance(t, dict):
            continue
        task_id = str(t.get("id") or "").strip()
        if not task_id:
            continue
        task_type = str(t.get("task_type") or "function").strip()
        depends = [{"externalId": str(d)} for d in (t.get("depends_on") or []) if str(d).strip()]
        tasks_out.append(
            {
                "externalId": task_id,
                "type": task_type,
                "dependsOn": depends,
                "parameters": _task_parameters(t),
                "retries": 3,
                "timeout": 7200,
                "onFailure": "abortWorkflow",
            }
        )
    return {
        "externalId": f"{workflow_external_id}:{version}",
        "workflowExternalId": workflow_external_id,
        "version": version,
        "description": description,
        "workflowDefinition": {"tasks": tasks_out},
    }


def emit_transformation_resources(compiled: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Return Transformation resource stubs for spark_transform tasks."""
    out: List[Dict[str, Any]] = []
    for t in compiled.get("tasks") or []:
        if not isinstance(t, dict):
            continue
        if str(t.get("executable_kind") or "") != "spark_transform":
            continue
        payload = t.get("payload") if isinstance(t.get("payload"), dict) else {}
        cfg = payload.get("config") if isinstance(payload.get("config"), dict) else {}
        ext = str(cfg.get("transformation_external_id") or f"tr_etl_{t.get('id')}").strip()
        out.append(
            {
                "externalId": ext,
                "name": str(cfg.get("description") or ext),
                "query": str(cfg.get("query") or ""),
                "destination": cfg.get("destination") or {},
            }
        )
    return out
