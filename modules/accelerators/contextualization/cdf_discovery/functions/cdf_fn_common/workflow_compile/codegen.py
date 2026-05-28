"""Workflow YAML codegen helpers for ETL workflows."""

from __future__ import annotations

import copy
import re
from typing import Any, Dict, List, Mapping, Optional

_WORKFLOW_INPUT_REF_RE = re.compile(r"^\$\{workflow\.input\.[^}]+\}$")


def escape_cdf_workflow_dollar_literals(value: str) -> str:
    if not value or "$" not in value:
        return value
    if _WORKFLOW_INPUT_REF_RE.match(value.strip()):
        return value
    out: List[str] = []
    i = 0
    n = len(value)
    while i < n:
        ch = value[i]
        if ch != "$":
            out.append(ch)
            i += 1
            continue
        if i + 1 < n and value[i + 1] == "{":
            out.append("$")
            i += 1
            continue
        if i + 1 < n and value[i + 1] == "$":
            out.append("$$")
            i += 2
            continue
        out.append("$$")
        i += 1
    return "".join(out)


def escape_cdf_workflow_string_tree(value: Any) -> Any:
    if isinstance(value, str):
        return escape_cdf_workflow_dollar_literals(value)
    if isinstance(value, list):
        return [escape_cdf_workflow_string_tree(v) for v in value]
    if isinstance(value, dict):
        return {k: escape_cdf_workflow_string_tree(v) for k, v in value.items()}
    return value


def escape_workflow_version_document_for_cdf(doc: Mapping[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(dict(doc))
    wd = out.get("workflowDefinition")
    if not isinstance(wd, dict):
        return out
    if "input" in wd:
        wd = {k: v for k, v in wd.items() if k != "input"}
        out["workflowDefinition"] = wd
    tasks = wd.get("tasks")
    if not isinstance(tasks, list):
        return out
    for task in tasks:
        if not isinstance(task, dict):
            continue
        params = task.get("parameters")
        if not isinstance(params, dict):
            continue
        fn = params.get("function")
        if not isinstance(fn, dict):
            continue
        data = fn.get("data")
        if isinstance(data, dict):
            fn["data"] = escape_cdf_workflow_string_tree(data)
    return out


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
    is_async_complete: Optional[bool] = None,
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
    fn_params: Dict[str, Any] = {
        "externalId": function_external_id,
        "data": data,
    }
    if is_async_complete is None:
        raw_async = policy.get("isAsyncComplete")
        if raw_async is not None:
            fn_params["isAsyncComplete"] = bool(raw_async)
    elif is_async_complete:
        fn_params["isAsyncComplete"] = True

    task_timeout = int(policy.get("timeout") or timeout)

    task: Dict[str, Any] = {
        "externalId": task_external_id,
        "type": "function",
        "dependsOn": [{"externalId": ext} for ext in depends_on] if depends_on else [],
        "parameters": {
            "function": fn_params,
        },
        "name": name,
        "description": description,
        "retries": task_retries,
        "timeout": task_timeout,
        "onFailure": task_on_failure,
    }
    return task


def _json_mapping_task(
    *,
    task_external_id: str,
    depends_on: List[str],
    name: str,
    description: str,
    input_obj: Mapping[str, Any],
    expression: str,
    retries: int = 2,
    timeout: int = 1800,
    on_failure: str = "abortWorkflow",
) -> Dict[str, Any]:
    return {
        "externalId": task_external_id,
        "type": "jsonMapping",
        "dependsOn": [{"externalId": d} for d in depends_on if d],
        "parameters": {
            "jsonMapping": {
                "input": dict(input_obj),
                "expression": expression,
            }
        },
        "name": name,
        "description": description,
        "retries": retries,
        "timeout": timeout,
        "onFailure": on_failure,
    }


def build_workflow_version_document(
    *,
    workflow_external_id: str,
    version: str,
    compiled_workflow: Dict[str, Any],
    description: str | None = None,
) -> Dict[str, Any]:
    tasks_ir = compiled_workflow.get("tasks") if isinstance(compiled_workflow, dict) else None
    if not isinstance(tasks_ir, list):
        tasks_ir = []

    desc = description or "CDF Discovery ETL workflow (canvas DAG)."
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
        elif task_type == "jsonMapping":
            payload = t.get("payload") if isinstance(t.get("payload"), dict) else {}
            cfg = payload.get("config") if isinstance(payload.get("config"), dict) else {}
            inp = cfg.get("input") if isinstance(cfg.get("input"), dict) else {}
            expr = str(cfg.get("expression") or "").strip()
            node_label = str(t.get("label") or "").strip()
            nm = node_label or "JSON mapping"
            dsc = str(cfg.get("description") or "").strip() or "CDF jsonMapping (Kuiper)"
            tasks_out.append(
                _json_mapping_task(
                    task_external_id=tid,
                    depends_on=deps,
                    name=nm,
                    description=dsc,
                    input_obj=inp,
                    expression=expr,
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
    if task_type == "jsonMapping":
        inp = cfg.get("input")
        expr = str(cfg.get("expression") or "").strip()
        return {
            "jsonMapping": {
                "input": inp if isinstance(inp, dict) else {},
                "expression": expr,
            }
        }

    fn_ext = str(ir_task.get("function_external_id") or cfg.get("function_external_id") or "").strip()
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
