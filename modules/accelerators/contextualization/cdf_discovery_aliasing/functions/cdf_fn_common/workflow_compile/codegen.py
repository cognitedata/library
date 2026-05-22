"""Build Cognite ``WorkflowVersion``-shaped YAML documents from compiled_workflow IR."""

from __future__ import annotations

import copy
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import yaml

logger = logging.getLogger(__name__)

# Whole-string workflow input references must stay single-$ (CDF expression syntax).
_WORKFLOW_INPUT_REF_RE = re.compile(r"^\$\{workflow\.input\.[^}]+\}$")


def escape_cdf_workflow_dollar_literals(value: str) -> str:
    """
    Escape literal ``$`` in Cognite Workflow task ``data`` strings for deploy.

    CDF treats ``$`` as expression syntax; regex anchors and similar need ``$$``.
    Preserves ``${workflow.input.*}`` references and already-escaped ``$$`` pairs.
    """
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
    """Recursively escape string leaves for CDF workflow task payloads."""
    if isinstance(value, str):
        return escape_cdf_workflow_dollar_literals(value)
    if isinstance(value, list):
        return [escape_cdf_workflow_string_tree(v) for v in value]
    if isinstance(value, dict):
        return {k: escape_cdf_workflow_string_tree(v) for k, v in value.items()}
    return value


def escape_workflow_version_document_for_cdf(doc: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Deep-copy a WorkflowVersion-shaped document for CDF upsert.

    Strips ``workflowDefinition.input`` (API rejection) and escapes literal ``$`` in each
    function task's ``parameters.function.data`` strings.
    """
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
    """Static keys merged onto each Cognite function task ``data`` (replaces runtime ``compiled_workflow`` slice)."""
    out: Dict[str, Any] = {}
    pnode = ir_task.get("pipeline_node_id")
    if pnode is not None and str(pnode).strip():
        out["pipeline_node_id"] = str(pnode).strip()
    cn = ir_task.get("canvas_node_id")
    if cn is not None and str(cn).strip():
        out["canvas_node_id"] = str(cn).strip()
    pl = ir_task.get("payload")
    if isinstance(pl, dict):
        for pk, pv in pl.items():
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
) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "logLevel": "INFO",
        "run_all": "${workflow.input.run_all}",
        "configuration": "${workflow.input.configuration}",
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
    (e.g. ``fn_dm_view_query``) to (name, description, timeout) and return optional top-level
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

    The document omits ``workflowDefinition.input``: CDF and Cognite Toolkit workflow deploy reject
    that field; ``workflow.input`` (``run_all``, ``run_id``, ``configuration``) is supplied by
    WorkflowTrigger static ``input`` at run time. Task ``data`` uses ``${workflow.input.*}`` refs.

    ``compiled_workflow`` is used only to list tasks (ids, function_external_id, depends_on) and
    to inline per-task ``payload`` / ``persistence`` / ``pipeline_node_id`` onto each function task.
    Per-task ``name`` uses canvas ``data.label`` when the IR carries ``label``; otherwise the
    workflow version template or built-in function display names apply.

    When ``module_root`` is set, per-function ``description`` / ``timeout`` and the top-level
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
            "Discovery pipeline (canvas DAG). "
            "Each task carries task_id plus inlined payload from build-time IR."
        )
    )

    tasks_out: List[Dict[str, Any]] = []
    meta = {
        "fn_dm_view_save": (
            "View save",
            "Apply instances from discovery payloads to DM views.",
            7200,
        ),
        "fn_dm_raw_save": (
            "RAW save",
            "Upsert RAW rows from discovery payloads.",
            7200,
        ),
        "fn_dm_classic_save": (
            "Classic save",
            "Upsert classic resources from discovery payloads.",
            7200,
        ),
        "fn_dm_view_query": (
            "View query",
            "DM view query — state update and cohort-scoped list; sink RAW.",
            7200,
        ),
        "fn_dm_raw_query": (
            "RAW query",
            "RAW query — state and cohort reads; sink RAW.",
            7200,
        ),
        "fn_dm_classic_query": (
            "Classic query",
            "Classic resource query — state and cohort reads; sink RAW.",
            7200,
        ),
        "fn_dm_sql_query": (
            "SQL query",
            "CDF SQL preview (transformations query/run) — sink RAW cohort rows.",
            7200,
        ),
        "fn_dm_transform": (
            "Transform",
            "Transform predecessor payloads; sink RAW.",
            7200,
        ),
        "fn_dm_join": (
            "Join",
            "Join two predecessor cohort RAW streams; sink RAW.",
            7200,
        ),
        "fn_dm_validate": (
            "Validate",
            "Validate predecessor payloads; sink RAW.",
            7200,
        ),
        "fn_dm_filter": (
            "Instance filter",
            "Exclude cohort rows that fail configured instance filters (query-style DSL); sink RAW.",
            7200,
        ),
        "fn_dm_confidence_filter": (
            "Confidence filter",
            "Prune aligned value lists by {field}_confidence threshold; sink RAW.",
            7200,
        ),
        "fn_dm_inverted_index": (
            "Inverted index",
            "Build inverted RAW index from discovery predecessor payloads (FK/doc refs); optional legacy RAW source.",
            7200,
        ),
        "fn_dm_discovery_raw_cleanup": (
            "RAW cleanup",
            "After pipeline sinks: delete cohort keys for this run_id or truncate discovery RAW tables (configured).",
            7200,
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
        node_label = str(t.get("label") or "").strip()
        if fn in tpl_by_fn:
            nm, dsc, tmo = tpl_by_fn[fn]
        else:
            nm, dsc, tmo = meta.get(
                fn,
                ("Workflow step", f"Execute {fn}", 7200),
            )
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

    doc = {
        "workflowExternalId": workflow_external_id,
        "version": version,
        "workflowDefinition": {
            "description": desc,
            "tasks": tasks_out,
        },
    }
    return escape_workflow_version_document_for_cdf(doc)


def strip_template_placeholders_for_compare(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Deep copy and normalize dynamic refs for tests (optional)."""
    return copy.deepcopy(doc)
