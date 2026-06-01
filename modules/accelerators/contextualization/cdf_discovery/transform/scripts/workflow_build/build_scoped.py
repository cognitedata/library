"""Compile and emit artifacts for one scoped workflow."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import yaml

from workflow_build.context import BuildContext
from workflow_build.ids import (
    patch_start_node_workflow_pairing,
    resolve_workflow_base_for_build,
    workflow_external_id,
    workflow_trigger_external_id,
)
from workflow_build.scope import patch_definition_for_scope
from workflow_build.sources import WorkflowBuildSource, template_document_for_build
from workflow_build.targets import ScopedWorkflowTarget
from workflow_build.trigger_from_canvas import (
    apply_start_trigger_to_workflow_trigger,
    read_start_trigger_config,
)
from workflow_build.workflow_document_limits import (
    assert_minimal_task_config,
    assert_task_function_data_within_limit,
    assert_workflow_document_within_limit,
    assert_workflow_trigger_input_within_limit,
)
from workflow_build.paths import (
    artifact_filename,
    is_scoped_build,
    workflow_artifacts_scope_dir,
)
from workflow_build.workflow_document_trim import build_trigger_input, trim_workflow_document_for_deploy

logger = logging.getLogger(__name__)


def _validate_codegen_tasks(wv: Dict[str, Any]) -> None:
    for t in (wv.get("workflowDefinition") or {}).get("tasks") or []:
        if not isinstance(t, dict):
            continue
        if str(t.get("type") or "") != "function":
            continue
        fn_block = (t.get("parameters") or {}).get("function") or {}
        data = fn_block.get("data") if isinstance(fn_block, dict) else {}
        if not isinstance(data, dict):
            continue
        cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
        task_id = str(t.get("externalId") or "")
        assert_task_function_data_within_limit(data, task_id=task_id)
        assert_minimal_task_config(cfg, executor_kind="function")


def build_scoped_workflow(
    *,
    module_root: Path,
    config: dict,
    source: WorkflowBuildSource,
    target: ScopedWorkflowTarget,
    levels: List[str],
    dry_run: bool = False,
) -> List[Path]:
    from cdf_fn_common.workflow_compile.canvas_dag import compile_canvas_dag
    from cdf_fn_common.workflow_compile.codegen import (
        build_workflow_version_document,
        escape_workflow_version_document_for_cdf,
        emit_transformation_resources,
    )

    doc = dict(source.document)
    if is_scoped_build(target.scope_suffix) and target.scope_id and target.node_chain:
        doc = patch_definition_for_scope(
            doc,
            scope_id=target.scope_id,
            node_chain=target.node_chain,
            levels=levels,
        )
    doc.setdefault("id", target.workflow_id)

    canvas = doc.get("canvas") or {}
    compiled = compile_canvas_dag(canvas if isinstance(canvas, dict) else {})
    doc["compiled_workflow"] = compiled

    workflow_base = resolve_workflow_base_for_build(
        source_kind=target.source_kind,
        config=config,
        workflow_id=target.workflow_id,
        canvas=canvas if isinstance(canvas, dict) else {},
    )
    default_cron = str(config.get("workflow_schedule") or "0 2 * * *")
    start_trigger = read_start_trigger_config(
        canvas if isinstance(canvas, dict) else {},
        default_cron=default_cron,
    )
    workflow_version = str(start_trigger.get("workflow_version") or "1")
    wf_ext = workflow_external_id(workflow_base=workflow_base, scope_suffix=target.scope_suffix)

    wv = build_workflow_version_document(
        workflow_external_id=wf_ext,
        version=workflow_version,
        compiled_workflow=compiled,
        description=str(doc.get("label") or doc.get("description") or target.workflow_id),
    )
    _validate_codegen_tasks(wv)
    wv = escape_workflow_version_document_for_cdf(wv)

    if not is_scoped_build(target.scope_suffix) and isinstance(doc.get("canvas"), dict):
        patch_start_node_workflow_pairing(
            doc["canvas"],
            workflow_base=workflow_base,
            scope_suffix=target.scope_suffix,
            workflow_version=workflow_version,
        )

    trimmed = trim_workflow_document_for_deploy(doc)
    assert_workflow_document_within_limit(trimmed, workflow_id=target.workflow_id)

    trigger_input = build_trigger_input(trimmed, default_cron=default_cron)
    assert_workflow_trigger_input_within_limit(trigger_input, workflow_id=target.workflow_id)

    trigger_doc: Dict[str, Any] = {
        "externalId": workflow_trigger_external_id(wf_ext),
        "workflowExternalId": wf_ext,
        "workflowVersion": workflow_version,
        "triggerRule": {
            "triggerType": "schedule",
            "cronExpression": default_cron,
        },
        "input": trigger_input,
    }
    apply_start_trigger_to_workflow_trigger(
        trigger_doc,
        workflow_external_id=wf_ext,
        trigger_cfg=start_trigger,
    )

    out_dir = workflow_artifacts_scope_dir(module_root, target.scope_suffix)
    written: List[Path] = []
    artifacts = {
        artifact_filename(target.workflow_id, target.scope_suffix, "config.yaml"): trimmed,
        artifact_filename(target.workflow_id, target.scope_suffix, "WorkflowVersion.yaml"): wv,
        artifact_filename(target.workflow_id, target.scope_suffix, "Workflow.yaml"): {
            "externalId": wf_ext,
            "description": str(doc.get("label") or doc.get("description") or target.workflow_id),
            "dataSetExternalId": str(config.get("dataset") or "ds_discovery_etl"),
        },
        artifact_filename(target.workflow_id, target.scope_suffix, "WorkflowTrigger.yaml"): trigger_doc,
    }
    tx_resources = emit_transformation_resources(compiled)
    if tx_resources:
        for i, tx in enumerate(tx_resources):
            ext = str(tx.get("externalId") or f"tr_{target.workflow_id}_{i}")
            artifacts[f"../transformations/{ext}.Transformation.yaml"] = tx

    for rel_name, payload in artifacts.items():
        out_path = (
            (out_dir / rel_name).resolve()
            if not rel_name.startswith("../")
            else (module_root / rel_name[3:]).resolve()
        )
        if dry_run:
            logger.info("[dry-run] would write %s", out_path)
        else:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open("w", encoding="utf-8") as f:
                yaml.safe_dump(payload, f, sort_keys=False, default_flow_style=False)
            logger.info("Wrote %s", out_path)
        written.append(out_path)

    if not is_scoped_build(target.scope_suffix):
        from workflow_build.sources import instances_dir, templates_dir

        if target.source_kind == "instance":
            inst_dir = instances_dir(module_root, config)
            inst_path = inst_dir / f"{target.workflow_id}.yaml"
            if dry_run:
                logger.info("[dry-run] would write %s", inst_path)
            else:
                inst_dir.mkdir(parents=True, exist_ok=True)
                with inst_path.open("w", encoding="utf-8") as f:
                    yaml.safe_dump(doc, f, sort_keys=False, default_flow_style=False)
                logger.info("Wrote %s", inst_path)
            written.append(inst_path)
        else:
            tpl_dir = templates_dir(module_root, config)
            tpl_path = tpl_dir / f"{target.workflow_id}.template.yaml"
            tpl_out = dict(doc)
            tpl_out["template_id"] = target.workflow_id
            tpl_out.pop("id", None)
            if dry_run:
                logger.info("[dry-run] would write %s", tpl_path)
            else:
                tpl_dir.mkdir(parents=True, exist_ok=True)
                with tpl_path.open("w", encoding="utf-8") as f:
                    yaml.safe_dump(tpl_out, f, sort_keys=False, default_flow_style=False)
                logger.info("Wrote %s", tpl_path)
            written.append(tpl_path)

    return written


def prepare_context(
    *,
    module_root: Path,
    config: dict,
    source: WorkflowBuildSource,
    target: ScopedWorkflowTarget,
    levels: List[str],
) -> BuildContext:
    from cdf_fn_common.workflow_compile.canvas_dag import compile_canvas_dag

    doc = dict(source.document)
    if is_scoped_build(target.scope_suffix) and target.scope_id and target.node_chain:
        doc = patch_definition_for_scope(
            doc,
            scope_id=target.scope_id,
            node_chain=target.node_chain,
            levels=levels,
        )
    canvas = doc.get("canvas") if isinstance(doc.get("canvas"), dict) else {}
    compiled = compile_canvas_dag(canvas)
    workflow_base = resolve_workflow_base_for_build(
        source_kind=target.source_kind,
        config=config,
        workflow_id=target.workflow_id,
        canvas=canvas,
    )
    default_cron = str(config.get("workflow_schedule") or "0 2 * * *")
    start = read_start_trigger_config(canvas, default_cron=default_cron)
    wf_ver = str(start.get("workflow_version") or "1")
    wf_ext = workflow_external_id(workflow_base=workflow_base, scope_suffix=target.scope_suffix)
    return BuildContext(
        module_root=module_root,
        config=config,
        target=target,
        source=source,
        scoped_document=doc,
        compiled_workflow=compiled,
        workflow_external_id=wf_ext,
        trigger_external_id=workflow_trigger_external_id(wf_ext),
        workflow_version=wf_ver,
        workflow_base=workflow_base,
    )
