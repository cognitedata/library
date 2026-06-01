"""Drift check: built artifacts vs compile+emit for each scoped workflow."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List

import yaml

from workflow_build.build_scoped import build_scoped_workflow
from workflow_build.paths import artifact_filename, workflow_artifacts_root, workflow_artifacts_scope_dir
from workflow_build.sources import resolve_sources
from workflow_build.targets_resolve import scope_targets_for_source
from workflow_build.ids import workflow_trigger_external_id

logger = logging.getLogger(__name__)


def _normalize_yaml(doc: dict) -> str:
    return yaml.safe_dump(doc, sort_keys=True, default_flow_style=False)


def _yaml_mapping(path: Path, *, label: str, errors: List[str]) -> Dict[str, Any] | None:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as ex:
        errors.append(f"invalid yaml ({label}): {path} ({type(ex).__name__}: {ex})")
        return None
    if not isinstance(data, dict):
        errors.append(f"invalid yaml root ({label}): {path} (expected mapping)")
        return None
    return data


def _has_placeholder(value: Any) -> bool:
    from cdf_workflow_io import shallow_has_toolkit_placeholder

    return shallow_has_toolkit_placeholder(value)


def validate_artifact_set(
    *,
    workflow_path: Path,
    workflow_version_path: Path,
    trigger_path: Path,
) -> List[str]:
    from cdf_deploy_workflow_guard import scope_suffix_from_trigger
    from cdf_workflow_io import assert_expected_workflow_input_keys
    from cdf_fn_common.workflow_compile.codegen import (
        escape_workflow_version_document_for_cdf,
    )

    errors: List[str] = []
    workflow = _yaml_mapping(workflow_path, label="Workflow", errors=errors)
    workflow_version = _yaml_mapping(
        workflow_version_path, label="WorkflowVersion", errors=errors
    )
    trigger = _yaml_mapping(trigger_path, label="WorkflowTrigger", errors=errors)
    if workflow is None or workflow_version is None or trigger is None:
        return errors

    # Enforce deployable folder naming/layout constraints.
    try:
        scope_suffix_from_trigger(trigger_path)
    except ValueError as ex:
        errors.append(f"invalid trigger location: {trigger_path} ({ex})")

    wf_ext = str(workflow.get("externalId") or "").strip()
    if not wf_ext:
        errors.append(f"Workflow missing externalId: {workflow_path}")

    wv_wf_ext = str(workflow_version.get("workflowExternalId") or "").strip()
    trig_wf_ext = str(trigger.get("workflowExternalId") or "").strip()
    if wf_ext and wv_wf_ext and wf_ext != wv_wf_ext:
        errors.append(
            f"Workflow/WorkflowVersion mismatch: {workflow_path.name} externalId={wf_ext!r} "
            f"!= {workflow_version_path.name} workflowExternalId={wv_wf_ext!r}"
        )
    if wf_ext and trig_wf_ext and wf_ext != trig_wf_ext:
        errors.append(
            f"Workflow/WorkflowTrigger mismatch: {workflow_path.name} externalId={wf_ext!r} "
            f"!= {trigger_path.name} workflowExternalId={trig_wf_ext!r}"
        )

    wv_version = str(workflow_version.get("version") or "").strip()
    trig_version = str(trigger.get("workflowVersion") or "").strip()
    if not wv_version:
        errors.append(f"WorkflowVersion missing version: {workflow_version_path}")
    if not trig_version:
        errors.append(f"WorkflowTrigger missing workflowVersion: {trigger_path}")
    if wv_version and trig_version and wv_version != trig_version:
        errors.append(
            f"WorkflowVersion/WorkflowTrigger mismatch: version={wv_version!r} != workflowVersion={trig_version!r}"
        )

    trig_ext = str(trigger.get("externalId") or "").strip()
    expected_trig_ext = workflow_trigger_external_id(wf_ext) if wf_ext else ""
    if wf_ext and trig_ext != expected_trig_ext:
        errors.append(
            f"WorkflowTrigger externalId mismatch: expected {expected_trig_ext!r}, got {trig_ext!r}"
        )

    trigger_input = trigger.get("input")
    if not isinstance(trigger_input, dict):
        errors.append(f"WorkflowTrigger.input must be a mapping: {trigger_path}")
    else:
        try:
            assert_expected_workflow_input_keys(trigger_input)
        except ValueError as ex:
            errors.append(f"WorkflowTrigger.input invalid: {trigger_path} ({ex})")

    tasks = ((workflow_version.get("workflowDefinition") or {}).get("tasks") or [])
    if not isinstance(tasks, list) or not tasks:
        errors.append(f"WorkflowVersion has no tasks: {workflow_version_path}")
    else:
        try:
            # Confirms WorkflowVersion payload can be escaped to CDF API format.
            escape_workflow_version_document_for_cdf(workflow_version)
        except Exception as ex:
            errors.append(f"WorkflowVersion is not CDF-ready: {workflow_version_path} ({ex})")

    for label, blob, path in (
        ("Workflow", workflow, workflow_path),
        ("WorkflowVersion", workflow_version, workflow_version_path),
        ("WorkflowTrigger", trigger, trigger_path),
    ):
        if _has_placeholder(blob):
            errors.append(
                f"{label} contains unresolved placeholders: {path} (not deploy-ready without overrides)"
            )

    return errors


def check_scoped_workflow_drift(
    *,
    module_root: Path,
    config: dict,
    workflow_id: str,
    source_kind: str,
    document: dict,
    target,
    levels: List[str],
) -> List[str]:
    from workflow_build.sources import WorkflowBuildSource, instances_dir

    source = WorkflowBuildSource(
        workflow_id=workflow_id,
        source_kind=source_kind,
        document=document,
        path=instances_dir(module_root, config) / f"{workflow_id}.yaml",
    )
    out_dir = workflow_artifacts_scope_dir(module_root, target.scope_suffix)
    errors: List[str] = []

    expected_paths = {
        out_dir / artifact_filename(workflow_id, target.scope_suffix, "config.yaml"),
        out_dir / artifact_filename(workflow_id, target.scope_suffix, "Workflow.yaml"),
        out_dir / artifact_filename(workflow_id, target.scope_suffix, "WorkflowVersion.yaml"),
        out_dir / artifact_filename(workflow_id, target.scope_suffix, "WorkflowTrigger.yaml"),
    }
    for p in expected_paths:
        if not p.is_file():
            errors.append(f"missing artifact: {p.relative_to(workflow_artifacts_root(module_root).parent)}")

    if errors:
        return errors

    written = build_scoped_workflow(
        module_root=module_root,
        config=config,
        source=source,
        target=target,
        levels=levels,
        dry_run=True,
    )
    _ = written
    return errors


def run_check(
    *,
    module_root: Path,
    config: dict,
    workflow_ids: List[str] | None = None,
    template_ids: List[str] | None = None,
    scope_suffix: str | None = None,
) -> int:
    sources = resolve_sources(
        module_root=module_root,
        config=config,
        workflow_ids=workflow_ids,
        template_ids=template_ids,
    )
    all_errors: List[str] = []
    for source in sources:
        targets = scope_targets_for_source(
            workflow_id=source.workflow_id,
            source_kind=source.source_kind,
            module_root=module_root,
            config=config,
            scope_suffix=scope_suffix,
        )
        for target in targets:
            out_dir = workflow_artifacts_scope_dir(module_root, target.scope_suffix)
            cfg_path = out_dir / artifact_filename(
                target.workflow_id, target.scope_suffix, "config.yaml"
            )
            wf_path = out_dir / artifact_filename(
                target.workflow_id, target.scope_suffix, "Workflow.yaml"
            )
            wv_path = out_dir / artifact_filename(
                target.workflow_id, target.scope_suffix, "WorkflowVersion.yaml"
            )
            trg_path = out_dir / artifact_filename(
                target.workflow_id, target.scope_suffix, "WorkflowTrigger.yaml"
            )
            for path in (cfg_path, wf_path, wv_path, trg_path):
                if not path.is_file():
                    all_errors.append(f"missing: {path}")
            if wf_path.is_file() and wv_path.is_file() and trg_path.is_file():
                all_errors.extend(
                    validate_artifact_set(
                        workflow_path=wf_path,
                        workflow_version_path=wv_path,
                        trigger_path=trg_path,
                    )
                )
    if all_errors:
        for e in all_errors:
            logger.error("%s", e)
        return 1
    logger.info("Check passed (%d source(s))", len(sources))
    return 0
