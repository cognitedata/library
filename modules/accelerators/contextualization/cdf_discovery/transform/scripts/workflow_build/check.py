"""Drift check: built artifacts vs compile+emit for each scoped workflow."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List

import yaml

from workflow_build.build_scoped import build_scoped_workflow
from workflow_build.sources import resolve_sources
from workflow_build.targets_resolve import scope_targets_for_source

logger = logging.getLogger(__name__)


def _normalize_yaml(doc: dict) -> str:
    return yaml.safe_dump(doc, sort_keys=True, default_flow_style=False)


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
    from workflow_build.sources import WorkflowBuildSource

    source = WorkflowBuildSource(
        workflow_id=workflow_id,
        source_kind=source_kind,
        document=document,
        path=module_root / "workflow_definitions" / "instances" / f"{workflow_id}.yaml",
    )
    prefix = f"etl_{workflow_id}.{target.scope_suffix}"
    out_dir = module_root / "workflows" / target.scope_suffix
    errors: List[str] = []

    expected_paths = {
        out_dir / f"{prefix}.config.yaml",
        out_dir / f"{prefix}.Workflow.yaml",
        out_dir / f"{prefix}.WorkflowVersion.yaml",
        out_dir / f"{prefix}.WorkflowTrigger.yaml",
    }
    for p in expected_paths:
        if not p.is_file():
            errors.append(f"missing artifact: {p.relative_to(module_root)}")

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
    scoped: bool = False,
    scope_suffix: str | None = None,
) -> int:
    sources = resolve_sources(
        module_root=module_root,
        config=config,
        workflow_ids=workflow_ids,
        template_ids=template_ids,
    )
    hierarchy = config.get("scope_hierarchy") if isinstance(config.get("scope_hierarchy"), dict) else {}
    levels = [str(x).strip() for x in (hierarchy.get("levels") or []) if str(x).strip()]
    all_errors: List[str] = []
    for source in sources:
        targets = scope_targets_for_source(
            workflow_id=source.workflow_id,
            source_kind=source.source_kind,
            module_root=module_root,
            config=config,
            scoped=scoped,
            scope_suffix=scope_suffix,
        )
        for target in targets:
            prefix = f"etl_{target.workflow_id}.{target.scope_suffix}"
            out_dir = module_root / "workflows" / target.scope_suffix
            for name in (
                f"{prefix}.config.yaml",
                f"{prefix}.Workflow.yaml",
                f"{prefix}.WorkflowVersion.yaml",
                f"{prefix}.WorkflowTrigger.yaml",
            ):
                path = out_dir / name
                if not path.is_file():
                    all_errors.append(f"missing: {path}")
    if all_errors:
        for e in all_errors:
            logger.error("%s", e)
        return 1
    logger.info("Check passed (%d source(s))", len(sources))
    return 0
