"""Orchestrate hierarchy load, validation, and builder execution."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Sequence

import yaml

from scope_build.builders.workflow_definitions import (
    RootWorkflowDefinitionsBuilder,
    ScopedWorkflowDefinitionsBuilder,
    verify_all_scoped_workflow_bundles,
    verify_root_workflow_bundle,
)
from scope_build.builders.workflow_triggers import WorkflowTriggersBuilder, verify_triggers_file
from scope_build.context import ScopeBuildContext
from scope_build.hierarchy import build_contexts, load_hierarchy_doc
from scope_build.mode import scope_build_mode_from_doc
from scope_build.paths import WORKFLOW_ARTIFACTS_REL, WORKFLOW_TEMPLATE_REL
from scope_build.workflow_clean import run_clean_workflow_artifacts
from scope_build.registry import (
    ScopeArtifactBuilder,
    default_builders,
    filter_builders,
)

logger = logging.getLogger(__name__)

DEFAULT_HIERARCHY = "default.config.yaml"
DEFAULT_WORKFLOW_EXTERNAL_ID = "key_extraction_aliasing"


def workflow_external_id_from_hierarchy(doc: dict) -> str:
    """Resolve workflow external id base (``default.config.yaml`` key ``workflow``)."""
    w = doc.get("workflow")
    if w is not None and str(w).strip():
        return str(w).strip()
    return DEFAULT_WORKFLOW_EXTERNAL_ID


DEFAULT_SCOPE_DOCUMENT = WORKFLOW_TEMPLATE_REL / "workflow.template.config.yaml"


def module_root_from_package() -> Path:
    """cdf_key_extraction_aliasing/ (parent of scripts/)."""
    return Path(__file__).resolve().parent.parent.parent


def run_build(
    *,
    module_root: Path,
    hierarchy_path: Path,
    builders: Sequence[ScopeArtifactBuilder],
    dry_run: bool,
) -> List[ScopeBuildContext]:
    doc = load_hierarchy_doc(hierarchy_path)
    contexts = build_contexts(module_root=module_root, doc=doc, dry_run=dry_run)
    logger.info("Found %d leaf scope(s)", len(contexts))

    for b in builders:
        if isinstance(b, RootWorkflowDefinitionsBuilder):
            b.write_once(module_root, dry_run=dry_run)

    for ctx in contexts:
        for b in builders:
            if isinstance(b, (RootWorkflowDefinitionsBuilder, WorkflowTriggersBuilder)):
                continue
            logger.debug("Builder %r → scope_id=%s", b.name, ctx.scope_id)
            b.run(ctx)

    triggers_builder = next((b for b in builders if isinstance(b, WorkflowTriggersBuilder)), None)
    if triggers_builder is not None:
        triggers_builder.write_all(
            contexts,
            dry_run=dry_run,
            module_root=module_root,
        )
    return contexts


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Create missing workflow artifacts from default.config.yaml (aliasing_scope_hierarchy + scope_build_mode). "
            "trigger_only: root Workflow/WorkflowVersion if missing, flat *.WorkflowTrigger.yaml under workflows/. "
            "full: scoped trio under workflows/<suffix>/. "
            "Templates are read from workflow_template/. Use --force to overwrite existing generated files. "
            "Use --clean to remove generated YAML under workflows/ (with confirmation or --yes); no build runs after clean."
        )
    )
    p.add_argument(
        "--force",
        action="store_true",
        help=(
            "Overwrite existing Workflow, WorkflowVersion, and WorkflowTrigger YAML when they "
            "already exist (refresh from templates). Does not apply to --check-workflow-triggers."
        ),
    )
    p.add_argument(
        "--hierarchy",
        "-f",
        type=Path,
        default=None,
        help=f"Path to hierarchy YAML (default: <module>/{DEFAULT_HIERARCHY})",
    )
    p.add_argument(
        "--scope-document",
        type=Path,
        default=None,
        help=f"Scope document YAML template (default: <module>/{DEFAULT_SCOPE_DOCUMENT})",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write files; log intended actions",
    )
    p.add_argument(
        "--clean",
        action="store_true",
        help=(
            "Remove generated Workflow/WorkflowVersion/WorkflowTrigger YAML under workflows/ "
            "(from hierarchy ``workflow`` id + legacy names). Does not run a build afterward; "
            "use a separate run without --clean to recreate. Non-interactive use requires --yes."
        ),
    )
    p.add_argument(
        "--yes",
        action="store_true",
        help="With --clean, skip confirmation (required when stdin is not a TTY).",
    )
    p.add_argument(
        "--list-builders",
        action="store_true",
        help="Print registered builder names and exit",
    )
    p.add_argument(
        "--only",
        action="append",
        dest="only_builders",
        metavar="NAME",
        default=None,
        help="Run only these builders (repeatable). Default: all. See --list-builders.",
    )
    p.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Debug logging",
    )
    p.add_argument(
        "--check-workflow-triggers",
        action="store_true",
        help=(
            "Exit 1 if workflow triggers or Workflow/WorkflowVersion artifacts required by "
            "scope_build_mode and the current hierarchy are missing, invalid, or out of date "
            "vs templates (no writes)."
        ),
    )
    p.add_argument(
        "--workflow-trigger-template",
        type=Path,
        default=None,
        help=(
            "YAML template for each schedule trigger (default: "
            "workflow_template/workflow.template.WorkflowTrigger.yaml)"
        ),
    )
    p.add_argument(
        "--workflow-template",
        type=Path,
        default=None,
        help=(
            "Workflow container template (default: "
            "workflow_template/workflow.template.Workflow.yaml)"
        ),
    )
    p.add_argument(
        "--workflow-version-template",
        type=Path,
        default=None,
        help=(
            "WorkflowVersion template (default: "
            "workflow_template/workflow.template.WorkflowVersion.yaml)"
        ),
    )
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )
    if args.clean and (args.list_builders or args.check_workflow_triggers):
        logging.getLogger(__name__).error(
            "--clean cannot be used with --list-builders or --check-workflow-triggers"
        )
        return 1
    module_root = module_root_from_package()
    hierarchy = args.hierarchy or (module_root / DEFAULT_HIERARCHY)
    scope_document = args.scope_document or (module_root / DEFAULT_SCOPE_DOCUMENT)
    workflow_template = args.workflow_template
    workflow_version_template = args.workflow_version_template
    if args.list_builders:
        try:
            doc = load_hierarchy_doc(hierarchy)
        except (OSError, ValueError, yaml.YAMLError):
            doc = {}
        mode = scope_build_mode_from_doc(doc)
        wf_base = workflow_external_id_from_hierarchy(doc)
        builders = default_builders(
            scope_build_mode=mode,
            workflow_base=wf_base,
            scope_document_path=scope_document,
            workflow_trigger_template_path=args.workflow_trigger_template,
            workflow_template_path=workflow_template,
            workflow_version_template_path=workflow_version_template,
            overwrite=bool(args.force),
        )
        for b in builders:
            print(b.name)
        return 0
    try:
        doc = load_hierarchy_doc(hierarchy)
        mode = scope_build_mode_from_doc(doc)
        wf_base = workflow_external_id_from_hierarchy(doc)
    except (OSError, ValueError, yaml.YAMLError) as e:
        logging.getLogger(__name__).error("%s", e)
        return 1
    if args.clean:
        return run_clean_workflow_artifacts(
            module_root,
            wf_base,
            dry_run=bool(args.dry_run),
            assume_yes=bool(args.yes),
            stdin_isatty=sys.stdin.isatty(),
        )
    builders = default_builders(
        scope_build_mode=mode,
        workflow_base=wf_base,
        scope_document_path=scope_document,
        workflow_trigger_template_path=args.workflow_trigger_template,
        workflow_template_path=workflow_template,
        workflow_version_template_path=workflow_version_template,
        overwrite=bool(args.force),
    )
    try:
        builders = filter_builders(builders, args.only_builders)
    except ValueError as e:
        logger.error("%s", e)
        return 1
    if args.check_workflow_triggers:
        contexts = build_contexts(module_root=module_root, doc=doc, dry_run=True)
        try:
            verify_triggers_file(
                module_root,
                list(contexts),
                template_path=args.workflow_trigger_template,
                scope_document_path=scope_document,
                mode=mode,
                workflow_base=wf_base,
            )
            if mode == "trigger_only":
                verify_root_workflow_bundle(
                    module_root,
                    wf_base,
                    workflow_template_path=workflow_template,
                    workflow_version_template_path=workflow_version_template,
                )
            else:
                verify_all_scoped_workflow_bundles(
                    module_root,
                    contexts,
                    wf_base,
                    workflow_template_path=workflow_template,
                    workflow_version_template_path=workflow_version_template,
                )
        except SystemExit as e:
            logger.error("%s", e)
            code = getattr(e, "code", 1)
            return int(code) if isinstance(code, int) else 1
        logger.info("Workflow artifacts OK for scope_build_mode=%s", mode)
        return 0
    try:
        run_build(
            module_root=module_root,
            hierarchy_path=hierarchy,
            builders=builders,
            dry_run=bool(args.dry_run),
        )
    except (OSError, ValueError, yaml.YAMLError) as e:
        logger.error("%s", e)
        return 1
    return 0
