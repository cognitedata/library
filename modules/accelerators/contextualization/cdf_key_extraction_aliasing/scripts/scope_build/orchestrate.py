"""Orchestrate hierarchy load, validation, and builder execution."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Sequence

import yaml

from scope_build.builders.workflow_definitions import (
    ScopedWorkflowDefinitionsBuilder,
    verify_all_scoped_workflow_bundles,
)
from scope_build.builders.workflow_triggers import WorkflowTriggersBuilder, verify_triggers_file
from scope_build.context import ScopeBuildContext
from scope_build.hierarchy import build_contexts, load_hierarchy_doc
from scope_build.mode import scope_build_mode_from_doc
from scope_build.naming import cdf_external_id_suffix
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


def filter_contexts_by_scope_suffix(
    contexts: Sequence[ScopeBuildContext], scope_suffix: str
) -> List[ScopeBuildContext]:
    """Keep leaves whose CDF suffix (``workflows/<suffix>/`` folder name) equals ``scope_suffix``."""
    wanted = scope_suffix.strip()
    if not wanted:
        raise ValueError("scope_suffix must be non-empty")
    return [c for c in contexts if cdf_external_id_suffix(c.scope_id) == wanted]


def resolve_contexts_for_optional_suffix(
    *,
    module_root: Path,
    doc: dict,
    dry_run: bool,
    scope_suffix: str | None,
) -> List[ScopeBuildContext]:
    """All leaves, or exactly one leaf when ``scope_suffix`` is set; raises ``ValueError`` if invalid."""
    contexts = build_contexts(module_root=module_root, doc=doc, dry_run=dry_run)
    if scope_suffix is None or not str(scope_suffix).strip():
        return contexts
    ss = str(scope_suffix).strip()
    filtered = filter_contexts_by_scope_suffix(contexts, ss)
    if len(filtered) == 1:
        return filtered
    valid = sorted({cdf_external_id_suffix(c.scope_id) for c in contexts})
    if not filtered:
        raise ValueError(
            f"No leaf matches --scope-suffix {ss!r}. Valid suffixes (workflows/ folder names): {valid}"
        )
    raise ValueError(
        f"Multiple leaves match --scope-suffix {ss!r} ({len(filtered)} matches). "
        "Hierarchy scope paths must yield a unique suffix."
    )


def run_build(
    *,
    module_root: Path,
    hierarchy_path: Path,
    builders: Sequence[ScopeArtifactBuilder],
    dry_run: bool,
    contexts: Sequence[ScopeBuildContext] | None = None,
) -> List[ScopeBuildContext]:
    if contexts is None:
        doc = load_hierarchy_doc(hierarchy_path)
        ctx_list = build_contexts(module_root=module_root, doc=doc, dry_run=dry_run)
    else:
        ctx_list = list(contexts)
    logger.info("Processing %d leaf scope(s)", len(ctx_list))

    for ctx in ctx_list:
        for b in builders:
            if isinstance(b, WorkflowTriggersBuilder):
                continue
            logger.debug("Builder %r → scope_id=%s", b.name, ctx.scope_id)
            b.run(ctx)

    triggers_builder = next((b for b in builders if isinstance(b, WorkflowTriggersBuilder)), None)
    if triggers_builder is not None:
        triggers_builder.write_all(
            ctx_list,
            dry_run=dry_run,
            module_root=module_root,
        )
    return ctx_list


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Create scoped workflow artifacts from default.config.yaml (aliasing_scope_hierarchy). "
            "Per leaf: workflows/<suffix>/Workflow.yaml, WorkflowVersion.yaml, WorkflowTrigger.yaml "
            "(created when missing; existing files are only overwritten with --force). "
            "workflow_template/workflow.execution.graph.yaml is refreshed from IR on every build (no --force). "
            "WorkflowVersion is generated from compiled_workflow IR (canvas); Workflow.yaml uses workflow_template/. "
            "Scope template loads with sibling *.canvas.yaml merge (same as module.py run). "
            "Use --clean to remove generated YAML under workflows/ (with confirmation or --yes); no build runs after clean."
        )
    )
    p.add_argument(
        "--force",
        action="store_true",
        help=(
            "Overwrite existing scoped Workflow.yaml, WorkflowVersion.yaml, and WorkflowTrigger.yaml "
            "from templates + compiled IR. Without --force, those files are created if missing but left "
            "unchanged when already present. workflow.execution.graph.yaml is refreshed every build without --force. "
            "Does not apply to --check-workflow-triggers."
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
            "Exit 1 if scoped workflow triggers or Workflow/WorkflowVersion artifacts required by "
            "the current hierarchy are missing, invalid, or out of date vs templates (no writes)."
        ),
    )
    p.add_argument(
        "--scope-suffix",
        type=str,
        default=None,
        metavar="SUFFIX",
        help=(
            "Restrict build or --check-workflow-triggers to the single hierarchy leaf whose "
            "workflows/ folder name is SUFFIX (same as cdf_external_id_suffix(scope_id)). "
            "Other leaves' files under workflows/ are not updated on build."
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
    if args.clean and (
        args.list_builders or args.check_workflow_triggers or args.scope_suffix
    ):
        logging.getLogger(__name__).error(
            "--clean cannot be used with --list-builders, --check-workflow-triggers, or --scope-suffix"
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
        scope_build_mode_from_doc(doc)
        wf_base = workflow_external_id_from_hierarchy(doc)
        builders = default_builders(
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
        scope_build_mode_from_doc(doc)
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
        try:
            contexts = resolve_contexts_for_optional_suffix(
                module_root=module_root,
                doc=doc,
                dry_run=True,
                scope_suffix=args.scope_suffix,
            )
        except ValueError as e:
            logger.error("%s", e)
            return 1
        try:
            verify_triggers_file(
                module_root,
                list(contexts),
                template_path=args.workflow_trigger_template,
                scope_document_path=scope_document,
                workflow_base=wf_base,
            )
            verify_all_scoped_workflow_bundles(
                module_root,
                contexts,
                wf_base,
                workflow_template_path=workflow_template,
                scope_document_path=scope_document,
            )
        except SystemExit as e:
            logger.error("%s", e)
            code = getattr(e, "code", 1)
            return int(code) if isinstance(code, int) else 1
        logger.info("Workflow artifacts OK")
        return 0
    try:
        contexts = resolve_contexts_for_optional_suffix(
            module_root=module_root,
            doc=doc,
            dry_run=bool(args.dry_run),
            scope_suffix=args.scope_suffix,
        )
    except ValueError as e:
        logger.error("%s", e)
        return 1
    try:
        run_build(
            module_root=module_root,
            hierarchy_path=hierarchy,
            builders=builders,
            dry_run=bool(args.dry_run),
            contexts=contexts,
        )
    except (OSError, ValueError, yaml.YAMLError) as e:
        logger.error("%s", e)
        return 1
    return 0
