"""Orchestrate hierarchy load, validation, and builder execution."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List, Sequence

import yaml

from scope_build.builders.workflow_triggers import WorkflowTriggersBuilder
from scope_build.context import ScopeBuildContext
from scope_build.hierarchy import build_contexts, load_hierarchy_doc
from scope_build.registry import (
    ScopeArtifactBuilder,
    default_builders,
    filter_builders,
)

logger = logging.getLogger(__name__)

DEFAULT_HIERARCHY = "default.config.yaml"
DEFAULT_SCOPE_DOCUMENT = Path("workflows") / "_template" / "workflow.template.config.yaml"


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
    per_scope_builders = [b for b in builders if not isinstance(b, WorkflowTriggersBuilder)]
    triggers_builder = next((b for b in builders if isinstance(b, WorkflowTriggersBuilder)), None)
    for ctx in contexts:
        for b in per_scope_builders:
            logger.debug("Builder %r → scope_id=%s", b.name, ctx.scope_id)
            b.run(ctx)
    if triggers_builder is not None:
        triggers_builder.write_all(
            contexts, dry_run=dry_run, module_root=module_root
        )
    return contexts


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Create missing workflow schedule triggers from default.config.yaml (scope_hierarchy.levels + scope_hierarchy.locations). "
            "Does not overwrite existing key_extraction_aliasing.*.WorkflowTrigger.yaml files. "
            "Embeds patched scope documents from workflows/_template/workflow.template.config.yaml."
        )
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
            "Exit 1 if any trigger required by the current hierarchy is missing, invalid, or out of "
            "date vs templates (no writes). Extra key_extraction_aliasing.*.WorkflowTrigger.yaml "
            "files on disk are allowed."
        ),
    )
    p.add_argument(
        "--workflow-trigger-template",
        type=Path,
        default=None,
        help=(
            "YAML template for each schedule trigger (default: "
            "workflows/_template/workflow.template.WorkflowTrigger.yaml.template)"
        ),
    )
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )
    module_root = module_root_from_package()
    hierarchy = args.hierarchy or (module_root / DEFAULT_HIERARCHY)
    scope_document = args.scope_document or (module_root / DEFAULT_SCOPE_DOCUMENT)
    builders = default_builders(
        scope_document_path=scope_document,
        workflow_trigger_template_path=args.workflow_trigger_template,
    )
    if args.list_builders:
        for b in builders:
            print(b.name)
        return 0
    try:
        builders = filter_builders(builders, args.only_builders)
    except ValueError as e:
        logger.error("%s", e)
        return 1
    if args.check_workflow_triggers:
        from scope_build.builders.workflow_triggers import verify_triggers_file

        doc = load_hierarchy_doc(hierarchy)
        contexts = build_contexts(module_root=module_root, doc=doc, dry_run=True)
        try:
            verify_triggers_file(
                module_root,
                list(contexts),
                template_path=args.workflow_trigger_template,
                scope_document_path=scope_document,
            )
        except SystemExit as e:
            logger.error("%s", e)
            code = getattr(e, "code", 1)
            return int(code) if isinstance(code, int) else 1
        logger.info("Workflow trigger files OK for current hierarchy")
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
