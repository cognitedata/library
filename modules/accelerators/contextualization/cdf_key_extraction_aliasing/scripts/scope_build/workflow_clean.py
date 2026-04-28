"""Discover and remove generated workflow YAML under ``workflows/`` (templates untouched)."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from scope_build.builders.workflow_triggers import LEGACY_MONOLITHIC_NAME
from scope_build.paths import WORKFLOW_ARTIFACTS_REL

logger = logging.getLogger(__name__)

_WORKFLOW_ARTIFACT_SUFFIXES = (
    ".Workflow.yaml",
    ".WorkflowVersion.yaml",
    ".WorkflowTrigger.yaml",
)

_SUMMARY_MAX_LIST = 50


def discover_workflow_artifact_paths(module_root: Path, wf_base: str) -> list[Path]:
    """Paths to generated workflow artifacts (recursive under ``workflows/``) plus legacy root names."""
    workflows_dir = module_root / WORKFLOW_ARTIFACTS_REL
    if not workflows_dir.is_dir():
        return []

    found: set[Path] = set()
    prefix = f"{wf_base}."
    for p in workflows_dir.rglob("*.yaml"):
        if not p.is_file():
            continue
        n = p.name
        if n.startswith(prefix) and n.endswith(_WORKFLOW_ARTIFACT_SUFFIXES):
            found.add(p.resolve())

    legacy = workflows_dir / LEGACY_MONOLITHIC_NAME
    if legacy.is_file():
        found.add(legacy.resolve())

    for pattern in (
        f"{wf_base}_*.WorkflowTrigger.yaml",
        f"cdf_{wf_base}_*.WorkflowTrigger.yaml",
    ):
        for p in workflows_dir.glob(pattern):
            if p.is_file():
                found.add(p.resolve())

    return sorted(found)


def _prune_empty_dirs_under(workflows_dir: Path) -> None:
    """Remove empty subdirectories under ``workflows_dir``; never removes ``workflows_dir`` itself."""
    if not workflows_dir.is_dir():
        return
    root_resolved = workflows_dir.resolve()
    for root, _dirnames, _filenames in os.walk(workflows_dir, topdown=False):
        path = Path(root).resolve()
        if path == root_resolved:
            continue
        try:
            if not any(path.iterdir()):
                path.rmdir()
                logger.info("Removed empty directory %s", path.relative_to(workflows_dir))
        except OSError:
            pass


def _format_summary_lines(paths: list[Path], module_root: Path) -> tuple[list[str], int]:
    rel = [p.relative_to(module_root) for p in paths]
    lines = [str(r) for r in rel[:_SUMMARY_MAX_LIST]]
    more = max(0, len(rel) - len(lines))
    return lines, more


def run_clean_workflow_artifacts(
    module_root: Path,
    wf_base: str,
    *,
    dry_run: bool,
    assume_yes: bool,
    stdin_isatty: bool,
) -> int:
    """Delete discovered artifacts unless ``dry_run``. Returns 0 on success, 1 on abort or error."""
    paths = discover_workflow_artifact_paths(module_root, wf_base)
    if not paths:
        logger.info("No generated workflow YAML matched for workflow=%r under %s/", wf_base, WORKFLOW_ARTIFACTS_REL)
        return 0

    lines, more = _format_summary_lines(paths, module_root)
    print(
        f"This will permanently delete {len(paths)} file(s) under {WORKFLOW_ARTIFACTS_REL}/. "
        "This cannot be undone.",
        file=sys.stderr,
    )
    for line in lines:
        print(f"  {line}", file=sys.stderr)
    if more:
        print(f"  … and {more} more", file=sys.stderr)

    if dry_run:
        logger.info("Dry-run: no files deleted.")
        return 0

    if not assume_yes and not stdin_isatty:
        logger.error(
            "Refusing to delete without a TTY: re-run with --yes to confirm non-interactive clean."
        )
        return 1

    if not assume_yes:
        print("Type 'yes' to delete these files:", file=sys.stderr, end=" ")
        sys.stderr.flush()
        if input().strip() != "yes":
            logger.info("Clean cancelled.")
            return 1

    workflows_dir = module_root / WORKFLOW_ARTIFACTS_REL
    for p in paths:
        try:
            p.unlink()
            logger.info("Deleted %s", p.relative_to(module_root))
        except OSError as e:
            logger.error("Failed to delete %s: %s", p, e)
            return 1

    _prune_empty_dirs_under(workflows_dir)
    logger.info("Workflow artifact clean finished (%d file(s) removed).", len(paths))
    return 0
