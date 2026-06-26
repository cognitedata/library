"""Workflow build orchestration for CDF Discovery ETL."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import yaml
from runtime_paths import discovery_root_from_path, ensure_import_paths

from workflow_build.build_scoped import build_scoped_workflow
from workflow_build.paths import workflow_artifacts_root
from workflow_build.check import run_check
from workflow_build.sources import load_yaml, resolve_sources
from workflow_build.targets_resolve import scope_targets_for_source

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = "default.config.yaml"


def module_root_from_package() -> Path:
    return discovery_root_from_path(__file__)


def run_build(
    *,
    module_root: Path,
    config: Optional[dict] = None,
    config_path: Optional[Path] = None,
    workflow_ids: Optional[List[str]] = None,
    template_ids: Optional[List[str]] = None,
    scope_suffix: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Build workflow artifacts in-process (UI + CLI)."""
    ensure_import_paths(__file__, include_discovery_root=True)
    if config is None:
        cp = config_path or (module_root / DEFAULT_CONFIG)
        config = load_yaml(cp)

    sources = resolve_sources(
        module_root=module_root,
        config=config,
        workflow_ids=workflow_ids,
        template_ids=template_ids,
    )
    written: List[str] = []
    errors: List[str] = []
    for source in sources:
        targets = scope_targets_for_source(
            workflow_id=source.workflow_id,
            source_kind=source.source_kind,
            module_root=module_root,
            config=config,
            scope_suffix=scope_suffix,
        )
        for target in targets:
            try:
                paths = build_scoped_workflow(
                    module_root=module_root,
                    config=config,
                    source=source,
                    target=target,
                    levels=[],
                    dry_run=dry_run,
                )
                written.extend(str(p) for p in paths)
            except Exception as ex:
                errors.append(
                    f"{target.workflow_id}@{target.scope_suffix}: {type(ex).__name__}: {ex}"
                )

    return {
        "ok": not errors,
        "written": written,
        "errors": errors,
        "task_count": 0,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build CDF Discovery ETL workflow definitions")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--module-root", type=Path, default=None)
    parser.add_argument("--workflow", action="append", default=[], help="Workflow definition id")
    parser.add_argument("--template", action="append", default=[], help="Workflow template id")
    parser.add_argument("--scope-suffix", type=str, default=None, metavar="SUFFIX")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--check", action="store_true", help="Verify artifacts exist; no writes")
    parser.add_argument("--clean", action="store_true", help="Remove generated workflows/ YAML")
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.workflow and args.template:
        parser.error("Use --workflow or --template, not both")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    root = args.module_root or module_root_from_package()
    config_path = root / args.config
    if not config_path.is_file():
        logger.error("Missing config: %s", config_path)
        return 1

    ensure_import_paths(__file__, include_discovery_root=True)
    config = load_yaml(config_path)

    if args.check:
        return run_check(
            module_root=root,
            config=config,
            workflow_ids=args.workflow or None,
            template_ids=args.template or None,
            scope_suffix=args.scope_suffix,
        )

    if args.clean:
        workflows_dir = workflow_artifacts_root(root)
        if workflows_dir.is_dir():
            for p in sorted(workflows_dir.rglob("etl_*.yaml")):
                if args.dry_run:
                    logger.info("[dry-run] would remove %s", p)
                else:
                    p.unlink()
                    logger.info("Removed %s", p)
        return 0

    result = run_build(
        module_root=root,
        config=config,
        workflow_ids=args.workflow or None,
        template_ids=args.template or None,
        scope_suffix=args.scope_suffix,
        dry_run=args.dry_run,
    )
    for err in result.get("errors") or []:
        logger.error("%s", err)
    if not result.get("ok"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
