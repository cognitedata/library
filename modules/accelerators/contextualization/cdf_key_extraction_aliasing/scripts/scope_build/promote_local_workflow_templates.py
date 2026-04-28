"""Copy ``workflow.local.*`` scope + canvas YAML into ``workflow_template/`` as canonical templates."""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path
from typing import Sequence

from scope_build.orchestrate import module_root_from_package
from scope_build.paths import WORKFLOW_TEMPLATE_REL

logger = logging.getLogger(__name__)

LOCAL_CONFIG_NAME = "workflow.local.config.yaml"
LOCAL_CANVAS_NAME = "workflow.local.canvas.yaml"


def run_promote_local_workflow_templates(argv: Sequence[str] | None) -> int:
    p = argparse.ArgumentParser(
        description=(
            "Overwrite workflow authoring templates from the local operator files: "
            f"``{LOCAL_CONFIG_NAME}`` → ``{WORKFLOW_TEMPLATE_REL / 'workflow.template.config.yaml'}``, "
            f"``{LOCAL_CANVAS_NAME}`` → ``{WORKFLOW_TEMPLATE_REL / 'workflow.template.canvas.yaml'}``. "
            "Review the diff before committing (instance spaces, client ids, or other env-specific values "
            "often belong only in local or Toolkit substitutions)."
        )
    )
    p.add_argument(
        "--module-root",
        type=Path,
        default=None,
        help="cdf_key_extraction_aliasing/ directory (default: inferred from this package)",
    )
    p.add_argument(
        "--config-only",
        action="store_true",
        help=f"Promote only ``{LOCAL_CONFIG_NAME}``",
    )
    p.add_argument(
        "--canvas-only",
        action="store_true",
        help=f"Promote only ``{LOCAL_CANVAS_NAME}``",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Log planned copies; do not write template files",
    )
    p.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Debug logging",
    )
    args = p.parse_args(list(argv) if argv is not None else None)

    if args.config_only and args.canvas_only:
        p.error("--config-only and --canvas-only are mutually exclusive")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    module_root = (args.module_root or module_root_from_package()).resolve()
    tpl_dir = module_root / WORKFLOW_TEMPLATE_REL

    pairs: list[tuple[Path, Path, str]] = []
    if not args.canvas_only:
        pairs.append(
            (
                module_root / LOCAL_CONFIG_NAME,
                tpl_dir / "workflow.template.config.yaml",
                "config",
            )
        )
    if not args.config_only:
        pairs.append(
            (
                module_root / LOCAL_CANVAS_NAME,
                tpl_dir / "workflow.template.canvas.yaml",
                "canvas",
            )
        )

    for src, dst, label in pairs:
        if not src.is_file():
            logger.error("Missing source %s file: %s", label, src)
            return 1
        if args.dry_run:
            logger.info("[dry-run] would copy %s → %s", src, dst)
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        logger.info("Wrote %s (%s)", dst, label)

    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run_promote_local_workflow_templates(argv)
    except (OSError, ValueError) as e:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        logging.getLogger(__name__).error("%s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
