"""Copy ``workflow.local.config.yaml`` into ``workflow_template/workflow.template.config.yaml`` (unified scope)."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Sequence

from scope_build.orchestrate import module_root_from_package
from scope_build.paths import WORKFLOW_TEMPLATE_REL
from scope_build.scope_yaml_io import promote_unified_scope_file_to_template_config

logger = logging.getLogger(__name__)

LOCAL_CONFIG_NAME = "workflow.local.config.yaml"


def run_promote_local_workflow_templates(argv: Sequence[str] | None) -> int:
    p = argparse.ArgumentParser(
        description=(
            f"Overwrite ``{WORKFLOW_TEMPLATE_REL / 'workflow.template.config.yaml'}`` from "
            f"``{LOCAL_CONFIG_NAME}`` (unified scope document including embedded ``canvas``). "
            "Review the diff before committing (instance spaces, client ids, or other env-specific values "
            "often belong only in local or Toolkit substitutions)."
        )
    )
    p.add_argument(
        "--module-root",
        type=Path,
        default=None,
        help="cdf_discovery_aliasing/ directory (default: inferred from this package)",
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

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    module_root = (args.module_root or module_root_from_package()).resolve()
    tpl_dir = module_root / WORKFLOW_TEMPLATE_REL
    src = module_root / LOCAL_CONFIG_NAME
    dst = tpl_dir / "workflow.template.config.yaml"

    if not src.is_file():
        logger.error("Missing source file: %s", src)
        return 1
    if args.dry_run:
        logger.info("[dry-run] would promote %s → %s (normalize, strip compiled_workflow)", src, dst)
        return 0
    try:
        promote_unified_scope_file_to_template_config(source=src, destination=dst)
    except ValueError as e:
        logger.error("%s", e)
        return 1
    logger.info("Wrote %s", dst)
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
