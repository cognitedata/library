"""Deploy built workflow artifacts to CDF (CLI entry for ``module.py transform deploy-scope``)."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Sequence


def module_root_from_package() -> Path:
    return Path(__file__).resolve().parent.parent


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Deploy ETL workflow to CDF via Cognite SDK")
    parser.add_argument("--module-root", type=Path, default=None)
    parser.add_argument(
        "--workflow",
        "--pipeline",
        action="append",
        dest="workflow",
        default=[],
        help="Workflow instance id (repeatable; default: all instances with built artifacts)",
    )
    parser.add_argument("--scope-suffix", type=str, default="", metavar="SUFFIX")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-unresolved-placeholders", action="store_true")
    parser.add_argument(
        "--deploy-functions",
        choices=("never", "if-missing", "if-stale", "always"),
        default="if-stale",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    transform_root = args.module_root or module_root_from_package()
    scripts = transform_root / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))

    from deploy_workflow_cdf import main as deploy_main

    workflow_ids = args.workflow or []
    if not workflow_ids:
        instances = transform_root / "workflow_definitions" / "instances"
        if instances.is_dir():
            workflow_ids = [p.stem for p in sorted(instances.glob("*.yaml"))]
        if not workflow_ids:
            logging.error("No --workflow id and no instances under workflow_definitions/instances/")
            return 1

    rc = 0
    for wid in workflow_ids:
        deploy_argv = [
            "--workflow",
            wid,
            "--module-root",
            str(transform_root),
        ]
        if args.scope_suffix:
            deploy_argv.extend(["--scope-suffix", args.scope_suffix])
        if args.skip_build:
            deploy_argv.append("--skip-build")
        if args.dry_run:
            deploy_argv.append("--dry-run")
        if args.allow_unresolved_placeholders:
            deploy_argv.append("--allow-unresolved-placeholders")
        deploy_argv.extend(["--deploy-functions", args.deploy_functions])
        code = deploy_main(deploy_argv)
        if code != 0:
            rc = code
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
