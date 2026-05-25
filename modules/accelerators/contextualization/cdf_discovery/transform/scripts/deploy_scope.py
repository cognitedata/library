"""Deploy-scope stub for CDF Discovery ETL pipelines."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Sequence


def module_root_from_package() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Deploy ETL pipeline scope to CDF (stub)")
    parser.add_argument("--config", default="default.config.yaml")
    parser.add_argument("--module-root", type=Path, default=None)
    parser.add_argument("--workflow", action="append", default=[], dest="pipeline", help="Workflow instance id (repeatable)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    root = args.module_root or module_root_from_package()
    instances_dir = root / "workflow_definitions" / "instances"
    if not instances_dir.is_dir():
        logging.error("No workflow definitions at %s — run build first", instances_dir)
        return 1
    if args.pipeline:
        yaml_files = [instances_dir / f"{wid}.yaml" for wid in args.pipeline]
    else:
        yaml_files = sorted(instances_dir.glob("*.yaml"))
    if not yaml_files:
        logging.error("No workflow instance YAML in %s", instances_dir)
        return 1
    for path in yaml_files:
        if not path.is_file():
            logging.error("Missing workflow instance %s", path)
            return 1
        logging.info(
            "%s deploy-scope: %s (stub — wire Cognite SDK upsert here)",
            "dry-run" if args.dry_run else "would",
            path.name,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
