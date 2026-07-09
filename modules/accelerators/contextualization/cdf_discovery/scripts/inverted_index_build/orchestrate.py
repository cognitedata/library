"""CLI for inverted-index WorkflowTrigger generation and parity checks."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from inverted_index_build.workflow_triggers import (
    check_inverted_index_triggers,
    generate_inverted_index_triggers,
)

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_REL = Path("submodules") / "inverted_index" / "default.config.yaml"


def module_root_from_here() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    root = module_root_from_here()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--module-root",
        type=Path,
        default=root,
        help="cdf_discovery module root",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help=f"Inverted-index config YAML (default: <module>/{DEFAULT_CONFIG_REL})",
    )
    parser.add_argument(
        "--workflows-dir",
        type=Path,
        default=None,
        help="Output directory for WorkflowTrigger YAML (default: <module>/workflows)",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--build-inverted-index-triggers",
        action="store_true",
        help="Write generated wf_discovery_idx_* WorkflowTrigger YAML",
    )
    group.add_argument(
        "--check-inverted-index-triggers",
        action="store_true",
        help="Verify committed triggers match runtime config (no writes)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing trigger files when building",
    )
    args = parser.parse_args(argv)

    module_root = args.module_root.resolve()
    config_path = (args.config or (module_root / DEFAULT_CONFIG_REL)).resolve()
    workflows_dir = (args.workflows_dir or (module_root / "workflows")).resolve()

    if not config_path.is_file():
        logger.error("Missing config: %s", config_path)
        return 1

    if args.check_inverted_index_triggers:
        errors = check_inverted_index_triggers(
            config_path=config_path,
            workflows_dir=workflows_dir,
        )
        if errors:
            for err in errors:
                logger.error("%s", err)
            return 1
        print("Inverted-index WorkflowTrigger check OK")
        return 0

    written = generate_inverted_index_triggers(
        config_path=config_path,
        workflows_dir=workflows_dir,
        overwrite=args.force,
    )
    if not written and not args.force:
        logger.warning("No files written (existing triggers present; use --force)")
    print(f"Wrote {len(written)} WorkflowTrigger file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
