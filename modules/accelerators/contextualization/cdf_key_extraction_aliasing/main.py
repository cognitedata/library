"""
Main Entry Point - Fetch CDF instances from data model views, run key extraction and aliasing, write results.

Reads CDF credentials from environment (.env supported), queries instances from configured views,
runs the key extraction engine followed by the aliasing engine, and writes
JSON results into the tests/results/ directory (relative to this package).
"""

import argparse
import logging
import sys
from pathlib import Path

# Bootstrap ``sys.path`` so ``local_runner`` imports work when not run as ``python -m ...``.
_PACKAGE_ROOT = Path(__file__).resolve().parent
_REPO_ROOT = _PACKAGE_ROOT.parent.parent.parent.parent
for _p in (_REPO_ROOT, _PACKAGE_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from local_runner.paths import ensure_repo_on_path

ensure_repo_on_path()

try:
    from local_runner.client import create_cognite_client
    from local_runner.config_loading import load_configs
    from local_runner.env import load_env
    from local_runner.report import generate_report as _generate_report
    from local_runner.run import run_pipeline

    MODULES_AVAILABLE = True
except ImportError as e:
    print(f"Import error: {e}")
    MODULES_AVAILABLE = False
    create_cognite_client = None  # type: ignore
    load_configs = None  # type: ignore
    load_env = None  # type: ignore
    _generate_report = None  # type: ignore
    run_pipeline = None  # type: ignore

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Fetch instances from CDF views, run extraction & aliasing, write results to tests/results/."""
    parser = argparse.ArgumentParser(
        description="Run key extraction + aliasing on CDF data model instances"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max instances per view (0 = no limit, fetch all). Default 0.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without persisting aliases to CDF (skip alias persistence step)",
    )
    parser.add_argument(
        "--write-foreign-keys",
        action="store_true",
        help="Persist extracted foreign key references to DM (requires foreign key write-back property)",
    )
    parser.add_argument(
        "--foreign-key-writeback-property",
        type=str,
        default=None,
        help="DM property for FK reference strings (e.g. references_found); overrides config/env",
    )
    parser.add_argument(
        "--instance-space",
        type=str,
        default=None,
        help="Only process source views with this instance_space (e.g. sp_enterprise_schema)",
    )
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if not MODULES_AVAILABLE:
        logger.error("Required modules not available.")
        sys.exit(1)

    load_env()

    try:
        client = create_cognite_client()
    except Exception as e:
        logger.error(f"Failed to create CogniteClient: {e}")
        sys.exit(1)

    try:
        (
            extraction_config,
            aliasing_config,
            source_views,
            alias_writeback_property,
            write_foreign_key_references,
            foreign_key_writeback_property,
        ) = load_configs(logger)
    except Exception as e:
        logger.error(f"Failed to load configs: {e}")
        sys.exit(1)

    if args.instance_space:
        source_views = [
            v
            for v in source_views
            if v.get("instance_space", "").strip() == args.instance_space.strip()
        ]
        if not source_views:
            logger.error(
                f"No source views found with instance_space={args.instance_space!r}. "
                "Check pipeline configs for matching instance_space."
            )
            sys.exit(1)
        logger.info(
            f"Filtered to {len(source_views)} view(s) with instance_space={args.instance_space!r}"
        )

    run_pipeline(
        args,
        logger,
        client,
        extraction_config,
        aliasing_config,
        source_views,
        alias_writeback_property,
        write_foreign_key_references,
        foreign_key_writeback_property,
    )


if __name__ == "__main__":
    main()
