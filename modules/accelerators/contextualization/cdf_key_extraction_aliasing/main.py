"""
Main entry point — fetch CDF instances from data model views, run key extraction and aliasing, write results.

Configuration: by default loads ``config/scopes/<scope>/key_extraction_aliasing.yaml`` (``--scope``), or
``--config-path`` to a v1 scope document. See ``config/README.md`` and ``scope_hierarchy.yaml`` /
``scripts/build_scopes.py`` for generating per-leaf scope folders.

Reads CDF credentials from environment (.env supported), queries instances from configured views,
runs the key extraction engine followed by the aliasing engine, and writes JSON results under
``tests/results/`` (relative to this package).
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

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


def _source_view_matches_instance_space(view: Dict[str, Any], wanted: str) -> bool:
    """Match ``instance_space`` field or a node ``space`` filter (EQUALS / IN)."""
    wanted_s = wanted.strip()
    if (view.get("instance_space") or "").strip() == wanted_s:
        return True
    for f in view.get("filters") or []:
        if str(f.get("property_scope", "view")).lower() != "node":
            continue
        if f.get("target_property") != "space":
            continue
        op = str(f.get("operator", "")).upper()
        vals = f.get("values")
        if op == "EQUALS":
            vs: List[Any]
            if isinstance(vals, list):
                vs = vals
            elif vals is None:
                continue
            else:
                vs = [vals]
            if any(str(x).strip() == wanted_s for x in vs if x is not None):
                return True
        elif op == "IN" and isinstance(vals, list):
            if wanted_s in {str(x).strip() for x in vals}:
                return True
    return False


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
        help=(
            "Only process source views whose instance_space matches, or whose filters "
            "include property_scope: node / target_property: space (EQUALS or IN) for this space"
        ),
    )
    parser.add_argument(
        "--scope",
        type=str,
        default=None,
        help="Load config/scopes/<scope>/key_extraction_aliasing.yaml. Ignored if --config-path is set.",
    )
    parser.add_argument(
        "--config-path",
        type=str,
        default=None,
        help="Path to a v1 scope YAML document (overrides --scope).",
    )
    parser.add_argument(
        "--process-all",
        action="store_true",
        help=(
            "When key_extraction.parameters.incremental_change_processing is enabled, "
            "passes process_all to the local runner (full scope rescan; same semantics as "
            "workflow input process_all). No effect if incremental mode is off."
        ),
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
        ) = load_configs(
            logger,
            scope=args.scope,
            config_path=args.config_path,
        )
    except Exception as e:
        logger.error(f"Failed to load configs: {e}")
        sys.exit(1)

    if args.instance_space:
        source_views = [
            v
            for v in source_views
            if _source_view_matches_instance_space(v, args.instance_space)
        ]
        if not source_views:
            logger.error(
                f"No source views found matching instance_space={args.instance_space!r} "
                "(field or node space filter). Check pipeline configs."
            )
            sys.exit(1)
        logger.info(
            f"Filtered to {len(source_views)} view(s) with instance_space={args.instance_space!r}"
        )

    scope_yaml_path: Optional[Path] = None
    if args.config_path:
        scope_yaml_path = Path(args.config_path).expanduser().resolve()
    else:
        sc = (args.scope or "default").strip() or "default"
        scope_yaml_path = (
            _PACKAGE_ROOT / "config" / "scopes" / sc / "key_extraction_aliasing.yaml"
        ).resolve()

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
        scope_yaml_path=scope_yaml_path,
    )


if __name__ == "__main__":
    main()
