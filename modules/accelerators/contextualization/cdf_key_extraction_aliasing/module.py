"""
Local CLI entry point — fetch CDF instances from data model views, run key discovery and aliasing, write results.

**Invoke the pipeline with** ``python module.py run`` (not ``module.py`` alone; bare ``module.py`` prints help).

Configuration: ``run`` loads the v1 scope document ``workflow.local.config.yaml`` at the
module root when ``--scope default`` (the default); other scope names require ``--config-path``.
Incremental workflow-parity runs use ``key_extraction.config.parameters`` the same way as deployed
workflows: optional ``key_discovery_instance_space`` / ``workflow_scope`` for Key Discovery FDM state
(watermark and hash use RAW automatically if the views are not deployed).
CDF workflows use the same v1 shape via ``workflow.input.configuration`` on each task (built by
``scripts/build_scopes.py`` into ``workflows/`` from templates in ``workflow_template/``). Create **missing** workflow artifacts with
``python module.py build`` (same CLI as ``scripts/build_scopes.py``; legacy ``python module.py --build`` is accepted).
Respects ``scope_build_mode``; does not overwrite existing files; pass ``--dry-run``, ``--check-workflow-triggers``, etc. Remove generated
workflow YAML with ``python module.py build --clean`` (confirmation or ``--yes``; no rebuild after delete—run
``build`` again to recreate). This is unrelated to ``run --clean-state``, which drops RAW tables. See
``config/README.md`` and ``default.config.yaml``.

Reads CDF credentials from environment (.env supported) for ``run``, queries instances from configured views,
runs the key extraction engine followed by the aliasing engine, and writes JSON results under
``tests/results/`` (relative to this package). Use ``run --clean-state`` / ``run --clean-state-only`` to drop
incremental RAW state tables from the scope YAML (not data-model alias/FK properties).
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
    from local_runner.config_loading import load_configs, resolve_scope_document_path
    from local_runner.env import load_env
    from local_runner.report import generate_report as _generate_report
    from local_runner.run import run_pipeline

    MODULES_AVAILABLE = True
except ImportError as e:
    print(f"Import error: {e}")
    MODULES_AVAILABLE = False
    create_cognite_client = None  # type: ignore
    load_configs = None  # type: ignore
    resolve_scope_document_path = None  # type: ignore
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


def _run_scope_build(build_argv: List[str]) -> int:
    """Run ``scripts/scope_build`` orchestrator (same CLI as ``scripts/build_scopes.py``)."""
    scripts_dir = _PACKAGE_ROOT / "scripts"
    sd = str(scripts_dir)
    if sd not in sys.path:
        sys.path.insert(0, sd)
    from scope_build.orchestrate import main as scope_build_main

    return int(scope_build_main(build_argv))


def _add_run_arguments(p: argparse.ArgumentParser) -> None:
    """CLI flags for ``module.py run``."""
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max instances per view (0 = no limit, fetch all). Default 0.",
    )
    p.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    p.add_argument(
        "--progress-every",
        type=int,
        default=0,
        metavar="N",
        help=(
            "Log long-running steps every N items (0 = off). Non-incremental: extraction/aliasing "
            "per view. Incremental (workflow parity): reference-index entities with refs + tag "
            "aliasing. Example: --progress-every 100"
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without persisting aliases to CDF (skip alias persistence step)",
    )
    p.add_argument(
        "--write-foreign-keys",
        action="store_true",
        help="Persist extracted foreign key references to DM (requires write-back property)",
    )
    p.add_argument(
        "--foreign-key-writeback-property",
        type=str,
        default=None,
        help="DM property for FK reference strings (e.g. references_found); overrides config/env",
    )
    p.add_argument(
        "--instance-space",
        type=str,
        default=None,
        help=(
            "Only process source views whose instance_space matches, or whose filters "
            "include property_scope: node / target_property: space (EQUALS or IN) for this space"
        ),
    )
    p.add_argument(
        "--scope",
        type=str,
        default=None,
        help=(
            "Scope label for resolving the v1 scope YAML. Only 'default' is supported without "
            "--config-path (loads module-root workflow.local.config.yaml). "
            "Ignored if --config-path is set."
        ),
    )
    p.add_argument(
        "--config-path",
        type=str,
        default=None,
        help="Path to a v1 scope YAML document (overrides --scope).",
    )
    p.add_argument(
        "--full-rescan",
        action="store_true",
        help=(
            "When key_extraction.parameters.incremental_change_processing is enabled, "
            "passes full_rescan to the local runner (full scope rescan; same semantics as "
            "workflow input full_rescan). No effect if incremental mode is off."
        ),
    )
    p.add_argument(
        "--skip-reference-index",
        action="store_true",
        help=(
            "In incremental/workflow-parity mode, skip fn_dm_reference_index even when "
            "key_extraction.config.parameters.enable_reference_index is true. "
            "When the scope flag is false (default), the reference index step is skipped regardless. "
            "No effect on non-incremental runs (reference index is not available there)."
        ),
    )
    clean_group = p.add_mutually_exclusive_group()
    clean_group.add_argument(
        "--clean-state",
        action="store_true",
        help=(
            "Delete RAW state tables for this scope (key extraction, reference index, aliasing state/aliases) "
            "then run the pipeline. With incremental mode, combine with --full-rescan for a full reprocess."
        ),
    )
    clean_group.add_argument(
        "--clean-state-only",
        action="store_true",
        help=(
            "Delete RAW state tables for this scope only; exit without running extraction/aliasing."
        ),
    )


def _print_root_cli_help() -> None:
    """Top-level help (``module.py`` / ``module.py -h``): lists ``run`` and ``build``."""
    parser = argparse.ArgumentParser(
        prog="module.py",
        description=(
            "cdf_key_extraction_aliasing local CLI. Use ``module.py run`` for the pipeline; "
            "``module.py build`` for workflow YAML generation. Invoking ``module.py`` with no "
            "subcommand prints this help."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python module.py run --dry-run\n"
            "  python module.py run --scope default --limit 50\n"
            "  python module.py build\n"
            "  python module.py build --check-workflow-triggers\n"
            "Legacy: ``python module.py --build`` is equivalent to ``python module.py build``."
        ),
    )
    sub = parser.add_subparsers(title="commands", metavar="COMMAND", dest="command")
    sub.add_parser(
        "run",
        help="Run key discovery + aliasing on CDF data model instances (writes tests/results/*.json)",
    )
    sub.add_parser(
        "build",
        help="Generate workflow YAML from default.config.yaml (same as scripts/build_scopes.py)",
    )
    parser.print_help()


def cmd_run(args: argparse.Namespace) -> None:
    """Fetch instances from CDF views, run key discovery & aliasing, write results to tests/results/."""
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
        scope_yaml_path = resolve_scope_document_path(sc).resolve()

    if args.clean_state or args.clean_state_only:
        from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.clean_state_tables import (
            clean_state_tables_from_scope_yaml,
        )

        cleaned = clean_state_tables_from_scope_yaml(client, logger, scope_yaml_path)
        if cleaned:
            logger.info("Cleaned %s RAW table(s).", len(cleaned))
        if args.clean_state_only:
            logger.info("Exiting after --clean-state-only (no pipeline run).")
            sys.exit(0)

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


def main() -> None:
    argv = list(sys.argv[1:])
    # Legacy: `module.py --build ...` → `module.py build ...`
    if argv and argv[0] == "--build":
        argv = ["build"] + argv[1:]

    if not argv or (len(argv) == 1 and argv[0] in ("-h", "--help")):
        _print_root_cli_help()
        sys.exit(0)

    if argv[0] == "build":
        raise SystemExit(_run_scope_build(argv[1:]))

    if argv[0] != "run":
        print(f"module.py: error: unknown command {argv[0]!r}", file=sys.stderr)
        _print_root_cli_help()
        sys.exit(2)

    run_parser = argparse.ArgumentParser(
        prog="module.py run",
        description="Fetch instances from CDF views, run key discovery & aliasing.",
    )
    _add_run_arguments(run_parser)
    args = run_parser.parse_args(argv[1:])
    cmd_run(args)


if __name__ == "__main__":
    main()
