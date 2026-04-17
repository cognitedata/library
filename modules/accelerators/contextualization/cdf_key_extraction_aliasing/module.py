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
Use ``python module.py copy-workflow-config`` to copy ``input.configuration`` between leaf WorkflowTrigger YAML files.
Respects ``scope_build_mode``; does not overwrite existing files; pass ``--dry-run``, ``--check-workflow-triggers``, etc. Remove generated
workflow YAML with ``python module.py build --clean`` (confirmation or ``--yes``; no rebuild after delete—run
``build`` again to recreate). This is unrelated to ``run --clean-state``, which drops RAW tables. See
``config/README.md`` and ``default.config.yaml``.

  python module.py ui [--api-host HOST] [--api-port PORT] [--vite-port PORT] [--no-browser]

Reads CDF credentials from environment (.env supported) for ``run``, queries instances from configured views,
runs the key extraction engine followed by the aliasing engine, and writes JSON results under
``tests/results/`` (relative to this package). Use ``run --clean-state`` / ``run --clean-state-only`` to drop
incremental RAW state tables from the scope YAML (not data-model alias/FK properties).
"""

import argparse
import atexit
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
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


def _run_copy_workflow_config(copy_argv: List[str]) -> int:
    """Copy ``input.configuration`` between leaf WorkflowTrigger.yaml files (see ``copy_workflow_config``)."""
    scripts_dir = _PACKAGE_ROOT / "scripts"
    sd = str(scripts_dir)
    if sd not in sys.path:
        sys.path.insert(0, sd)
    from scope_build.copy_workflow_config import main as copy_workflow_config_main

    return int(copy_workflow_config_main(copy_argv))


_UI_DIR = _PACKAGE_ROOT / "ui"


def _wait_for_http(url: str, *, timeout_sec: float = 45.0, poll_interval: float = 0.25) -> bool:
    """Return True once ``url`` responds with a 2xx/3xx status; False on timeout."""
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if 200 <= resp.status < 400:
                    return True
        except (urllib.error.URLError, OSError, TimeoutError):
            pass
        time.sleep(poll_interval)
    return False


def _run_ui(argv: List[str]) -> int:
    """Start FastAPI (operator API) and Vite dev server; open browser unless --no-browser."""
    p = argparse.ArgumentParser(
        prog="module.py ui",
        description="Host key discovery & aliasing operator UI.",
    )
    p.add_argument(
        "--api-host", default="127.0.0.1", help="Bind address for FastAPI (default 127.0.0.1)"
    )
    p.add_argument("--api-port", type=int, default=8765, help="Port for FastAPI (default 8765)")
    p.add_argument("--vite-port", type=int, default=5173, help="Port for Vite dev server (default 5173)")
    p.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not open a browser tab",
    )
    p.add_argument(
        "--no-reload",
        action="store_true",
        help="Disable uvicorn --reload (API only)",
    )
    args = p.parse_args(argv)

    if not shutil.which("npm"):
        print(
            "npm not found on PATH; install Node.js or use manual API + Vite steps.",
            file=sys.stderr,
        )
        return 1

    if not (_UI_DIR / "package.json").is_file():
        print(f"Missing {_UI_DIR / 'package.json'}", file=sys.stderr)
        return 1

    node_modules = _UI_DIR / "node_modules"
    if not node_modules.is_dir():
        print("Installing UI dependencies (npm install)…")
        r = subprocess.run(
            ["npm", "install"],
            cwd=str(_UI_DIR),
            check=False,
        )
        if r.returncode != 0:
            return r.returncode

    env = {**os.environ, "PYTHONPATH": str(_PACKAGE_ROOT)}
    api_cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "ui.server.main:app",
        "--host",
        args.api_host,
        "--port",
        str(args.api_port),
    ]
    if not args.no_reload:
        api_cmd.append("--reload")

    procs: List[subprocess.Popen] = []

    def _terminate_all() -> None:
        for pr in reversed(procs):
            if pr.poll() is None:
                pr.terminate()
        for pr in reversed(procs):
            try:
                pr.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pr.kill()

    atexit.register(_terminate_all)

    def _handle_sigint(_signum: int, _frame: object) -> None:
        _terminate_all()
        sys.exit(130)

    signal.signal(signal.SIGINT, _handle_sigint)

    print(f"Starting API on http://{args.api_host}:{args.api_port} …")
    procs.append(
        subprocess.Popen(
            api_cmd,
            cwd=str(_PACKAGE_ROOT),
            env=env,
        )
    )
    time.sleep(0.8)

    vite_url = f"http://{args.api_host}:{args.vite_port}/"
    print(f"Starting Vite on {vite_url} …")
    vite_env = {
        **os.environ,
        "VITE_API_PROXY": f"http://{args.api_host}:{args.api_port}",
    }
    procs.append(
        subprocess.Popen(
            ["npm", "run", "dev", "--", "--port", str(args.vite_port), "--host", args.api_host],
            cwd=str(_UI_DIR),
            env=vite_env,
        )
    )

    if not args.no_browser:
        print("Waiting for dev server…")
        if _wait_for_http(vite_url):
            webbrowser.open(vite_url)
        else:
            print(
                f"Timed out waiting for {vite_url}; open it manually when the dev server is ready.",
                file=sys.stderr,
            )

    print("Operator UI running — Ctrl+C to stop both servers.\n")
    try:
        code = procs[-1].wait()
    finally:
        _terminate_all()
    return code if code is not None else 1


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
            "  python module.py copy-workflow-config --from SITE_A --to SITE_B\n"
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
    sub.add_parser(
        "copy-workflow-config",
        help=(
            "Copy workflow input.configuration from one leaf WorkflowTrigger.yaml to another "
            "(reconciles destination scope ids, filters, scope block)"
        ),
    )
    sub.add_parser(
        "ui",
        help="Local operator UI (FastAPI + Vite) for default.config.yaml, scope YAML, and workflows/",
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

    if argv[0] == "copy-workflow-config":
        raise SystemExit(_run_copy_workflow_config(argv[1:]))

    if argv[0] == "ui":
        raise SystemExit(_run_ui(argv[1:]))

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
