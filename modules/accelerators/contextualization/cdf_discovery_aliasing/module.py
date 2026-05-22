"""
Local CLI entry point — run discovery data workflows against CDF and write local snapshots.

**Invoke the pipeline with** ``python module.py run`` (not ``module.py`` alone; bare ``module.py`` prints help).

Configuration: ``run`` loads the v1 scope document ``workflow.local.config.yaml`` at the
module root when ``--scope default`` (the default); other scope names require ``--config-path``.
The executable graph is built from ``canvas`` (``compile_workflow_dag: canvas`` or omitted)
into ``compiled_workflow`` IR—the same shape deployed workflows pass on ``workflow.input``.
``Workflow.yaml`` is derived from ``workflow_template/workflow.template.Workflow.yaml``; ``WorkflowVersion.yaml``
embeds per-task IR fields from the build-time canvas compile. Run ``python module.py build`` to create missing scoped **Workflow**,
**WorkflowVersion**, and **WorkflowTrigger** YAML. Refreshes ``workflow.execution.graph.yaml`` from IR on every run;
pass ``--force`` to overwrite existing scoped **Workflow** / **WorkflowVersion** / **WorkflowTrigger** (same CLI as ``scripts/build_scopes.py``).
Use ``python module.py copy-workflow-config`` to copy ``input.configuration`` between leaf WorkflowTrigger YAML files.
Use ``python module.py promote-local-templates`` to overwrite ``workflow_template/workflow.template.config.yaml``
from ``workflow.local.config.yaml`` (see ``--dry-run``).
Use ``python module.py build --scope-suffix <suffix>`` to limit writes to one leaf under ``workflows/<suffix>/`` (still refreshes ``workflow.execution.graph.yaml``; use ``--force`` to overwrite that leaf’s existing scoped YAML).
Use ``python module.py deploy-scope`` to upsert Workflow / WorkflowVersion / WorkflowTrigger to CDF via the Cognite SDK,
or ``python module.py cdf-workflow-run`` to run a deployed workflow (see ``scripts/deploy_scope_cdf.py`` and ``scripts/cdf_workflow_run.py``).
Optional ``scope_build_mode`` in hierarchy YAML must be ``full`` or omitted (``trigger_only`` is rejected). Pass ``--force`` to overwrite existing scoped workflow/trigger YAML, or ``--dry-run``, ``--check-workflow-triggers``, etc. Remove generated
workflow YAML with ``python module.py build --clean`` (confirmation or ``--yes``; no rebuild after delete—run
``build`` again to recreate). See ``config/README.md`` and ``default.config.yaml``.

  python module.py ui [--api-host HOST] [--api-port PORT] [--vite-port PORT] [--no-browser]

Reads CDF credentials from environment (.env supported) for ``run``, executes the compiled discovery DAG
(``fn_dm_*`` canvas tasks), and writes JSON under ``local_run_results/`` (relative to this package).
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
from typing import Any, Dict, List, Optional, Tuple

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
    from local_runner.config_loading import load_discovery_scope
    from local_runner.env import load_env
    from local_runner.report import generate_report as _generate_report
    from local_runner.run import run_pipeline

    MODULES_AVAILABLE = True
except ImportError as e:
    print(f"Import error: {e}")
    MODULES_AVAILABLE = False
    create_cognite_client = None  # type: ignore
    load_discovery_scope = None  # type: ignore
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


def _run_promote_local_templates(promote_argv: List[str]) -> int:
    """Copy ``workflow.local.*`` into ``workflow_template/workflow.template.*`` (see ``promote_local_workflow_templates``)."""
    scripts_dir = _PACKAGE_ROOT / "scripts"
    sd = str(scripts_dir)
    if sd not in sys.path:
        sys.path.insert(0, sd)
    from scope_build.promote_local_workflow_templates import main as promote_local_templates_main

    return int(promote_local_templates_main(promote_argv))


def _pythonpath_for_module_scripts() -> str:
    """``functions:scripts:<module>`` plus any existing ``PYTHONPATH`` (for deploy / workflow-run helpers)."""
    parts = [
        str(_PACKAGE_ROOT / "functions"),
        str(_PACKAGE_ROOT / "scripts"),
        str(_PACKAGE_ROOT),
    ]
    joined = ":".join(parts)
    prev = (os.environ.get("PYTHONPATH") or "").strip()
    return f"{joined}:{prev}" if prev else joined


def _run_cdf_helper_script(script_name: str, forward_argv: List[str]) -> int:
    """Run ``scripts/<script_name>`` with ``cwd`` at module root and a suitable ``PYTHONPATH``."""
    script = _PACKAGE_ROOT / "scripts" / script_name
    if not script.is_file():
        logger.error("Script not found: %s", script)
        return 1
    env = os.environ.copy()
    env["PYTHONPATH"] = _pythonpath_for_module_scripts()
    proc = subprocess.run(
        [sys.executable, str(script)] + forward_argv,
        cwd=str(_PACKAGE_ROOT),
        env=env,
    )
    return int(proc.returncode)


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
        description="Host the discovery operator UI (FastAPI + Vite).",
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

    # Match ``module.py build`` / pytest: ``functions`` + ``scripts`` packages live under the module root.
    _py_path = ":".join(
        str(p)
        for p in (
            _PACKAGE_ROOT / "functions",
            _PACKAGE_ROOT / "scripts",
            _PACKAGE_ROOT,
        )
    )
    _prev_py = (os.environ.get("PYTHONPATH") or "").strip()
    env = {
        **os.environ,
        "PYTHONPATH": f"{_py_path}:{_prev_py}" if _prev_py else _py_path,
    }
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
    p.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Passes through to task payloads where supported (e.g. save steps that honor dry_run); "
            "does not skip DAG execution."
        ),
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
        "--all",
        dest="run_all",
        action="store_true",
        help=(
            "Passes run_all=true on workflow-shaped task data (same as CDF workflow input). "
            "Interpreted by individual discovery handlers (e.g. cohort-wide RAW queries)."
        ),
    )
    p.add_argument(
        "--raw-results-rows",
        type=int,
        default=500,
        metavar="N",
        help=(
            "After the DAG, embed up to N rows per RAW table referenced in task summaries "
            "into local_run_results JSON (0 disables). Default 500."
        ),
    )
    p.add_argument(
        "--raw-results-max-tables",
        type=int,
        default=30,
        metavar="M",
        help="Max distinct RAW tables to sample for JSON (default 30). Ignored when --raw-results-rows 0.",
    )
    p.add_argument(
        "--raw-results-max-rows-scanned",
        type=int,
        default=0,
        metavar="K",
        help=(
            "Max RAW rows read per table when sampling for discovery_run.json raw_table_samples "
            "(0 = use env KEA_RAW_RESULTS_MAX_RAW_ROWS_SCANNED or default 100000). Stops early when "
            "reached; see raw_table_samples.tables[].raw_scan_truncated."
        ),
    )
    p.add_argument(
        "--clean-state",
        action="store_true",
        help=(
            "Before the DAG: baseline RAW purge (operator tables + all per-run node cohort "
            "tables). Does not remove aliases/indexKey already on DM instances."
        ),
    )
    p.add_argument(
        "--clean-state-only",
        action="store_true",
        help="Baseline RAW purge only, then exit (no pipeline).",
    )
    p.add_argument(
        "--local-task-retries",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Local runner only: CDF-style task retries per function (default from workflow "
            "policy, usually 3). Use 0 to disable retries. Overrides KEA_LOCAL_TASK_RETRIES."
        ),
    )


def _print_root_cli_help() -> None:
    """Top-level help (``module.py`` / ``module.py -h``): lists ``run`` and ``build``."""
    parser = argparse.ArgumentParser(
        prog="module.py",
        description=(
            "cdf_discovery_aliasing local CLI. Use ``module.py run`` for the pipeline; "
            "``module.py build`` for workflow YAML generation. Invoking ``module.py`` with no "
            "subcommand prints this help."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python module.py run --dry-run\n"
            "  python module.py run --scope default\n"
            "  python module.py build\n"
            "  python module.py build --scope-suffix site_01\n"
            "  python module.py build --check-workflow-triggers\n"
            "  python module.py deploy-scope --scope-suffix site_01\n"
            "  python module.py cdf-workflow-run --scope-suffix site_01 --dry-run\n"
            "  python module.py raw-purge-baseline --dry-run\n"
            "  python module.py raw-purge-baseline --yes\n"
            "  python module.py run --clean-state --all\n"
            "  python module.py raw-purge-truncate --dry-run\n"
            "  python module.py raw-purge-truncate --yes\n"
            "  python module.py copy-workflow-config --from SITE_A --to SITE_B\n"
            "  python module.py promote-local-templates --dry-run\n"
        ),
    )
    sub = parser.add_subparsers(title="commands", metavar="COMMAND", dest="command")
    sub.add_parser(
        "run",
        help="Run the discovery canvas workflow locally (writes local_run_results discovery snapshot + report)",
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
        "promote-local-templates",
        help=(
            "Overwrite workflow_template/workflow.template.config.yaml from workflow.local.config.yaml "
            "(unified scope including embedded canvas)"
        ),
    )
    sub.add_parser(
        "ui",
        help="Local operator UI (FastAPI + Vite) for default.config.yaml, scope YAML, and workflows/",
    )
    sub.add_parser(
        "deploy-scope",
        help=(
            "Validate one scope's workflow YAML, optionally run build --scope-suffix, then upsert Workflow, "
            "WorkflowVersion, and WorkflowTrigger to CDF via the Cognite SDK (see scripts/deploy_scope_cdf.py --help)"
        ),
    )
    sub.add_parser(
        "cdf-workflow-run",
        help="Start a CDF workflow execution for one scope and poll status (scripts/cdf_workflow_run.py --help)",
    )
    parser.print_help()


def _load_scope_for_purge(
    args: argparse.Namespace,
) -> Tuple[Any, Path, List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    """Shared scope load for destructive RAW purge commands."""
    load_env()
    try:
        client = create_cognite_client()
    except Exception as e:
        logger.error("Failed to create CogniteClient: %s", e)
        sys.exit(1)
    try:
        scope_yaml_path, source_views = load_discovery_scope(
            logger,
            scope=args.scope,
            config_path=args.config_path,
        )
    except Exception as e:
        logger.error("Failed to load scope: %s", e)
        sys.exit(1)
    from local_runner.workflow_payload import (
        compiled_workflow_for_merged_scope_document,
        merged_scope_document_for_local_run,
    )

    merged = merged_scope_document_for_local_run(scope_yaml_path.resolve(), source_views)
    cw = compiled_workflow_for_merged_scope_document(merged)
    return client, scope_yaml_path, source_views, merged, cw


def _add_raw_purge_truncate_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    p.add_argument(
        "--scope",
        type=str,
        default=None,
        help="Scope label for workflow.local.config.yaml (only 'default' without --config-path).",
    )
    p.add_argument(
        "--config-path",
        type=str,
        default=None,
        help="Path to a v1 scope YAML (overrides --scope).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print tables that would be truncated without calling CDF.",
    )
    p.add_argument(
        "--yes",
        action="store_true",
        help="Confirm destructive RAW table deletion (required unless --dry-run).",
    )
    p.add_argument(
        "--table",
        dest="tables",
        action="append",
        default=None,
        metavar="DB:TABLE",
        help="Override: truncate this RAW table (repeatable). Default: tables from scope + compiled IR.",
    )


def cmd_raw_purge_baseline(args: argparse.Namespace) -> None:
    """Truncate operator RAW tables and delete all per-run node cohort tables (destructive)."""
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if not MODULES_AVAILABLE:
        logger.error("Required modules not available.")
        sys.exit(1)
    if not args.dry_run and not args.yes:
        logger.error("Refusing baseline purge without --yes (or use --dry-run).")
        sys.exit(1)

    from cdf_fn_common.discovery_raw_purge import purge_discovery_raw_baseline

    client, _path, _views, merged, cw = _load_scope_for_purge(args)
    summary = purge_discovery_raw_baseline(client, merged, cw, dry_run=bool(args.dry_run))
    logger.info("Baseline purge result: %s", summary)
    op_tables = summary.get("operator_tables") or {}
    for row in op_tables.get("tables") or []:
        if row.get("error"):
            sys.exit(1)
    for block in summary.get("run_node_tables") or []:
        for row in block.get("tables") or []:
            if row.get("error"):
                sys.exit(1)


def cmd_raw_purge_truncate(args: argparse.Namespace) -> None:
    """Delete discovery RAW tables listed in scope (destructive)."""
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if not MODULES_AVAILABLE:
        logger.error("Required modules not available.")
        sys.exit(1)
    if not args.dry_run and not args.yes:
        logger.error("Refusing to truncate without --yes (or use --dry-run).")
        sys.exit(1)

    from cdf_fn_common.discovery_raw_purge import collect_discovery_raw_tables, truncate_raw_tables

    client, _path, _views, merged, cw = _load_scope_for_purge(args)
    if args.tables:
        tables: List[Tuple[str, str]] = []
        for spec in args.tables:
            if not spec or ":" not in spec:
                logger.error("Invalid --table %r (expected db:table)", spec)
                sys.exit(2)
            db, _, tbl = spec.partition(":")
            db, tbl = db.strip(), tbl.strip()
            if db and tbl:
                tables.append((db, tbl))
        tables = sorted(set(tables))
    else:
        tables = collect_discovery_raw_tables(merged, cw)
    if not tables:
        logger.error("No RAW tables resolved to truncate.")
        sys.exit(2)
    logger.info("Tables: %s", tables)
    summary = truncate_raw_tables(client, tables, dry_run=bool(args.dry_run))
    logger.info("Result: %s", summary)
    if any("error" in x for x in summary.get("tables") or []):
        sys.exit(1)


def cmd_run(args: argparse.Namespace) -> None:
    """Execute the discovery canvas DAG locally and write results under local_run_results/."""
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
        scope_yaml_path, source_views = load_discovery_scope(
            logger,
            scope=args.scope,
            config_path=args.config_path,
        )
    except Exception as e:
        logger.error(f"Failed to load scope: {e}")
        sys.exit(1)

    if args.instance_space and source_views:
        source_views = [
            v
            for v in source_views
            if _source_view_matches_instance_space(v, args.instance_space)
        ]
        if not source_views:
            logger.error(
                f"No source views found matching instance_space={args.instance_space!r} "
                "(field or node space filter). Add top-level source_views or adjust filters."
            )
            sys.exit(1)
        logger.info(
            f"Filtered to {len(source_views)} view(s) with instance_space={args.instance_space!r}"
        )

    scope_yaml_path = scope_yaml_path.resolve()

    if getattr(args, "clean_state", False) or getattr(args, "clean_state_only", False):
        from cdf_fn_common.discovery_raw_purge import purge_discovery_raw_baseline
        from local_runner.workflow_payload import (
            compiled_workflow_for_merged_scope_document,
            merged_scope_document_for_local_run,
        )

        merged = merged_scope_document_for_local_run(scope_yaml_path, source_views)
        cw = compiled_workflow_for_merged_scope_document(merged)
        logger.info("Purging discovery RAW baseline (operator + per-run node cohort tables)…")
        summary = purge_discovery_raw_baseline(client, merged, cw, dry_run=False)
        logger.info("Clean-state purge: %s", summary)
        if getattr(args, "clean_state_only", False):
            return

    run_pipeline(
        args,
        logger,
        client,
        source_views,
        scope_yaml_path=scope_yaml_path,
    )


def main() -> None:
    argv = list(sys.argv[1:])

    if not argv or (len(argv) == 1 and argv[0] in ("-h", "--help")):
        _print_root_cli_help()
        sys.exit(0)

    if argv[0] == "build":
        raise SystemExit(_run_scope_build(argv[1:]))

    if argv[0] == "copy-workflow-config":
        raise SystemExit(_run_copy_workflow_config(argv[1:]))

    if argv[0] == "promote-local-templates":
        raise SystemExit(_run_promote_local_templates(argv[1:]))

    if argv[0] == "ui":
        raise SystemExit(_run_ui(argv[1:]))

    if argv[0] == "deploy-scope":
        raise SystemExit(_run_cdf_helper_script("deploy_scope_cdf.py", argv[1:]))

    if argv[0] == "cdf-workflow-run":
        raise SystemExit(_run_cdf_helper_script("cdf_workflow_run.py", argv[1:]))

    if argv[0] in ("raw-purge-baseline", "raw-purge-truncate"):
        prog = f"module.py {argv[0]}"
        desc = (
            "Destructive baseline: truncate operator RAW tables and delete all "
            "discovery_state__{run}__{node} cohort tables."
            if argv[0] == "raw-purge-baseline"
            else "Destructive: delete discovery operator RAW tables from scope (not per-run node tables)."
        )
        p = argparse.ArgumentParser(prog=prog, description=desc)
        _add_raw_purge_truncate_arguments(p)
        ra = p.parse_args(argv[1:])
        if argv[0] == "raw-purge-baseline":
            cmd_raw_purge_baseline(ra)
        else:
            cmd_raw_purge_truncate(ra)
        raise SystemExit(0)

    if argv[0] != "run":
        print(f"module.py: error: unknown command {argv[0]!r}", file=sys.stderr)
        _print_root_cli_help()
        sys.exit(2)

    run_parser = argparse.ArgumentParser(
        prog="module.py run",
        description="Run the discovery canvas workflow (compiled_workflow DAG) against CDF.",
    )
    _add_run_arguments(run_parser)
    args = run_parser.parse_args(argv[1:])
    cmd_run(args)


if __name__ == "__main__":
    main()
