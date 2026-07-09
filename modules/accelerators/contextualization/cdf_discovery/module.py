"""
CDF Discovery — local read-only browser for Classic, Data Modeling, and RAW.

  python module.py ui [--api-host HOST] [--api-port PORT] [--vite-port PORT] [--no-browser]
  python module.py build [--config default.config.yaml] [--dry-run] [--force]
  python module.py build --clean [--yes]
  python module.py build --check-generated
  python module.py build --build-inverted-index-triggers [--force]
  python module.py build --check-inverted-index-triggers
  python module.py transform build [--pipeline ID | --template ID] [--scoped] [--dry-run]
  python module.py transform run [--instance ID | --template ID] [--dry-run] [--predecessor-mode in_memory|cohort]
  python module.py transform deploy-scope [--dry-run]
  python module.py index {build-metadata|query|target-driven|…}  (inverted index CLI)

Reads CDF credentials from the repository root ``.env`` (same variables as cdf_discovery_aliasing).
"""

from __future__ import annotations

import argparse
import atexit
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
from typing import List

_MODULE_ROOT = Path(__file__).resolve().parent
_UI_DIR = _MODULE_ROOT / "ui"
_REPO_ROOT = _MODULE_ROOT.parent.parent.parent.parent

for _p in (_REPO_ROOT, _MODULE_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from shared.python.paths import (  # noqa: E402
    ensure_sys_path,
    governance_root,
    inverted_index_root,
    module_root,
    submodules_root,
    transform_root,
)


def _wait_for_http(url: str, *, timeout_sec: float = 45.0, poll_interval: float = 0.25) -> bool:
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
    p = argparse.ArgumentParser(
        prog="module.py ui",
        description="Host the CDF Discovery operator UI (FastAPI + Vite).",
    )
    p.add_argument("--api-host", default="127.0.0.1", help="Bind address for FastAPI")
    p.add_argument(
        "--api-port",
        type=int,
        default=8785,
        help="Port for FastAPI (default 8785)",
    )
    p.add_argument(
        "--vite-port",
        type=int,
        default=5193,
        help="Port for Vite dev server (default 5193)",
    )
    p.add_argument("--no-browser", action="store_true", help="Do not open a browser tab")
    p.add_argument("--no-reload", action="store_true", help="Disable uvicorn --reload")
    args = p.parse_args(argv)

    if not shutil.which("npm"):
        print("npm not found on PATH; install Node.js.", file=sys.stderr)
        return 1
    if not (_UI_DIR / "package.json").is_file():
        print(f"Missing {_UI_DIR / 'package.json'}", file=sys.stderr)
        return 1
    if not (_UI_DIR / "node_modules").is_dir():
        print("Installing UI dependencies (npm install)…")
        r = subprocess.run(["npm", "install"], cwd=str(_UI_DIR), check=False)
        if r.returncode != 0:
            return r.returncode

    idx_root = inverted_index_root()
    env = {
        **os.environ,
        "PYTHONPATH": str(_MODULE_ROOT),
        "CDF_INVERTED_INDEX_ROOT": str(idx_root),
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
        "--log-level",
        "debug",
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
    procs.append(subprocess.Popen(api_cmd, cwd=str(_MODULE_ROOT), env=env))
    time.sleep(0.8)

    vite_env = {
        **os.environ,
        "VITE_API_PROXY": f"http://{args.api_host}:{args.api_port}",
    }
    vite_cmd = ["npm", "run", "dev", "--", "--host", "127.0.0.1", "--port", str(args.vite_port)]
    print(f"Starting Vite on http://127.0.0.1:{args.vite_port} …")
    procs.append(subprocess.Popen(vite_cmd, cwd=str(_UI_DIR), env=vite_env))

    ui_url = f"http://127.0.0.1:{args.vite_port}/"
    if _wait_for_http(ui_url):
        print(f"UI ready at {ui_url}")
        if not args.no_browser:
            webbrowser.open(ui_url)
    else:
        print(f"Timed out waiting for {ui_url}", file=sys.stderr)

    for pr in procs:
        pr.wait()
    return 0


def _run_compliance_gates() -> int:
    gates = _MODULE_ROOT / "scripts" / "run_module_compliance_gates.py"
    if not gates.is_file():
        return 0
    proc = subprocess.run(
        [sys.executable, str(gates), "--module-root", str(_MODULE_ROOT)],
        cwd=str(_MODULE_ROOT),
    )
    return int(proc.returncode or 0)


def _run_inverted_index_triggers(argv: List[str]) -> int:
    ensure_sys_path()
    scripts = _MODULE_ROOT / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    from inverted_index_build.orchestrate import main as trigger_build_main  # noqa: WPS433

    if "--module-root" not in argv:
        argv = ["--module-root", str(_MODULE_ROOT), *argv]
    return int(trigger_build_main(argv))


def _run_build(argv: List[str]) -> int:
    if "--check-inverted-index-triggers" in argv:
        return _run_inverted_index_triggers(
            ["--check-inverted-index-triggers", "--module-root", str(_MODULE_ROOT)]
        )

    if "--build-inverted-index-triggers" in argv:
        build_argv = ["--build-inverted-index-triggers", "--module-root", str(_MODULE_ROOT)]
        if "--force" in argv:
            build_argv.append("--force")
        code = _run_inverted_index_triggers(build_argv)
        if code != 0:
            return code
        argv = [a for a in argv if a not in ("--build-inverted-index-triggers", "--force")]
        if not argv:
            return _run_compliance_gates()

    ensure_sys_path()
    from governance_build.orchestrate import run as governance_run  # noqa: WPS433

    declared_default = governance_root().resolve()
    if not os.environ.get("CDF_DISCOVERY_GOVERNANCE_ROOT"):
        os.environ.setdefault("CDF_DISCOVERY_GOVERNANCE_ROOT", str(declared_default))
    if "--module-root" not in argv:
        argv = ["--module-root", os.environ["CDF_DISCOVERY_GOVERNANCE_ROOT"], *argv]
    code = int(governance_run(argv))
    if code != 0 or "--dry-run" in argv or "--check-generated" in argv or "--clean" in argv:
        return code
    return _run_compliance_gates()


def _run_transform_build(argv: List[str]) -> int:
    ensure_sys_path()
    from workflow_build.orchestrate import main as workflow_build_main

    if "--module-root" not in argv:
        argv = ["--module-root", str(_MODULE_ROOT), *argv]
    return int(workflow_build_main(argv))


def _run_transform_run(argv: List[str]) -> int:
    from ui.server.etl_syspath import prepare_etl_local_runner

    prepare_etl_local_runner(_MODULE_ROOT)
    from local_runner.run import main as local_run_main

    return int(local_run_main(argv))


def _run_transform_deploy_scope(argv: List[str]) -> int:
    ensure_sys_path()
    from deploy_scope import main as deploy_scope_main

    tr = transform_root()
    if "--module-root" not in argv:
        argv = ["--module-root", str(tr), *argv]
    return int(deploy_scope_main(argv))


def _run_index(argv: List[str]) -> int:
    """Run inverted-index CLI from ``submodules/inverted_index/cli.py``."""
    if argv and argv[0] == "ui":
        print(
            "The standalone inverted index UI was removed. "
            "Use: python module.py ui  (from cdf_discovery)",
            file=sys.stderr,
        )
        return 2
    ensure_sys_path()
    idx_root = inverted_index_root()
    os.environ.setdefault("CDF_INVERTED_INDEX_ROOT", str(idx_root))
    from inverted_index.cli import main as index_cli_main  # noqa: WPS433

    return int(index_cli_main(argv))


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print(__doc__, file=sys.stderr)
        raise SystemExit(2)
    if args[0] == "ui":
        raise SystemExit(_run_ui(args[1:]))
    if args[0] == "build":
        raise SystemExit(_run_build(args[1:]))
    if args[0] == "transform":
        if len(args) < 2:
            print("Usage: module.py transform {build|run|deploy-scope} …", file=sys.stderr)
            raise SystemExit(2)
        sub = args[1]
        sub_argv = args[2:]
        if sub == "build":
            raise SystemExit(_run_transform_build(sub_argv))
        if sub == "run":
            raise SystemExit(_run_transform_run(sub_argv))
        if sub == "deploy-scope":
            raise SystemExit(_run_transform_deploy_scope(sub_argv))
        print(f"Unknown transform subcommand: {sub}", file=sys.stderr)
        raise SystemExit(2)
    if args[0] == "index":
        raise SystemExit(_run_index(args[1:]))
    print(f"Unknown command: {args[0]}\n{__doc__}", file=sys.stderr)
    raise SystemExit(2)


if __name__ == "__main__":
    main()
