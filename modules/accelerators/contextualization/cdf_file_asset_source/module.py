"""
Local CLI for cdf_file_asset_source — operator UI, validation, and pipeline runs.

  python module.py ui [--api-host HOST] [--api-port PORT] [--vite-port PORT] [--no-browser]
  python module.py validate [--step extract|create|write]
  python module.py build [--check] [--force]
  python module.py run [--step extract|create|write|all]

Reads CDF credentials from the repository root ``.env``.
"""

from __future__ import annotations

import argparse
import atexit
import json
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

_PACKAGE_ROOT = Path(__file__).resolve().parent
_UI_DIR = _PACKAGE_ROOT / "ui"
_REPO_ROOT = _PACKAGE_ROOT.parent.parent.parent.parent

for _p in (_REPO_ROOT, _PACKAGE_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))


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


def _module_pythonpath() -> str:
    parts = [str(_PACKAGE_ROOT / "functions"), str(_PACKAGE_ROOT), str(_REPO_ROOT)]
    joined = ":".join(parts)
    prev = (os.environ.get("PYTHONPATH") or "").strip()
    return f"{joined}:{prev}" if prev else joined


def _run_ui(argv: List[str]) -> int:
    p = argparse.ArgumentParser(
        prog="module.py ui",
        description="Host the file asset source operator UI (FastAPI + Vite).",
    )
    p.add_argument("--api-host", default="127.0.0.1", help="Bind address for FastAPI")
    p.add_argument("--api-port", type=int, default=8770, help="Port for FastAPI (default 8770)")
    p.add_argument("--vite-port", type=int, default=5188, help="Port for Vite (default 5188)")
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

    env = {**os.environ, "PYTHONPATH": _module_pythonpath()}
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
    procs.append(subprocess.Popen(api_cmd, cwd=str(_PACKAGE_ROOT), env=env))
    time.sleep(0.8)

    api_base = f"http://{args.api_host}:{args.api_port}"
    vite_env = {
        **os.environ,
        "VITE_API_PROXY": api_base,
        "VITE_API_BASE": api_base,
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


def _run_validate(argv: List[str]) -> int:
    p = argparse.ArgumentParser(prog="module.py validate")
    p.add_argument(
        "--step",
        action="append",
        dest="steps",
        choices=("extract", "create", "write"),
        help="Validate only these steps (repeatable)",
    )
    args = p.parse_args(argv)

    from local_runner.validate import validate_default_config

    out = validate_default_config(args.steps)
    print(json.dumps(out, indent=2))
    if not out["valid"]:
        return 1
    gates = Path(__file__).resolve().parent / "scripts" / "run_module_compliance_gates.py"
    if gates.is_file():
        import subprocess

        proc = subprocess.run(
            [sys.executable, str(gates), "--module-root", str(Path(__file__).resolve().parent)],
            cwd=str(Path(__file__).resolve().parent),
        )
        if proc.returncode != 0:
            return int(proc.returncode or 1)
    return 0


def _run_build(argv: List[str]) -> int:
    p = argparse.ArgumentParser(
        prog="module.py build",
        description="Sync workflow trigger input.configuration from default.config.yaml (file_asset_source + scope_hierarchy).",
    )
    p.add_argument(
        "--check",
        action="store_true",
        help="Exit 1 if trigger configuration drifts from default.config.yaml",
    )
    p.add_argument("--force", action="store_true", help="Rewrite trigger even when unchanged")
    args = p.parse_args(argv)

    from local_runner.build_trigger import sync_workflow_trigger

    out = sync_workflow_trigger(check_only=args.check, force=args.force)
    print(out.get("message", out))
    return 0 if out.get("ok") else 1


def _run_pipeline(argv: List[str]) -> int:
    p = argparse.ArgumentParser(prog="module.py run")
    p.add_argument(
        "--step",
        choices=("extract", "create", "write", "all"),
        default="all",
        help="Pipeline step to run (default: all)",
    )
    args = p.parse_args(argv)

    from local_runner.run import run_pipeline_step, run_pipeline_workflow

    if args.step == "all":
        out = run_pipeline_workflow()
    else:
        out = run_pipeline_step(args.step)
    print(json.dumps(out, indent=2, default=str))
    return 0 if out.get("succeeded") else 1


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print(__doc__, file=sys.stderr)
        raise SystemExit(2)
    cmd = args[0]
    rest = args[1:]
    if cmd == "ui":
        raise SystemExit(_run_ui(rest))
    if cmd == "validate":
        raise SystemExit(_run_validate(rest))
    if cmd == "build":
        raise SystemExit(_run_build(rest))
    if cmd == "run":
        raise SystemExit(_run_pipeline(rest))
    print(f"Unknown command: {cmd}\n{__doc__}", file=sys.stderr)
    raise SystemExit(2)


if __name__ == "__main__":
    main()
