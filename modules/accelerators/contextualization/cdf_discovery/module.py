"""
CDF Discovery — local read-only browser for Classic, Data Modeling, and RAW.

  python module.py ui [--api-host HOST] [--api-port PORT] [--vite-port PORT] [--no-browser]
  python module.py build [--config default.config.yaml] [--dry-run] [--force]
  python module.py build --clean [--yes]
  python module.py build --check-generated

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

    env = {**os.environ, "PYTHONPATH": str(_MODULE_ROOT)}
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


def _run_build(argv: List[str]) -> int:
    from governance_build.orchestrate import run as governance_run  # noqa: WPS433

    scripts = _MODULE_ROOT / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    declared_default = (_MODULE_ROOT / "governance").resolve()
    if not os.environ.get("CDF_DISCOVERY_GOVERNANCE_ROOT"):
        os.environ.setdefault("CDF_DISCOVERY_GOVERNANCE_ROOT", str(declared_default))
    if "--module-root" not in argv:
        argv = ["--module-root", os.environ["CDF_DISCOVERY_GOVERNANCE_ROOT"], *argv]
    code = int(governance_run(argv))
    if code != 0 or "--dry-run" in argv or "--check-generated" in argv or "--clean" in argv:
        return code
    return _run_compliance_gates()


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print(__doc__, file=sys.stderr)
        raise SystemExit(2)
    if args[0] == "ui":
        raise SystemExit(_run_ui(args[1:]))
    if args[0] == "build":
        raise SystemExit(_run_build(args[1:]))
    print(f"Unknown command: {args[0]}\n{__doc__}", file=sys.stderr)
    raise SystemExit(2)


if __name__ == "__main__":
    main()
