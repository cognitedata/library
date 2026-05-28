#!/usr/bin/env python3
"""Run convention + Toolkit placeholder checks for this module."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent
_MODULE_ROOT = _SCRIPTS.parent
CONVENTIONS = _SCRIPTS / "check_config_conventions.py"
PLACEHOLDERS = _SCRIPTS / "check_toolkit_placeholders.py"


def _run(script: Path, module_root: Path, *, extra_argv: list[str] | None = None) -> int:
    argv = [sys.executable, str(script), "--module-root", str(module_root)]
    if extra_argv:
        argv.extend(extra_argv)
    proc = subprocess.run(argv, cwd=str(module_root))
    return int(proc.returncode or 0)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--module-root", type=Path, default=_MODULE_ROOT)
    parser.add_argument("--skip-placeholders", action="store_true")
    args = parser.parse_args()
    root = args.module_root.resolve()
    if not (root / "module.py").is_file():
        raise SystemExit(f"Not an accelerator module root: {root}")

    code = _run(CONVENTIONS, root)
    if code != 0:
        raise SystemExit(code)

    if args.skip_placeholders:
        print("Module compliance gates OK (conventions only)")
        return

    has_toolkit = (root / "default.config.yaml").is_file() and (
        (root / "functions").is_dir() or (root / "workflows").is_dir() or (root / "data_sets").is_dir()
    )
    if has_toolkit:
        code = _run(PLACEHOLDERS, root)
        if code != 0:
            raise SystemExit(code)

    print("Module compliance gates OK")


if __name__ == "__main__":
    main()
