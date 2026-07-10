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
TRIGGER_ORCHESTRATE = _SCRIPTS / "inverted_index_build" / "orchestrate.py"


def _pythonpath_for(module_root: Path) -> str:
    import os

    root = module_root.resolve()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from shared.python.paths import prepare_sys_path  # noqa: WPS433

    parts = prepare_sys_path()
    existing = os.environ.get("PYTHONPATH", "")
    if existing:
        parts.append(existing)
    return os.pathsep.join(parts)


def _run(script: Path, module_root: Path, *, extra_argv: list[str] | None = None) -> int:
    import os

    argv = [sys.executable, str(script), "--module-root", str(module_root)]
    if extra_argv:
        argv.extend(extra_argv)
    env = os.environ.copy()
    env["PYTHONPATH"] = _pythonpath_for(module_root)
    proc = subprocess.run(argv, cwd=str(module_root), env=env)
    return int(proc.returncode or 0)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--module-root", type=Path, default=_MODULE_ROOT)
    parser.add_argument("--skip-placeholders", action="store_true")
    parser.add_argument("--skip-inverted-index-triggers", action="store_true")
    args = parser.parse_args()
    root = args.module_root.resolve()
    if not (root / "module.py").is_file():
        raise SystemExit(f"Not an accelerator module root: {root}")

    code = _run(CONVENTIONS, root)
    if code != 0:
        raise SystemExit(code)

    if not args.skip_inverted_index_triggers and TRIGGER_ORCHESTRATE.is_file():
        code = _run(
            TRIGGER_ORCHESTRATE,
            root,
            extra_argv=["--check-inverted-index-triggers"],
        )
        if code != 0:
            raise SystemExit(code)

    if args.skip_placeholders:
        print("Module compliance gates OK (conventions + inverted-index triggers)")
        return

    has_toolkit = (root / "default.config.yaml").is_file() and (
        (root / "submodules/transform/functions").is_dir() or (root / "workflows").is_dir() or (root / "data_sets").is_dir()
    )
    if has_toolkit:
        code = _run(PLACEHOLDERS, root)
        if code != 0:
            raise SystemExit(code)

    print("Module compliance gates OK")


if __name__ == "__main__":
    main()
