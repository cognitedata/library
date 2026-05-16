#!/usr/bin/env python3
"""Restore helper: extract i18n from ui/dist bundle or regenerate from en.ts."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

MODULE = Path(__file__).resolve().parent.parent
EXTRACT = MODULE / "scripts/extract_i18n_locales.py"
GENERATE = MODULE / "scripts/generate_i18n_locales.py"
BUNDLE_DIR = MODULE / "ui/dist/assets"


def main() -> None:
    js_files = sorted(BUNDLE_DIR.glob("index-*.js")) if BUNDLE_DIR.is_dir() else []
    if js_files:
        subprocess.check_call([sys.executable, str(EXTRACT), str(js_files[0])])
        return
    if GENERATE.is_file():
        subprocess.check_call([sys.executable, str(GENERATE)])
        return
    raise SystemExit("Missing scripts/generate_i18n_locales.py")


if __name__ == "__main__":
    main()
