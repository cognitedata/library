#!/usr/bin/env python3
"""Merge missing message keys from en.ts into other locale files (keeps existing translations)."""

from __future__ import annotations

import argparse
from pathlib import Path

from i18n_parse import merge_locale, parse_messages

_MODULE_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("i18n_dir", type=Path, nargs="?", default=None)
    p.add_argument(
        "--module-root",
        type=Path,
        default=_MODULE_ROOT,
        help="Accelerator module root (uses ui/src/i18n)",
    )
    p.add_argument(
        "--import-line",
        default='import type { Messages } from "./types";',
    )
    p.add_argument(
        "--partial",
        action="store_true",
        help="Use LocaleMessages import (file asset source style)",
    )
    args = p.parse_args()
    if args.module_root:
        i18n_dir = args.module_root.resolve() / "ui" / "src" / "i18n"
    elif args.i18n_dir:
        i18n_dir = args.i18n_dir.resolve()
    else:
        raise SystemExit("Provide i18n_dir or --module-root")
    imp = args.import_line
    if args.partial:
        imp = 'import type { LocaleMessages } from "./types";'
    locales = [
        f.stem
        for f in sorted(i18n_dir.glob("*.ts"))
        if f.stem not in ("en", "types", "index")
    ]
    for loc in locales:
        n = merge_locale(i18n_dir, loc, imp, loc, None)
        print(f"{loc}: +{n} keys ({len(parse_messages(i18n_dir / f'{loc}.ts'))} total)")


if __name__ == "__main__":
    main()
