#!/usr/bin/env python3
"""Verify every MessageKey in en.ts exists in all locale files."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from i18n_parse import parse_messages


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--module-root", type=Path, required=True)
    args = parser.parse_args()
    i18n_dir = args.module_root.resolve() / "ui" / "src" / "i18n"
    en_path = i18n_dir / "en.ts"
    if not en_path.is_file():
        raise SystemExit(f"Missing {en_path}")
    en_keys = set(parse_messages(en_path).keys())
    types_path = i18n_dir / "types.ts"
    types_text = types_path.read_text(encoding="utf-8") if types_path.is_file() else ""
    declared = set(
        re.findall(
            r'"([^"]+)"',
            types_text.split("MessageKey")[1].split("export")[0]
            if "MessageKey" in types_text
            else "",
        )
    )
    missing_declared = en_keys - declared if declared else set()
    if missing_declared and declared:
        print(f"warn: {len(missing_declared)} en keys not in MessageKey union", file=sys.stderr)
    failed = False
    for loc_path in sorted(i18n_dir.glob("*.ts")):
        if loc_path.stem in ("en", "types", "index", "localeTermFixes"):
            continue
        loc_keys = set(parse_messages(loc_path).keys())
        missing = en_keys - loc_keys
        if missing:
            print(f"{loc_path.name}: missing {len(missing)} keys from en.ts", file=sys.stderr)
            failed = True
        extra = loc_keys - en_keys
        if extra:
            print(f"{loc_path.name}: warn {len(extra)} extra keys not in en.ts", file=sys.stderr)
    if failed:
        raise SystemExit(1)
    print(f"i18n OK ({len(en_keys)} keys, {len(list(i18n_dir.glob('*.ts'))) - 3} locales)")


if __name__ == "__main__":
    main()
