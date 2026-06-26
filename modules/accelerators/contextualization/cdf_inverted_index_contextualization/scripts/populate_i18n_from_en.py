#!/usr/bin/env python3
"""Copy missing keys from en.ts into locale files."""

from __future__ import annotations

from pathlib import Path

from i18n_parse import merge_locale, parse_messages, write_messages


def main() -> None:
    module_root = Path(__file__).resolve().parent.parent
    i18n = module_root / "ui" / "src" / "i18n"
    en = parse_messages(i18n / "en.ts")
    import_line = 'import type { LocaleMessages } from "./types";'
    for loc in ["ar", "de", "es", "fr", "hi", "bn", "ja", "nb", "pt", "zh"]:
        loc_path = i18n / f"{loc}.ts"
        existing = parse_messages(loc_path) if loc_path.is_file() else {}
        merged = {k: existing.get(k, v) for k, v in en.items()}
        write_messages(loc_path, loc, merged, import_line)
        print(f"{loc}: synced {len(merged)} keys")


if __name__ == "__main__":
    main()
