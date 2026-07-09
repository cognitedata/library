#!/usr/bin/env python3
"""Prefix-merge inverted index en.ts keys into cdf_discovery i18n."""

from __future__ import annotations

import re
import sys
from pathlib import Path

MODULE_ROOT = Path(__file__).resolve().parent.parent
SRC = MODULE_ROOT.parent / "cdf_inverted_index_contextualization" / "ui" / "src" / "i18n" / "en.ts"
DST_EN = MODULE_ROOT / "ui" / "src" / "i18n" / "en.ts"
TYPES = MODULE_ROOT / "ui" / "src" / "i18n" / "types.ts"
LOCALES = ["ar", "de", "es", "fr", "hi", "bn", "ja", "nb", "pt", "zh"]


def extract_pairs(text: str) -> list[tuple[str, str]]:
    return re.findall(r'"([^"]+)":\s*"((?:[^"\\]|\\.)*)"', text)


def escape_ts(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def main() -> int:
    if not SRC.is_file():
        print(f"Missing source: {SRC}", file=sys.stderr)
        return 1
    pairs = extract_pairs(SRC.read_text(encoding="utf-8"))
    prefixed = {f"invertedIndex.{k}": v for k, v in pairs}

    # en.ts — skip if already merged
    en = DST_EN.read_text(encoding="utf-8")
    if '"invertedIndex.a11y.navTreeLabel"' not in en:
        lines = [f'  "{k}": "{escape_ts(v)}",' for k, v in sorted(prefixed.items())]
        idx = en.rfind("};")
        en = en[:idx].rstrip().rstrip(",") + ",\n" + "\n".join(lines) + "\n};\n"
        DST_EN.write_text(en, encoding="utf-8")

    # types.ts MessageKey union
    types_text = TYPES.read_text(encoding="utf-8")
    if "invertedIndex.a11y.navTreeLabel" not in types_text:
        new_keys = "\n".join(f'  | "invertedIndex.{k}"' for k, _ in pairs)
        types_text = types_text.replace(
            '  | "tree.desc.index";',
            '  | "tree.desc.index"\n' + new_keys + ";",
            1,
        )
        TYPES.write_text(types_text, encoding="utf-8")

    # Other locale files — English fallback for inverted index keys
    for loc in LOCALES:
        path = MODULE_ROOT / "ui" / "src" / "i18n" / f"{loc}.ts"
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        if '"invertedIndex.a11y.navTreeLabel"' in text:
            continue
        if '"tree.desc.index"' not in text:
            text = text.replace(
                '"tree.desc.monitor":',
                '"tree.desc.index": "Indexing",\n  "tree.desc.monitor":',
                1,
            )
        lines = [f'  "{k}": "{escape_ts(v)}",' for k, v in sorted(prefixed.items())]
        idx = text.rfind("};")
        text = text[:idx].rstrip().rstrip(",") + ",\n" + "\n".join(lines) + "\n};\n"
        path.write_text(text, encoding="utf-8")

    print(f"Merged {len(prefixed)} inverted index keys")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
