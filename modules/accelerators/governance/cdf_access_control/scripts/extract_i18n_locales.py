#!/usr/bin/env python3
"""Extract all locale message tables from a production ui/dist JS bundle into ui/src/i18n/*.ts."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

MODULE = Path(__file__).resolve().parent.parent
OUT = MODULE / "ui/src/i18n"

LOCALE_ANCHORS: dict[str, str] = {
    "en": '"app.title":"Access Control"',
    "es": '"app.title":"Control de acceso"',
    "de": '"app.title":"Zugriffskontrolle"',
    "fr": '"app.title":"Contrôle d\u2019accès"',  # typographic apostrophe
    "fr_alt": '"app.title":"Contrôle d\'accès"',
    "hi": '"app.title":"पहुँच नियंत्रण"',
    "ja": '"app.title":"アクセスコントロール"',
    "nb": '"app.title":"Tilgangskontroll"',
    "pt": '"app.title":"Controle de acesso"',
    "zh": '"app.title":"访问控制"',
    "ar": '"app.title":"التحكم في الوصول"',
}

PAIR_RE = re.compile(r'"((?:\\.|[^"\\])+)":"((?:\\.|[^"\\])*)"')


def extract_block(text: str, anchor: str) -> dict[str, str] | None:
    start = text.find(anchor)
    if start < 0:
        return None
    i = start - 1
    while i > 0 and text[i] != "{":
        i -= 1
    chunk = text[i:]
    pairs: list[tuple[str, str]] = []
    for m in PAIR_RE.finditer(chunk):
        k = json.loads(f'"{m.group(1)}"')
        v = json.loads(f'"{m.group(2)}"')
        if k == "app.title" and pairs and any(p[0] == "app.title" for p in pairs):
            break
        if "." in k:
            pairs.append((k, v))
    return dict(pairs)


def extract_all(text: str) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for loc, anchor in LOCALE_ANCHORS.items():
        if loc == "fr_alt":
            continue
        block = extract_block(text, anchor)
        if block is None and loc == "fr":
            block = extract_block(text, LOCALE_ANCHORS["fr_alt"])
        if block:
            out[loc] = block
    return out


def write_locale_files(locales: dict[str, dict[str, str]], *, keys_from: str = "en") -> None:
    base_keys = sorted(locales.get(keys_from, locales[next(iter(locales))]).keys())
    types = OUT / "types.ts"
    types.write_text(
        'export type Locale = "en" | "es" | "nb" | "ja" | "pt" | "fr" | "de" | "zh" | "hi" | "ar";\n\n'
        "export type Theme = \"light\" | \"dark\";\n\n"
        "export type MessageKey =\n"
        + "\n".join(f'  | "{k}"' for k in base_keys)
        + ";\n\n"
        "export type Messages = Record<MessageKey, string>;\n",
        encoding="utf-8",
    )
    for loc, msgs in sorted(locales.items()):
        lines = [
            'import type { Messages } from "./types";',
            "",
            f"export const {loc}: Messages = {{",
        ]
        for k in base_keys:
            val = msgs.get(k, locales[keys_from][k])
            lines.append(f"  {json.dumps(k)}: {json.dumps(val, ensure_ascii=False)},")
        lines.append("};")
        lines.append("")
        (OUT / f"{loc}.ts").write_text("\n".join(lines), encoding="utf-8")
        print(f"Wrote {loc}.ts ({len(base_keys)} keys)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "bundle",
        nargs="?",
        default=str(MODULE / "ui/dist/assets"),
        help="Path to index-*.js or assets directory",
    )
    args = ap.parse_args()
    path = Path(args.bundle)
    if path.is_dir():
        js_files = sorted(path.glob("index-*.js"))
        if not js_files:
            raise SystemExit(f"No index-*.js under {path}")
        path = js_files[0]
    text = path.read_text(encoding="utf-8", errors="ignore")
    locales = extract_all(text)
    if not locales:
        raise SystemExit(f"No locale blocks found in {path}")
    OUT.mkdir(parents=True, exist_ok=True)
    write_locale_files(locales)
    print(f"Extracted {len(locales)} locales from {path.name}")


if __name__ == "__main__":
    main()
