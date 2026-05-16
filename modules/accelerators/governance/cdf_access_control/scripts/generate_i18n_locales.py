#!/usr/bin/env python3
"""Generate non-English locale files from en.ts (used when bundle extraction is unavailable)."""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

try:
    from deep_translator import GoogleTranslator
except ImportError as e:
    raise SystemExit("pip install deep-translator") from e

MODULE = Path(__file__).resolve().parent.parent
OUT = MODULE / "ui/src/i18n"
EN_TS = OUT / "en.ts"

TARGETS: dict[str, str] = {
    "ar": "ar",
    "de": "de",
    "es": "es",
    "fr": "fr",
    "nb": "no",
    "pt": "pt",
    "hi": "hi",
    "ja": "ja",
    "zh": "zh-CN",
}

PH_RE = re.compile(r"\{(\w+)\}")
CODE_RE = re.compile(r"<code>[^<]*</code>")


def parse_en() -> dict[str, str]:
    text = EN_TS.read_text(encoding="utf-8")
    pairs = re.findall(
        r'^\s+"((?:\\.|[^"\\])+)":\s+((?:.+)),\s*$',
        text,
        flags=re.MULTILINE,
    )
    out: dict[str, str] = {}
    for k, raw in pairs:
        key = json.loads(f'"{k}"')
        out[key] = json.loads(raw)
    return out


def protect(text: str) -> tuple[str, dict[str, str]]:
    tokens: dict[str, str] = {}
    idx = 0

    def stash(match: re.Match[str]) -> str:
        nonlocal idx
        tok = f"⟦{idx}⟧"
        tokens[tok] = match.group(0)
        idx += 1
        return tok

    s = CODE_RE.sub(stash, text)
    s = PH_RE.sub(stash, s)
    return s, tokens


def restore(text: str, tokens: dict[str, str]) -> str:
    for tok, orig in tokens.items():
        text = text.replace(tok, orig)
    return text


def translate_batch(texts: list[str], translator: GoogleTranslator) -> list[str]:
    protected_list: list[str] = []
    token_maps: list[dict[str, str]] = []
    for text in texts:
        if not text.strip():
            protected_list.append(text)
            token_maps.append({})
        else:
            p, tok = protect(text)
            protected_list.append(p)
            token_maps.append(tok)
    try:
        translated = translator.translate_batch(protected_list)
    except Exception:
        time.sleep(2)
        translated = translator.translate_batch(protected_list)
    out: list[str] = []
    for orig, tr, tok in zip(texts, translated, token_maps):
        if not orig.strip():
            out.append(orig)
        else:
            out.append(restore(tr, tok))
    return out


def write_locale(loc: str, messages: dict[str, str], keys: list[str]) -> None:
    lines = ['import type { Messages } from "./types";', "", f"export const {loc}: Messages = {{"]
    for k in keys:
        lines.append(f"  {json.dumps(k)}: {json.dumps(messages[k], ensure_ascii=False)},")
    lines.append("};")
    lines.append("")
    (OUT / f"{loc}.ts").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    en = parse_en()
    keys = sorted(en.keys())
    print(f"Translating {len(keys)} keys into {len(TARGETS)} locales…")
    for loc, target in TARGETS.items():
        print(f"  {loc} ({target})…", flush=True)
        tr = GoogleTranslator(source="en", target=target)
        out: dict[str, str] = {}
        batch_size = 40
        values = [en[k] for k in keys]
        translated_values: list[str] = []
        for i in range(0, len(values), batch_size):
            chunk = values[i : i + batch_size]
            translated_values.extend(translate_batch(chunk, tr))
            time.sleep(0.4)
        for k, v in zip(keys, translated_values):
            out[k] = v
        write_locale(loc, out, keys)
    print("Done.")


if __name__ == "__main__":
    main()
