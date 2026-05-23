#!/usr/bin/env python3
"""Generate non-English locale files from en.ts for the file asset source operator UI."""

from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

try:
    from deep_translator import GoogleTranslator
except ImportError as e:
    raise SystemExit("pip install deep-translator") from e

MODULE = Path(__file__).resolve().parent.parent
OUT = MODULE / "ui/src/i18n"
EN_TS = OUT / "en.ts"

# Post-translation fixes (UI "run" = execute, not jog/sport).
TERM_OVERRIDES: dict[str, dict[str, str]] = {
    "ar": {
        "tabs.run": "تشغيل",
        "status.running": "قيد التشغيل…",
        "run.stepStatus.running": "قيد التشغيل",
    },
    "es": {
        "tabs.run": "Ejecutar",
        "status.running": "Ejecutando…",
        "run.stepStatus.running": "en ejecución",
    },
    "fr": {"tabs.run": "Exécuter", "configure.step.extract": "Extraire"},
    "nb": {"tabs.run": "Kjør", "patterns.category.general": "Generelt"},
    "pt": {"tabs.run": "Executar", "status.running": "Executando…"},
    "hi": {
        "tabs.run": "चलाएँ",
        "status.running": "चल रहा है…",
        "run.stepStatus.running": "चल रहा है",
    },
    "ja": {"tabs.run": "実行", "patterns.category.instrument": "計器"},
    "zh": {
        "tabs.run": "运行",
        "status.running": "运行中…",
        "btn.save": "保存",
        "configure.step.extract": "提取",
    },
}

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
    start = text.find("{")
    end = text.rfind("}")
    body = text[start + 1 : end]
    out: dict[str, str] = {}
    i = 0
    n = len(body)
    while i < n:
        while i < n and body[i] in " \t\n\r,":
            i += 1
        if i >= n or body[i] != '"':
            i += 1
            continue
        j = i + 1
        while j < n:
            if body[j] == "\\":
                j += 2
                continue
            if body[j] == '"':
                key = json.loads(body[i : j + 1])
                i = j + 1
                break
            j += 1
        else:
            break
        while i < n and body[i] in " \t\n\r":
            i += 1
        if i >= n or body[i] != ":":
            continue
        i += 1
        while i < n and body[i] in " \t\n\r":
            i += 1
        if i >= n:
            break
        if body[i] == '"':
            j = i + 1
            while j < n:
                if body[j] == "\\":
                    j += 2
                    continue
                if body[j] == '"':
                    out[key] = json.loads(body[i : j + 1])
                    i = j + 1
                    break
                j += 1
        elif body[i] == "'":
            j = i + 1
            while j < n:
                if body[j] == "\\":
                    j += 2
                    continue
                if body[j] == "'":
                    raw = body[i + 1 : j]
                    out[key] = raw.replace('\\"', '"').replace("\\'", "'")
                    i = j + 1
                    break
                j += 1
        else:
            i += 1
        while i < n and body[i] in " \t\n\r,":
            i += 1
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
        out.append(orig if not orig.strip() else restore(tr, tok))
    return out


def write_locale(loc: str, messages: dict[str, str], keys: list[str]) -> None:
    lines = ['import type { LocaleMessages } from "./types";', "", f"export const {loc}: LocaleMessages = {{"]
    for k in keys:
        if k in messages:
            lines.append(f"  {json.dumps(k)}: {json.dumps(messages[k], ensure_ascii=False)},")
    lines.append("};")
    lines.append("")
    (OUT / f"{loc}.ts").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    skip = {s.strip() for s in sys.argv[1:] if s.strip()}
    en = parse_en()
    keys = list(en.keys())
    print(f"Translating {len(keys)} keys into {len(TARGETS)} locales (skip: {skip or 'none'})…")
    for loc, target in TARGETS.items():
        if loc in skip:
            print(f"  {loc}: skipped")
            continue
        print(f"  {loc} ({target})…", flush=True)
        tr = GoogleTranslator(source="en", target=target)
        values = [en[k] for k in keys]
        translated_values: list[str] = []
        batch_size = 30
        for i in range(0, len(values), batch_size):
            chunk = values[i : i + batch_size]
            translated_values.extend(translate_batch(chunk, tr))
            time.sleep(0.5)
        out = dict(zip(keys, translated_values))
        out.update(TERM_OVERRIDES.get(loc, {}))
        write_locale(loc, out, keys)
    print("Done.")


if __name__ == "__main__":
    main()
