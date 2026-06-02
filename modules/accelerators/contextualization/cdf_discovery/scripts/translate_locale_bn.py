#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

from i18n_parse import parse_messages, write_messages


ROOT = Path(__file__).resolve().parents[1]
I18N_DIR = ROOT / "ui" / "src" / "i18n"
EN_PATH = I18N_DIR / "en.ts"
BN_PATH = I18N_DIR / "bn.ts"
CACHE_PATH = ROOT / "scripts" / ".bn_translate_cache.json"

PLACEHOLDER_RE = re.compile(r"\{[^{}]+\}")


def _load_cache() -> dict[str, str]:
    if CACHE_PATH.is_file():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {}


def _save_cache(cache: dict[str, str]) -> None:
    CACHE_PATH.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _protect_placeholders(text: str) -> tuple[str, list[str]]:
    placeholders: list[str] = []

    def repl(match: re.Match[str]) -> str:
        idx = len(placeholders)
        placeholders.append(match.group(0))
        return f"__PH_{idx}__"

    return PLACEHOLDER_RE.sub(repl, text), placeholders


def _restore_placeholders(text: str, placeholders: list[str]) -> str:
    out = text
    for idx, ph in enumerate(placeholders):
        out = out.replace(f"__PH_{idx}__", ph)
    return out


def _translate_batch(texts: list[str]) -> list[str]:
    if not texts:
        return []
    prepared: list[tuple[str, list[str]]] = []
    for text in texts:
        protected, placeholders = _protect_placeholders(text)
        prepared.append((protected, placeholders))
    params = urllib.parse.urlencode(
        [("client", "gtx"), ("sl", "en"), ("tl", "bn"), ("dt", "t")]
        + [("q", protected) for protected, _ in prepared]
    )
    url = f"https://translate.googleapis.com/translate_a/single?{params}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    chunk = payload[0]
    if len(prepared) == 1:
        translated = "".join(part[0] for part in chunk if part and part[0] is not None)
        return [_restore_placeholders(translated, prepared[0][1])]
    outs: list[str] = []
    for i, (_, placeholders) in enumerate(prepared):
        part = chunk[i]
        translated = part[0] if isinstance(part, list) and part else str(part)
        outs.append(_restore_placeholders(translated, placeholders))
    return outs


def main() -> None:
    en = parse_messages(EN_PATH)
    cache = _load_cache()
    out: dict[str, str] = {}
    keys = list(en.keys())
    total = len(keys)

    pending_keys: list[str] = []
    pending_src: list[str] = []
    for key in keys:
        src = en[key]
        if src in cache:
            out[key] = cache[src]
        else:
            pending_keys.append(key)
            pending_src.append(src)

    batch_size = 40
    done = total - len(pending_keys)
    for i in range(0, len(pending_keys), batch_size):
        batch_keys = pending_keys[i : i + batch_size]
        batch_src = pending_src[i : i + batch_size]
        try:
            batch_out = _translate_batch(batch_src)
        except Exception:
            batch_out = batch_src
        for key, src, out_val in zip(batch_keys, batch_src, batch_out):
            out[key] = out_val
            cache[src] = out_val
        done += len(batch_keys)
        print(f"translated {done}/{total}")
        _save_cache(cache)
        time.sleep(0.1)

    _save_cache(cache)
    write_messages(BN_PATH, "bn", out, 'import type { Messages } from "./types";')
    print(f"wrote {BN_PATH}")


if __name__ == "__main__":
    main()
