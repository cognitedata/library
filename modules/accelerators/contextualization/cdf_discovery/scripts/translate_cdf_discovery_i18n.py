#!/usr/bin/env python3
"""Fill locale files with translations for keys still identical to en."""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path

from deep_translator import GoogleTranslator

from i18n_parse import parse_messages, write_messages

_MODULE_ROOT = Path(__file__).resolve().parent.parent

LOCALES = ("de", "fr", "es", "nb", "pt", "ja", "zh", "hi", "ar")

TARGET_LANG = {"nb": "no", "zh": "zh-CN"}

KEEP_LITERAL = {
    "CDF Discovery",
    "CDF",
    "SQL",
    "JSON",
    "YAML",
    "CSV",
    "Excel",
    "Parquet",
    "DuckDB",
    "Jinja",
    "Entra",
    "GUID",
    "RAW",
    "ID",
    "#",
    "—",
    " · ",
    " → ",
    "#",
    "type: list",
    "exit_code: {code}\n\n",
    "--- stdout ---\n",
    "--- stderr ---\n",
    "gp_{{ data_type_id }}_{{ location_id }}_{{ access_type_id }}",
    "{{ data_type }}:{{ site_id | default(scope_id) }}:{{ access_type_id }}",
    "inst_{{ data_type_id }}_{{ source_system_id }}_{{ scope_id_snake }}",
    "{{ data_type }}:{{ site_id }}:{{ source_system_id }}",
    "e.g. inst_{{ source_system_id }}_{{ scope_id_snake }}",
    "default: {{ instance_space }}",
}

PLACEHOLDER_RE = re.compile(
    r"(\{\{[^}]+\}\}|\{[a-zA-Z_][a-zA-Z0-9_]*\}|``[^`]+``|`[^`]+`)"
)


def protect(text: str) -> tuple[str, list[str]]:
    tokens: list[str] = []

    def repl(m: re.Match[str]) -> str:
        tokens.append(m.group(0))
        return f"__PH{len(tokens) - 1}__"

    return PLACEHOLDER_RE.sub(repl, text), tokens


def restore(text: str, tokens: list[str]) -> str:
    out = text
    for i, tok in enumerate(tokens):
        out = out.replace(f"__PH{i}__", tok)
    return out


def should_skip_translation(en_val: str, key: str) -> bool:
    if en_val in KEEP_LITERAL:
        return True
    if key == "dimensions.keyDisplay":
        return True
    if key.endswith(".yamlCommentChar") or key == "grid.loadingStatus":
        return True
    if key in (
        "governance.build.logExitCode",
        "governance.build.logStdout",
        "governance.build.logStderr",
    ):
        return True
    return False


def translate_value(en_val: str, key: str, translator: GoogleTranslator) -> str:
    if should_skip_translation(en_val, key):
        return en_val
    protected, tokens = protect(en_val)
    if not protected.strip():
        return en_val
    try:
        out = translator.translate(protected)
    except Exception:
        time.sleep(0.5)
        out = translator.translate(protected)
    return restore(out, tokens)


def apply_locale(
    i18n_dir: Path,
    loc: str,
    *,
    only_untranslated: bool,
    dry_run: bool,
) -> int:
    en = parse_messages(i18n_dir / "en.ts")
    loc_path = i18n_dir / f"{loc}.ts"
    current = parse_messages(loc_path) if loc_path.is_file() else {}
    merged = {**en, **current}
    target = TARGET_LANG.get(loc, loc)
    translator = GoogleTranslator(source="en", target=target)

    updated = 0
    keys = sorted(en.keys())
    for i, key in enumerate(keys):
        en_val = en[key]
        cur = merged.get(key, en_val)
        if only_untranslated and cur != en_val:
            continue
        if should_skip_translation(en_val, key):
            if key not in merged:
                merged[key] = en_val
            continue
        new_val = translate_value(en_val, key, translator)
        if new_val is None:
            new_val = en_val
        if new_val != cur:
            merged[key] = new_val
            updated += 1
        if (i + 1) % 25 == 0:
            time.sleep(0.15)
        if dry_run and updated >= 5:
            break

    if not dry_run:
        write_messages(
            loc_path,
            loc,
            merged,
            'import type { Messages } from "./types";',
            None,
        )
    return updated


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--module-root", type=Path, default=_MODULE_ROOT)
    p.add_argument("--locale", action="append", dest="locales")
    p.add_argument("--all-keys", action="store_true", help="Retranslate even if value differs from en")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    i18n_dir = args.module_root.resolve() / "ui" / "src" / "i18n"
    locales = args.locales or list(LOCALES)
    for loc in locales:
        n = apply_locale(
            i18n_dir,
            loc,
            only_untranslated=not args.all_keys,
            dry_run=args.dry_run,
        )
        print(f"{loc}: updated {n} keys")


if __name__ == "__main__":
    main()
