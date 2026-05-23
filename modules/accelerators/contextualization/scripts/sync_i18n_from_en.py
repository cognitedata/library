#!/usr/bin/env python3
"""Merge missing message keys from en.ts into other locale files (keeps existing translations)."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def parse_messages(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return {}
    body = text[start + 1 : end]
    out: dict[str, str] = {}
    i = 0
    n = len(body)
    while i < n:
        while i < n and body[i] in " \t\n\r,":
            i += 1
        if i >= n:
            break
        if body[i] != '"':
            i += 1
            continue
        key, i = _read_quoted(body, i)
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
            val, i = _read_quoted(body, i)
        elif body[i] == "'":
            val, i = _read_single_quoted(body, i)
        else:
            continue
        out[key] = val
        while i < n and body[i] in " \t\n\r,":
            i += 1
    return out


def _read_quoted(s: str, i: int) -> tuple[str, int]:
    assert s[i] == '"'
    j = i + 1
    while j < len(s):
        if s[j] == "\\":
            j += 2
            continue
        if s[j] == '"':
            return json.loads(s[i : j + 1]), j + 1
        j += 1
    raise ValueError(f"unterminated double-quoted string at {i}")


def _read_single_quoted(s: str, i: int) -> tuple[str, int]:
    assert s[i] == "'"
    j = i + 1
    while j < len(s):
        if s[j] == "\\":
            j += 2
            continue
        if s[j] == "'":
            return json.loads('"' + s[i + 1 : j].replace('"', '\\"') + '"'), j + 1
        j += 1
    raise ValueError(f"unterminated single-quoted string at {i}")


def write_messages(
    path: Path,
    export_name: str,
    messages: dict[str, str],
    import_line: str,
    header_comment: str | None = None,
) -> None:
    lines: list[str] = [import_line]
    if header_comment:
        lines.append(header_comment)
    lines.append("")
    type_suffix = ": Messages" if "Messages" in import_line and "LocaleMessages" not in import_line else ""
    if "LocaleMessages" in import_line:
        type_suffix = ": LocaleMessages"
    lines.append(f"export const {export_name}{type_suffix} = {{")
    for k in sorted(messages.keys(), key=lambda x: (x.split(".")[0], x)):
        # preserve en.ts key order-ish: sort alphabetically is fine
        pass
    # Use en key order when possible
    en_path = path.parent / "en.ts"
    en_keys = list(parse_messages(en_path).keys()) if en_path.is_file() else []
    order = [k for k in en_keys if k in messages]
    order.extend(sorted(set(messages.keys()) - set(order)))
    for k in order:
        lines.append(f"  {json.dumps(k)}: {json.dumps(messages[k], ensure_ascii=False)},")
    lines.append("};")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def merge_locale(
    i18n_dir: Path,
    loc: str,
    import_line: str,
    export_name: str | None = None,
    header_comment: str | None = None,
) -> int:
    export_name = export_name or loc
    en = parse_messages(i18n_dir / "en.ts")
    loc_path = i18n_dir / f"{loc}.ts"
    existing = parse_messages(loc_path) if loc_path.is_file() else {}
    added = 0
    merged = dict(existing)
    for k, v in en.items():
        if k not in merged:
            merged[k] = v
            added += 1
    # preserve header comment from file if present
    if loc_path.is_file() and not header_comment:
        first_lines = loc_path.read_text(encoding="utf-8").splitlines()
        for line in first_lines[:3]:
            if line.startswith("/**"):
                header_comment = line
                break
    write_messages(loc_path, export_name, merged, import_line, header_comment)
    return added


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("i18n_dir", type=Path)
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
    i18n_dir = args.i18n_dir.resolve()
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
