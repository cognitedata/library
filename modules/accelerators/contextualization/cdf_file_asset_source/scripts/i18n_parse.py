"""Parse i18n message maps from locale TypeScript files."""

from __future__ import annotations

import json
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
