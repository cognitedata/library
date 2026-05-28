#!/usr/bin/env python3
"""Ensure {{ placeholder }} tokens in Toolkit YAML exist in default.config.yaml."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import yaml

PLACEHOLDER = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")


def _collect_placeholders(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    body = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    return {m.group(1) for m in PLACEHOLDER.finditer(body)}


def _config_keys(config_path: Path) -> set[str]:
    doc = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise SystemExit(f"Config root must be a mapping: {config_path}")

    def walk(prefix: str, node: object) -> set[str]:
        keys: set[str] = set()
        if isinstance(node, dict):
            for k, v in node.items():
                sk = f"{prefix}.{k}" if prefix else str(k)
                keys.add(str(k))
                keys.update(walk(sk, v))
        return keys

    return walk("", doc)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--module-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=None)
    args = parser.parse_args()
    root = args.module_root.resolve()
    config = args.config or (root / "default.config.yaml")
    if not config.is_file():
        raise SystemExit(f"Missing config: {config}")
    keys = _config_keys(config)
    globs = [
        "data_sets/**/*.yaml",
        "functions/**/*.yaml",
        "workflows/**/*.yaml",
    ]
    missing: list[str] = []
    for pattern in globs:
        for path in root.glob(pattern):
            if not path.is_file():
                continue
            for ph in _collect_placeholders(path):
                if ph not in keys:
                    rel = path.relative_to(root)
                    missing.append(f"{rel}: {{{{ {ph} }}}}")
    if missing:
        print("Toolkit placeholder check failed:", file=__import__("sys").stderr)
        for m in sorted(set(missing)):
            print(f"  {m}", file=__import__("sys").stderr)
        raise SystemExit(1)
    print("Toolkit placeholder check OK")


if __name__ == "__main__":
    main()
