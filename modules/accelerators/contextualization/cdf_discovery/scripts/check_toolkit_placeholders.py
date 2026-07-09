#!/usr/bin/env python3
"""Ensure {{ placeholder }} tokens in Toolkit YAML exist in module deploy config."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import yaml

PLACEHOLDER = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")

DEFAULT_DEPLOY_CONFIG = Path("inverted_index") / "config" / "inverted_index_deploy.config.yaml"


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


def _merged_config_keys(module_root: Path, config: Path | None) -> set[str]:
    keys: set[str] = set()
    primary = config or (module_root / "default.config.yaml")
    if primary.is_file():
        keys |= _config_keys(primary)
    deploy = module_root / DEFAULT_DEPLOY_CONFIG
    if deploy.is_file():
        keys |= _config_keys(deploy)
    return keys


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--module-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=None)
    args = parser.parse_args()
    root = args.module_root.resolve()
    primary = args.config or (root / "default.config.yaml")
    if not primary.is_file() and not (root / DEFAULT_DEPLOY_CONFIG).is_file():
        raise SystemExit(f"Missing config: {primary}")
    keys = _merged_config_keys(root, args.config)
    globs = [
        "data_sets/**/*.yaml",
        "submodules/transform/functions/**/*.yaml",
        "workflows/**/*.yaml",
    ]
    missing: list[str] = []
    for pattern in globs:
        for path in root.glob(pattern):
            if not path.is_file():
                continue
            if path.name.endswith(".config.yaml"):
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
