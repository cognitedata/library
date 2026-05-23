#!/usr/bin/env python3
"""Lightweight config convention checks for a single accelerator module root."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import yaml

INST_RE = re.compile(r"^inst_[a-z0-9_]+$")
GP_RE = re.compile(r"^gp_[a-z0-9_]+$")


def _load(path: Path) -> dict:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise ValueError(f"{path}: root must be a mapping")
    return doc


def check_governance(root: Path) -> list[str]:
    errs: list[str] = []
    cfg = root / "default.config.yaml"
    if cfg.is_file():
        doc = _load(cfg)
        if "scope_hierarchy" not in doc:
            errs.append("default.config.yaml: missing top-level scope_hierarchy")
        dims = doc.get("dimensions")
        if isinstance(dims, dict):
            for name, block in dims.items():
                if isinstance(block, dict) and block.get("type") == "hierarchy":
                    errs.append(
                        f"dimensions.{name}: hierarchy blocks belong in scope_hierarchy, not dimensions"
                    )
    for p in root.glob("spaces/**/*.Space.yaml"):
        d = _load(p)
        space = d.get("space")
        if not space or not INST_RE.match(str(space)):
            errs.append(f"{p.relative_to(root)}: space must match inst_*")
    for p in root.glob("auth/**/*.Group.yaml"):
        d = _load(p)
        name = d.get("name")
        if not name or not GP_RE.match(str(name)):
            errs.append(f"{p.relative_to(root)}: name must match gp_*")
    return errs


def check_file_asset(root: Path) -> list[str]:
    errs: list[str] = []
    cfg = root / "default.config.yaml"
    if not cfg.is_file():
        return errs
    doc = _load(cfg)
    if "scope_hierarchy" not in doc:
        errs.append("default.config.yaml: missing top-level scope_hierarchy")
    fas = doc.get("file_asset_source", {})
    if isinstance(fas, dict):
        create_data = (fas.get("create") or {}).get("data") if isinstance(fas.get("create"), dict) else {}
        if isinstance(create_data, dict) and (
            "scope" in create_data or "hierarchy_levels" in create_data
        ):
            errs.append("file_asset_source.create.data: scope/hierarchy_levels must live under scope_hierarchy")
    return errs


def check_discovery(root: Path) -> list[str]:
    errs: list[str] = []
    cfg = root / "default.config.yaml"
    if not cfg.is_file():
        return errs
    doc = _load(cfg)
    if "aliasing_scope_hierarchy" in doc:
        errs.append("default.config.yaml: rename aliasing_scope_hierarchy → scope_hierarchy")
    if "scope_hierarchy" not in doc:
        errs.append("default.config.yaml: missing scope_hierarchy")
    for camel in ("schemaSpace", "functionClientId", "key_extraction_aliasing_schedule"):
        if camel in doc:
            errs.append(f"default.config.yaml: legacy camelCase key {camel}")
    return errs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--module-root", type=Path, required=True)
    args = parser.parse_args()
    root = args.module_root.resolve()
    name = root.name
    if name == "cdf_discovery":
        errs = check_discovery(root)
        gov = root / "governance"
        if gov.is_dir():
            errs.extend(check_governance(gov))
    elif "file_asset" in name:
        errs = check_file_asset(root)
    elif "discovery" in name or "aliasing" in name:
        errs = check_discovery(root)
    else:
        errs = []
    if errs:
        print("Config convention check failed:", file=__import__("sys").stderr)
        for e in errs:
            print(f"  {e}", file=__import__("sys").stderr)
        raise SystemExit(1)
    print("Config convention check OK")


if __name__ == "__main__":
    main()
