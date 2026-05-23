#!/usr/bin/env python3
"""Apply governance/scope editor translations from governance-locales.json into locale *.ts files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from i18n_parse import parse_messages, write_messages

_MODULE_ROOT = Path(__file__).resolve().parent.parent

GOVERNANCE_PREFIXES = (
    "governance.",
    "dimensions.",
    "groups.",
    "spaces.",
    "editor.sourceIdsDragHint",
    "tabs.dimensions",
    "btn.cancel",
    "btn.dryRun",
    "btn.mirrorAccessControl",
    "btn.refreshList",
    "btn.reload",
    "btn.runBuild",
    "btn.runBuildForce",
    "btn.saveConfiguration",
    "btn.saveFile",
    "advanced.",
    "artifacts.tree",
    "treeContext.",
    "common.dragHandle.",
    "common.emptyValue",
    "common.metaSeparator",
)


def is_governance_key(key: str) -> bool:
    return any(
        key == p.rstrip(".") or key.startswith(p if p.endswith(".") else p + ".")
        for p in GOVERNANCE_PREFIXES
    )


def load_overrides(path: Path) -> dict[str, dict[str, str]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit("governance-locales.json must be a locale → messages object")
    return {str(loc): {str(k): str(v) for k, v in msgs.items()} for loc, msgs in data.items()}


def apply(i18n_dir: Path, overrides_path: Path, locales: list[str] | None) -> None:
    en = parse_messages(i18n_dir / "en.ts")
    gov_keys = {k for k in en if is_governance_key(k)}
    overrides = load_overrides(overrides_path)
    target_locales = locales or sorted(overrides.keys())
    import_line = 'import type { Messages } from "./types";'

    for loc in target_locales:
        loc_path = i18n_dir / f"{loc}.ts"
        if not loc_path.is_file():
            print(f"{loc}: skip (no file)")
            continue
        merged = parse_messages(loc_path)
        loc_overrides = overrides.get(loc, {})
        updated = 0
        for k in gov_keys:
            if k in loc_overrides:
                merged[k] = loc_overrides[k]
                updated += 1
        write_messages(loc_path, loc, merged, import_line, None)
        print(f"{loc}: updated {updated} governance keys ({len(merged)} total)")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--module-root", type=Path, default=_MODULE_ROOT)
    p.add_argument(
        "--overrides",
        type=Path,
        default=None,
        help="Path to governance-locales.json (default: ui/src/i18n/governance-locales.json)",
    )
    p.add_argument("--locale", action="append", dest="locales", help="Apply only these locales")
    args = p.parse_args()
    root = args.module_root.resolve()
    i18n_dir = root / "ui" / "src" / "i18n"
    overrides_path = args.overrides or (i18n_dir / "governance-locales.json")
    apply(i18n_dir, overrides_path, args.locales)


if __name__ == "__main__":
    main()
