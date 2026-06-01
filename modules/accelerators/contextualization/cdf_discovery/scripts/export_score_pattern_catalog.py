#!/usr/bin/env python3
"""Export score/validation regex pattern descriptions from cdf_discovery_aliasing for the UI."""

from __future__ import annotations

import json
import sys
from pathlib import Path

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise SystemExit("PyYAML required: pip install pyyaml") from exc

MODULE_ROOT = Path(__file__).resolve().parents[1]
ALIASING_ROOT = MODULE_ROOT.parent / "cdf_discovery_aliasing"
FUNCS = MODULE_ROOT / "functions"
if str(FUNCS) not in sys.path:
    sys.path.insert(0, str(FUNCS))

from cdf_fn_common.isa_tag_pattern import (  # noqa: E402
    ISA_TAG_AREA_PREFIX,
    should_apply_isa_tag_area_prefix,
)


def _collect_pattern_descriptions(obj: object, out: dict[str, str]) -> None:
    if isinstance(obj, dict):
        pat = obj.get("pattern")
        desc = obj.get("description")
        if isinstance(pat, str) and isinstance(desc, str):
            p = pat.strip()
            d = desc.strip()
            if p and d:
                out[p] = d
        for v in obj.values():
            _collect_pattern_descriptions(v, out)
    elif isinstance(obj, list):
        for item in obj:
            _collect_pattern_descriptions(item, out)


def _load_yaml(path: Path) -> object:
    if not path.is_file():
        raise FileNotFoundError(path)
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _add_score_suffix(pattern: str, description: str, out: dict[str, str]) -> None:
    if pattern.startswith(ISA_TAG_AREA_PREFIX):
        suffix = pattern[len(ISA_TAG_AREA_PREFIX) :].strip()
        if suffix and suffix not in out:
            out[suffix] = description
    elif should_apply_isa_tag_area_prefix(pattern):
        prefixed = ISA_TAG_AREA_PREFIX + pattern
        if prefixed not in out:
            out[prefixed] = description


def main() -> None:
    out: dict[str, str] = {}

    tag_patterns_path = ALIASING_ROOT / "config" / "tag_patterns.yaml"
    _collect_pattern_descriptions(_load_yaml(tag_patterns_path), out)
    for pattern, description in list(out.items()):
        _add_score_suffix(pattern, description, out)

    for rel in (
        "workflow.local.config.yaml",
        "workflow_template/workflow.template.config.yaml",
    ):
        path = ALIASING_ROOT / rel
        if path.is_file():
            _collect_pattern_descriptions(_load_yaml(path), out)

    etl_config = MODULE_ROOT / "workflows" / "etl_aliasing_workflow.config.yaml"
    if etl_config.is_file():
        _collect_pattern_descriptions(_load_yaml(etl_config), out)

    examples_dir = ALIASING_ROOT / "config" / "examples"
    if examples_dir.is_dir():
        for path in sorted(examples_dir.rglob("*.yaml")):
            _collect_pattern_descriptions(_load_yaml(path), out)

    if not out:
        raise RuntimeError("No pattern descriptions collected")

    out_dir = MODULE_ROOT / "ui" / "src" / "generated"
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "scorePatternCatalog.json"
    json_path.write_text(
        json.dumps(dict(sorted(out.items())), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(out)} entries to {json_path}")


if __name__ == "__main__":
    main()
