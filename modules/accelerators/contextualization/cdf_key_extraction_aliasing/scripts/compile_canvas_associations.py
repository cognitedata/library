#!/usr/bin/env python3
"""Merge source_view → extraction edges from a workflow canvas into a v1 scope file.

Writes ``associations`` on the scope document (same contract as ``syncWorkflowScopeFromCanvas`` for this slice only).

By default reads the embedded ``canvas`` mapping from ``--scope`` (e.g. ``workflow.local.config.yaml``). Pass
``--canvas`` for a separate WorkflowCanvasDocument YAML (top-level ``nodes`` / ``edges``).

Usage (from module root ``cdf_key_extraction_aliasing/``)::

  python scripts/compile_canvas_associations.py
  python scripts/compile_canvas_associations.py --canvas path/to.canvas.yaml --scope path/to.config.yaml
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

_SCRIPTS = Path(__file__).resolve().parent
_MODULE_ROOT = _SCRIPTS.parent
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.scope_canvas_merge import canvas_dict_from_layout_yaml  # noqa: E402
from cdf_fn_common.workflow_associations import (  # noqa: E402
    apply_canvas_dict_to_scope_associations,
    validate_workflow_associations,
)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--canvas",
        type=Path,
        default=None,
        help="WorkflowCanvasDocument YAML (optional). Default: embedded ``canvas`` in ``--scope``.",
    )
    p.add_argument(
        "--scope",
        type=Path,
        default=_MODULE_ROOT / "workflow.local.config.yaml",
        help="v1 scope YAML to update in place",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate only; print errors and do not write",
    )
    args = p.parse_args()

    scope_path = args.scope.resolve()
    if not scope_path.is_file():
        print(f"Missing scope file: {scope_path}", file=sys.stderr)
        return 1

    scope_doc = yaml.safe_load(scope_path.read_text(encoding="utf-8"))
    if not isinstance(scope_doc, dict):
        print("Scope YAML must be a mapping", file=sys.stderr)
        return 1

    canvas: dict
    canvas_label: str
    if args.canvas is not None:
        canvas_path = args.canvas.resolve()
        if not canvas_path.is_file():
            print(f"Missing canvas file: {canvas_path}", file=sys.stderr)
            return 1
        raw_c = yaml.safe_load(canvas_path.read_text(encoding="utf-8"))
        if not isinstance(raw_c, dict):
            print("Canvas YAML must be a mapping", file=sys.stderr)
            return 1
        cd = canvas_dict_from_layout_yaml(raw_c)
        if cd is None:
            print(f"No graph nodes in canvas file: {canvas_path}", file=sys.stderr)
            return 1
        canvas = cd
        canvas_label = str(canvas_path)
    else:
        cd = canvas_dict_from_layout_yaml(scope_doc)
        if cd is None:
            print(
                f"No embedded canvas with nodes in {scope_path}; add canvas.nodes or pass --canvas.",
                file=sys.stderr,
            )
            return 1
        canvas = cd
        canvas_label = f"embedded canvas in {scope_path}"

    apply_canvas_dict_to_scope_associations(canvas, scope_doc)
    errs = validate_workflow_associations(scope_doc)
    if errs:
        print("Validation failed:", file=sys.stderr)
        for e in errs:
            print(f"  - {e}", file=sys.stderr)
        return 1

    if args.dry_run:
        print("OK (dry-run): associations reconciled; not writing")
        return 0

    scope_path.write_text(
        yaml.dump(scope_doc, default_flow_style=False, allow_unicode=True, sort_keys=False, width=1000),
        encoding="utf-8",
    )
    print(f"Updated {scope_path} using {canvas_label}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
