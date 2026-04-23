#!/usr/bin/env python3
"""Merge source_view → extraction edges from ``workflow.local.canvas.yaml`` into scope YAML.

Writes ``associations`` on the scope document (same contract as ``syncWorkflowScopeFromCanvas`` for this slice only).

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

from cdf_fn_common.workflow_associations import (  # noqa: E402
    apply_canvas_dict_to_scope_associations,
    validate_workflow_associations,
)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--canvas",
        type=Path,
        default=_MODULE_ROOT / "workflow.local.canvas.yaml",
        help="WorkflowCanvasDocument YAML",
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

    if not args.canvas.is_file():
        print(f"Missing canvas file: {args.canvas}", file=sys.stderr)
        return 1
    if not args.scope.is_file():
        print(f"Missing scope file: {args.scope}", file=sys.stderr)
        return 1

    canvas = yaml.safe_load(args.canvas.read_text(encoding="utf-8"))
    if not isinstance(canvas, dict):
        print("Canvas YAML must be a mapping", file=sys.stderr)
        return 1

    scope_doc = yaml.safe_load(args.scope.read_text(encoding="utf-8"))
    if not isinstance(scope_doc, dict):
        print("Scope YAML must be a mapping", file=sys.stderr)
        return 1

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

    args.scope.write_text(
        yaml.dump(scope_doc, default_flow_style=False, allow_unicode=True, sort_keys=False, width=1000),
        encoding="utf-8",
    )
    print(f"Updated {args.scope} from {args.canvas}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
