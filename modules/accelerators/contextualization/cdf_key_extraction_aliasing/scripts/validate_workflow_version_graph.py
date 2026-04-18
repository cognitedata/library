#!/usr/bin/env python3
"""Validate workflow.execution.graph.yaml against workflow.template.WorkflowVersion.yaml (exit 0/1)."""

from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent
_MODULE_ROOT = _SCRIPTS.parent
if str(_MODULE_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODULE_ROOT))

from functions.cdf_fn_common.workflow_execution_graph import (  # noqa: E402
    default_execution_graph_path,
    load_execution_graph,
    validate_execution_graph,
    validate_template_workflow_version_matches_execution_graph,
)


def main() -> int:
    graph_path = default_execution_graph_path(_MODULE_ROOT)
    wv_path = _MODULE_ROOT / "workflow_template" / "workflow.template.WorkflowVersion.yaml"
    g = load_execution_graph(graph_path)
    errs = validate_execution_graph(g)
    errs.extend(validate_template_workflow_version_matches_execution_graph(_MODULE_ROOT, wv_path))
    if errs:
        print("Validation failed:", file=sys.stderr)
        for e in errs:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print(f"OK: {graph_path.name} matches {wv_path.name} ({len(g.nodes)} nodes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
