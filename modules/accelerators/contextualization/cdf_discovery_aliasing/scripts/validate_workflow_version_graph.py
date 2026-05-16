#!/usr/bin/env python3
"""Validate workflow.execution.graph.yaml against compiled_workflow IR from the scope template."""

from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent
_MODULE_ROOT = _SCRIPTS.parent
for p in (_MODULE_ROOT, _SCRIPTS):
    s = str(p)
    if s not in sys.path:
        sys.path.insert(0, s)

from functions.cdf_fn_common.workflow_compile.canvas_dag import (  # noqa: E402
    compiled_workflow_for_scope_document,
)
from functions.cdf_fn_common.workflow_execution_graph import (  # noqa: E402
    default_execution_graph_path,
    depends_on_from_execution_graph,
    execution_graph_from_compiled_workflow,
    load_execution_graph,
    validate_execution_graph,
)
from scope_build.scope_document_load import load_scope_document_dict_for_build  # noqa: E402


def main() -> int:
    graph_path = default_execution_graph_path(_MODULE_ROOT)
    scope_path = _MODULE_ROOT / "workflow_template" / "workflow.template.config.yaml"
    g_disk = load_execution_graph(graph_path)
    errs = validate_execution_graph(g_disk)
    scope_doc = load_scope_document_dict_for_build(scope_path)
    cw = compiled_workflow_for_scope_document(scope_doc)
    g_ir = execution_graph_from_compiled_workflow(cw)
    d_disk = depends_on_from_execution_graph(g_disk)
    d_ir = depends_on_from_execution_graph(g_ir)
    nodes_disk = set(d_disk.keys())
    nodes_ir = set(d_ir.keys())
    if nodes_disk != nodes_ir:
        only_d = sorted(nodes_disk - nodes_ir)
        only_i = sorted(nodes_ir - nodes_disk)
        if only_d:
            errs.append(f"Nodes only in workflow.execution.graph.yaml: {only_d}")
        if only_i:
            errs.append(f"Nodes only in compiled_workflow IR graph: {only_i}")
    for n in sorted(nodes_disk & nodes_ir):
        if d_disk.get(n) != d_ir.get(n):
            errs.append(
                f"dependsOn mismatch for {n}: disk={d_disk.get(n)} ir={d_ir.get(n)}"
            )
    if errs:
        print("Validation failed:", file=sys.stderr)
        for e in errs:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print(
        f"OK: {graph_path.name} matches compiled_workflow IR ({len(g_disk.nodes)} nodes)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
