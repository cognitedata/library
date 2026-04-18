"""Tests for workflow.execution.graph.yaml and WorkflowVersion alignment."""

import sys
import unittest
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
# Resolve cdf_fn_common without importing cdf_key_extraction_aliasing package __init__.
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.workflow_execution_graph import (  # noqa: E402
    ExecutionGraph,
    ExecutionGraphEdge,
    compare_graph_to_workflow_version,
    default_execution_graph_path,
    load_execution_graph,
    load_workflow_version_yaml,
    validate_execution_graph,
    validate_template_workflow_version_matches_execution_graph,
)


class TestWorkflowExecutionGraph(unittest.TestCase):
    def test_default_graph_loads_and_is_acyclic(self) -> None:
        p = default_execution_graph_path(_MODULE_ROOT)
        self.assertTrue(p.is_file(), msg=f"missing {p}")
        g = load_execution_graph(p)
        self.assertEqual(g.schema_version, 1)
        self.assertEqual(len(g.nodes), 5)
        errs = validate_execution_graph(g)
        self.assertEqual(errs, [])

    def test_graph_matches_workflow_version_template(self) -> None:
        wv = _MODULE_ROOT / "workflow_template" / "workflow.template.WorkflowVersion.yaml"
        errs = validate_template_workflow_version_matches_execution_graph(_MODULE_ROOT, wv)
        self.assertEqual(errs, [], msg="\n".join(errs))

    def test_compare_detects_mismatch(self) -> None:
        g = ExecutionGraph(
            schema_version=1,
            description="t",
            nodes=["fn_dm_key_extraction", "fn_dm_aliasing"],
            node_roles={},
            edges=[
                ExecutionGraphEdge(
                    from_id="fn_dm_key_extraction",
                    to_id="fn_dm_aliasing",
                    channel="x",
                )
            ],
        )
        wv = load_workflow_version_yaml(
            _MODULE_ROOT / "workflow_template" / "workflow.template.WorkflowVersion.yaml"
        )
        errs = compare_graph_to_workflow_version(g, wv)
        self.assertTrue(any("only in" in e or "mismatch" in e for e in errs))


if __name__ == "__main__":
    unittest.main()
