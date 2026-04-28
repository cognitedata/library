"""Tests for workflow.execution.graph.yaml and WorkflowVersion alignment."""

import sys
import unittest
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
# Resolve cdf_fn_common without importing cdf_key_extraction_aliasing package __init__.
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.workflow_compile.canvas_dag import (  # noqa: E402
    compile_canvas_dag,
    compiled_workflow_for_scope_document,
)
from cdf_fn_common.workflow_compile.codegen import build_workflow_version_document  # noqa: E402
from cdf_fn_common.workflow_execution_graph import (  # noqa: E402
    ExecutionGraph,
    ExecutionGraphEdge,
    compare_graph_to_workflow_version,
    compiled_workflow_structural_signature,
    default_execution_graph_path,
    depends_on_from_execution_graph,
    execution_graph_from_compiled_workflow,
    load_execution_graph,
    load_workflow_version_yaml,
    validate_compiled_workflow_matches_workflow_version_document,
    validate_execution_graph,
)


class TestWorkflowExecutionGraph(unittest.TestCase):
    def test_default_graph_loads_and_is_acyclic(self) -> None:
        p = default_execution_graph_path(_MODULE_ROOT)
        self.assertTrue(p.is_file(), msg=f"missing {p}")
        g = load_execution_graph(p)
        self.assertEqual(g.schema_version, 1)
        self.assertGreaterEqual(len(g.nodes), 1)
        errs = validate_execution_graph(g)
        self.assertEqual(errs, [])

    def test_committed_execution_graph_matches_scope_template_ir(self) -> None:
        """``workflow.execution.graph.yaml`` stays aligned with canvas IR from the scope template."""
        _SCRIPTS = _MODULE_ROOT / "scripts"
        if str(_SCRIPTS) not in sys.path:
            sys.path.insert(0, str(_SCRIPTS))
        from scope_build.scope_document_load import load_scope_document_dict_for_build  # noqa: E402

        scope_path = _MODULE_ROOT / "workflow_template" / "workflow.template.config.yaml"
        doc = load_scope_document_dict_for_build(scope_path)
        cw = compiled_workflow_for_scope_document(doc)
        g_disk = load_execution_graph(default_execution_graph_path(_MODULE_ROOT))
        g_ir = execution_graph_from_compiled_workflow(cw)
        self.assertEqual(set(g_disk.nodes), set(g_ir.nodes))
        self.assertEqual(
            depends_on_from_execution_graph(g_disk),
            depends_on_from_execution_graph(g_ir),
        )

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

    def test_execution_graph_from_compiled_workflow_matches_codegen_version(self) -> None:
        doc = {
            "schemaVersion": 1,
            "source_views": [{"view_space": "cdf_cdm", "view_external_id": "CogniteFile", "view_version": "v1"}],
            "key_extraction": {"config": {"parameters": {"raw_db": "db_ke", "raw_table_key": "tkey"}}},
            "aliasing": {"config": {"parameters": {"raw_db": "db_al", "raw_table_aliases": "tal"}}},
            "compile_workflow_dag": "canvas",
            "canvas": {
                "nodes": [
                    {"id": "st", "kind": "start"},
                    {"id": "ex", "kind": "extraction", "data": {}},
                    {"id": "al", "kind": "aliasing", "data": {}},
                ],
                "edges": [
                    {"source": "st", "target": "ex"},
                    {"source": "ex", "target": "al"},
                ],
            },
        }
        cw = compile_canvas_dag(doc)
        wv = build_workflow_version_document(
            workflow_external_id="key_extraction_aliasing",
            version="v5",
            compiled_workflow=cw,
        )
        errs = validate_compiled_workflow_matches_workflow_version_document(wv, cw)
        self.assertEqual(errs, [], msg="\n".join(errs))
        g = execution_graph_from_compiled_workflow(cw)
        self.assertEqual(validate_execution_graph(g), [])

    def test_compiled_workflow_structural_signature_stable(self) -> None:
        a = {"tasks": [{"id": "t1", "function_external_id": "fn_a", "depends_on": ["x"]}]}
        b = {"tasks": [{"id": "t1", "function_external_id": "fn_a", "depends_on": ["x"]}]}
        self.assertEqual(compiled_workflow_structural_signature(a), compiled_workflow_structural_signature(b))
        c = {"tasks": [{"id": "t1", "function_external_id": "fn_a", "depends_on": []}]}
        self.assertNotEqual(compiled_workflow_structural_signature(a), compiled_workflow_structural_signature(c))


if __name__ == "__main__":
    unittest.main()
