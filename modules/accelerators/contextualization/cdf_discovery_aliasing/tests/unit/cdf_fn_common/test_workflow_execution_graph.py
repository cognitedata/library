"""Tests for workflow.execution.graph.yaml and WorkflowVersion alignment."""

import sys
import unittest
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
# Resolve cdf_fn_common without importing cdf_discovery_aliasing package __init__.
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
            nodes=["kea__vq", "kea__orphan"],
            node_roles={},
            edges=[
                ExecutionGraphEdge(
                    from_id="kea__vq",
                    to_id="kea__orphan",
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
            "compile_workflow_dag": "canvas",
            "canvas": {
                "nodes": [
                    {"id": "st", "kind": "start"},
                    {
                        "id": "vq",
                        "kind": "query_view",
                        "data": {
                            "config": {
                                "description": "q",
                                "view_space": "cdf_cdm",
                                "view_external_id": "CogniteFile",
                                "view_version": "v1",
                            }
                        },
                    },
                    {
                        "id": "tr",
                        "kind": "transform",
                        "data": {"config": {"description": "tr"}},
                    },
                    {"id": "en", "kind": "end"},
                ],
                "edges": [
                    {"source": "st", "target": "vq"},
                    {"source": "vq", "target": "tr"},
                    {"source": "tr", "target": "en"},
                ],
            },
        }
        cw = compile_canvas_dag(doc)
        wv = build_workflow_version_document(
            workflow_external_id="discovery_chain",
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

    def test_build_workflow_version_task_name_from_canvas_label(self) -> None:
        doc = {
            "schemaVersion": 1,
            "compile_workflow_dag": "canvas",
            "canvas": {
                "nodes": [
                    {"id": "st", "kind": "start"},
                    {
                        "id": "vq",
                        "kind": "query_view",
                        "data": {
                            "label": "Asset Query",
                            "config": {
                                "description": "q0",
                                "view_space": "cdf_cdm",
                                "view_external_id": "CogniteAsset",
                                "view_version": "v1",
                            },
                        },
                    },
                    {"id": "en", "kind": "end"},
                ],
                "edges": [
                    {"source": "st", "target": "vq"},
                    {"source": "vq", "target": "en"},
                ],
            },
        }
        cw = compile_canvas_dag(doc)
        vq_ir = next(t for t in cw["tasks"] if t.get("canvas_node_id") == "vq")
        self.assertEqual(vq_ir.get("label"), "Asset Query")
        wv = build_workflow_version_document(
            workflow_external_id="key_extraction_aliasing",
            version="v5",
            compiled_workflow=cw,
            module_root=_MODULE_ROOT,
        )
        wtask = next(
            t
            for t in wv["workflowDefinition"]["tasks"]
            if t["externalId"] == vq_ir["id"]
        )
        self.assertEqual(wtask["name"], "Asset Query")
        self.assertIn("DM view query", wtask["description"])

    def test_compile_canvas_discovery_chain(self) -> None:
        doc = {
            "schemaVersion": 1,
            "compile_workflow_dag": "canvas",
            "canvas": {
                "nodes": [
                    {"id": "st", "kind": "start"},
                    {
                        "id": "vq",
                        "kind": "query_view",
                        "data": {
                            "config": {
                                "description": "q0",
                                "view_space": "cdf_cdm",
                                "view_external_id": "CogniteAsset",
                                "view_version": "v1",
                            }
                        },
                    },
                    {
                        "id": "tr",
                        "kind": "transform",
                        "data": {"config": {"description": "t0"}},
                    },
                    {
                        "id": "va",
                        "kind": "validation",
                        "data": {"config": {"description": "v1"}},
                    },
                    {"id": "en", "kind": "end"},
                ],
                "edges": [
                    {"source": "st", "target": "vq"},
                    {"source": "vq", "target": "tr"},
                    {"source": "tr", "target": "va"},
                    {"source": "va", "target": "en"},
                ],
            },
        }
        cw = compile_canvas_dag(doc)
        tasks = cw["tasks"]
        self.assertEqual(len(tasks), 4)
        by_cn = {t["canvas_node_id"]: t for t in tasks if t.get("canvas_node_id")}
        tid_vq = by_cn["vq"]["id"]
        tid_tr = by_cn["tr"]["id"]
        tid_va = by_cn["va"]["id"]
        self.assertEqual(tid_vq, "kea__query_view__q0")
        self.assertEqual(tid_tr, "kea__transform__t0")
        self.assertEqual(tid_va, "kea__validate__v1")
        by_id = {t["id"]: t for t in tasks}
        self.assertNotIn("kea__incremental_state", by_id)
        self.assertEqual(by_id[tid_vq]["function_external_id"], "fn_dm_view_query")
        self.assertEqual(by_id[tid_vq]["depends_on"], [])
        self.assertEqual(
            by_id[tid_vq]["payload"],
            {
                "config": {
                    "description": "q0",
                    "view_space": "cdf_cdm",
                    "view_external_id": "CogniteAsset",
                    "view_version": "v1",
                }
            },
        )
        self.assertEqual(by_id[tid_tr]["depends_on"], [tid_vq])
        self.assertEqual(by_id[tid_tr]["payload"], {"config": {"description": "t0"}})
        self.assertEqual(by_id[tid_va]["depends_on"], [tid_tr])
        self.assertEqual(by_id[tid_va]["payload"], {"config": {"description": "v1"}})
        cl = by_id["kea__discovery_raw_cleanup"]
        self.assertEqual(cl["function_external_id"], "fn_dm_discovery_raw_cleanup")
        self.assertEqual(cl["depends_on"], [tid_va])
        wv = build_workflow_version_document(
            workflow_external_id="key_extraction_aliasing",
            version="v5",
            compiled_workflow=cw,
            module_root=_MODULE_ROOT,
        )
        werrs = validate_compiled_workflow_matches_workflow_version_document(wv, cw)
        self.assertEqual(werrs, [], msg="\n".join(werrs))
        wtasks = wv["workflowDefinition"]["tasks"]
        by_ext = {t["parameters"]["function"]["externalId"]: t for t in wtasks}
        self.assertEqual(by_ext["fn_dm_view_query"]["name"], "View query")
        self.assertEqual(by_ext["fn_dm_validate"]["name"], "Validate")
        self.assertEqual(by_ext["fn_dm_validate"]["timeout"], 7200)
        self.assertIn("fn_dm_discovery_raw_cleanup", by_ext)

    def test_discovery_fixture_scope_compiles_and_ir_graph_valid(self) -> None:
        """Fixture YAML mirrors a small discovery canvas; IR must be acyclic and match WorkflowVersion."""
        _SCRIPTS = _MODULE_ROOT / "scripts"
        if str(_SCRIPTS) not in sys.path:
            sys.path.insert(0, str(_SCRIPTS))
        from scope_build.scope_document_load import load_scope_document_dict_for_build  # noqa: E402

        path = _MODULE_ROOT / "tests" / "fixtures" / "scope_discovery_linear.config.yaml"
        doc = load_scope_document_dict_for_build(path)
        cw = compiled_workflow_for_scope_document(doc)
        g = execution_graph_from_compiled_workflow(cw)
        self.assertEqual(validate_execution_graph(g), [])
        wv = build_workflow_version_document(
            workflow_external_id="discovery_fixture",
            version="v5",
            compiled_workflow=cw,
            module_root=_MODULE_ROOT,
        )
        errs = validate_compiled_workflow_matches_workflow_version_document(wv, cw)
        self.assertEqual(errs, [], msg="\n".join(errs))


if __name__ == "__main__":
    unittest.main()
