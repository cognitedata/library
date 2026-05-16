"""Tests for ``workflow_associations`` (v1 scope ``associations``)."""

from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.workflow_associations import (  # noqa: E402
    apply_canvas_dict_to_scope_associations,
    coerce_association_source_view_index,
    collect_source_view_to_extraction_pairs_from_canvas_dict,
    parse_source_view_to_extraction_pairs,
    validate_workflow_associations,
)


class TestWorkflowAssociations(unittest.TestCase):
    def test_validate_unknown_rule(self) -> None:
        doc = {
            "associations": [
                {"kind": "source_view_to_extraction", "source_view_index": 0, "extraction_rule_name": "missing"}
            ],
            "source_views": [{"entity_type": "asset"}],
            "key_extraction": {"config": {"data": {"extraction_rules": [{"name": "ok", "handler": "regex_handler", "fields": []}]}}},
        }
        errs = validate_workflow_associations(doc)
        self.assertTrue(any("missing" in e for e in errs))

    def test_collect_pairs_from_canvas_dict(self) -> None:
        canvas = {
            "schemaVersion": 1,
            "nodes": [
                {
                    "id": "sv_0",
                    "kind": "source_view",
                    "position": {"x": 0, "y": 0},
                    "data": {"label": "SV", "ref": {"source_view_index": 0}},
                },
                {
                    "id": "ext_x",
                    "kind": "extraction",
                    "position": {"x": 1, "y": 0},
                    "data": {"label": "E", "ref": {"extraction_rule_name": "rule_a"}},
                },
            ],
            "edges": [
                {
                    "id": "e1",
                    "source": "sv_0",
                    "target": "ext_x",
                    "kind": "data",
                    "source_handle": "out",
                    "target_handle": "in",
                }
            ],
        }
        pairs = collect_source_view_to_extraction_pairs_from_canvas_dict(canvas)
        self.assertEqual(pairs, [(0, "rule_a")])

    def test_apply_canvas_to_scope(self) -> None:
        scope = {
            "source_views": [{"entity_type": "asset"}],
            "key_extraction": {
                "config": {
                    "data": {
                        "extraction_rules": [
                            {"name": "rule_a", "handler": "regex_handler", "fields": [], "scope_filters": {}}
                        ]
                    }
                }
            },
        }
        canvas = {
            "nodes": [
                {
                    "id": "sv_0",
                    "kind": "source_view",
                    "position": {"x": 0, "y": 0},
                    "data": {"ref": {"source_view_index": 0}},
                },
                {
                    "id": "ext_x",
                    "kind": "extraction",
                    "position": {"x": 1, "y": 0},
                    "data": {"ref": {"extraction_rule_name": "rule_a"}},
                },
            ],
            "edges": [{"id": "e1", "source": "sv_0", "target": "ext_x", "kind": "data"}],
        }
        d = copy.deepcopy(scope)
        apply_canvas_dict_to_scope_associations(canvas, d)
        self.assertEqual(len(parse_source_view_to_extraction_pairs(d)), 1)
        errs = validate_workflow_associations(d)
        self.assertEqual(errs, [])

    def test_parse_accepts_string_numeric_source_view_index(self) -> None:
        doc = {
            "associations": [
                {
                    "kind": "source_view_to_extraction",
                    "source_view_index": "0",
                    "extraction_rule_name": "r1",
                }
            ]
        }
        self.assertEqual(parse_source_view_to_extraction_pairs(doc), [(0, "r1")])

    def test_coerce_association_source_view_index(self) -> None:
        self.assertEqual(coerce_association_source_view_index(2), 2)
        self.assertEqual(coerce_association_source_view_index(2.0), 2)
        self.assertEqual(coerce_association_source_view_index(" 3 "), 3)
        self.assertIsNone(coerce_association_source_view_index("x"))
        self.assertIsNone(coerce_association_source_view_index(True))


if __name__ == "__main__":
    unittest.main()
