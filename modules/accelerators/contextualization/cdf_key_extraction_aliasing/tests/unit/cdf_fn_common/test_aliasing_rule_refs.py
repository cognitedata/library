"""Tests for ``aliasing_rule_refs`` scope resolution."""

from __future__ import annotations

import copy
import unittest

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.aliasing_rule_refs import (
    expand_aliasing_pipeline_list,
    resolve_aliasing_pipeline_refs_in_scope_document,
)


class TestAliasingPipelineExpand(unittest.TestCase):
    def test_string_ref_expands_from_definitions(self) -> None:
        lookup = {
            "a": {"name": "a", "handler": "character_substitution", "config": {}},
        }
        out = expand_aliasing_pipeline_list(["a"], lookup, context="")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["handler"], "character_substitution")

    def test_resolve_mutates_extraction_rule_pipeline(self) -> None:
        doc = {
            "aliasing_rule_definitions": {
                "r1": {
                    "name": "r1",
                    "handler": "character_substitution",
                    "config": {"substitutions": {"_": "-"}},
                }
            },
            "key_extraction": {
                "config": {
                    "data": {
                        "extraction_rules": [
                            {
                                "name": "er1",
                                "rule_id": "er1",
                                "handler": "regex_handler",
                                "fields": [],
                                "aliasing_pipeline": ["r1"],
                            }
                        ]
                    }
                }
            },
        }
        d = copy.deepcopy(doc)
        resolve_aliasing_pipeline_refs_in_scope_document(d)
        rules = d["key_extraction"]["config"]["data"]["extraction_rules"]
        self.assertEqual(rules[0]["aliasing_pipeline"][0]["name"], "r1")
        self.assertNotIn("aliasing_rule_definitions", d)


if __name__ == "__main__":
    unittest.main()
