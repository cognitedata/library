"""Tests for ``aliasing_rule_refs`` scope resolution."""

from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.aliasing_rule_refs import (  # noqa: E402
    expand_aliasing_pipeline_list,
    resolve_aliasing_pipeline_refs_in_scope_document,
)


class TestAliasingPipelineExpand(unittest.TestCase):
    def test_string_ref_expands_from_definitions(self) -> None:
        lookup = {
            "a": {"name": "a", "handler": "character_substitution", "config": {}},
        }
        out = expand_aliasing_pipeline_list(["a"], lookup, rules_path="")
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

    def test_pathways_sequential_string_refs(self) -> None:
        doc = {
            "aliasing_rule_definitions": {
                "r1": {
                    "name": "r1",
                    "handler": "character_substitution",
                    "config": {"substitutions": {"/": "-"}},
                },
                "r2": {
                    "name": "r2",
                    "handler": "character_substitution",
                    "config": {"substitutions": {"a": "b"}},
                },
            },
            "aliasing": {
                "config": {
                    "data": {
                        "pathways": {
                            "steps": [
                                {
                                    "mode": "sequential",
                                    "rules": ["r1", "r2"],
                                }
                            ]
                        }
                    }
                }
            },
        }
        d = copy.deepcopy(doc)
        resolve_aliasing_pipeline_refs_in_scope_document(d)
        steps = d["aliasing"]["config"]["data"]["pathways"]["steps"]
        self.assertEqual(steps[0]["rules"][0]["name"], "r1")
        self.assertEqual(steps[0]["rules"][1]["name"], "r2")
        self.assertNotIn("aliasing_rule_definitions", d)

    def test_pathways_parallel_dict_branches(self) -> None:
        doc = {
            "aliasing_rule_definitions": {
                "left": {
                    "name": "left",
                    "handler": "character_substitution",
                    "config": {"substitutions": {"x": "y"}},
                },
                "right": {
                    "name": "right",
                    "handler": "character_substitution",
                    "config": {"substitutions": {"p": "q"}},
                },
            },
            "aliasing": {
                "config": {
                    "data": {
                        "pathways": {
                            "steps": [
                                {
                                    "mode": "parallel",
                                    "branches": [
                                        {"rules": ["left"]},
                                        {"rules": [{"ref": "right"}]},
                                    ],
                                }
                            ]
                        }
                    }
                }
            },
        }
        d = copy.deepcopy(doc)
        resolve_aliasing_pipeline_refs_in_scope_document(d)
        branches = d["aliasing"]["config"]["data"]["pathways"]["steps"][0]["branches"]
        self.assertEqual(branches[0]["rules"][0]["name"], "left")
        self.assertEqual(branches[1]["rules"][0]["name"], "right")

    def test_pathways_parallel_list_branch(self) -> None:
        doc = {
            "aliasing_rule_definitions": {
                "only": {
                    "name": "only",
                    "handler": "character_substitution",
                    "config": {"substitutions": {"1": "2"}},
                }
            },
            "aliasing": {
                "config": {
                    "data": {
                        "pathways": {
                            "steps": [
                                {
                                    "mode": "parallel",
                                    "branches": [["only"]],
                                }
                            ]
                        }
                    }
                }
            },
        }
        d = copy.deepcopy(doc)
        resolve_aliasing_pipeline_refs_in_scope_document(d)
        branches = d["aliasing"]["config"]["data"]["pathways"]["steps"][0]["branches"]
        self.assertEqual(len(branches), 1)
        self.assertEqual(branches[0][0]["name"], "only")

    def test_extraction_pipeline_resolves_rule_only_in_pathways(self) -> None:
        """``aliasing_pipeline`` ids may match inline rules living under ``pathways`` only."""
        doc = {
            "key_extraction": {
                "config": {
                    "data": {
                        "extraction_rules": [
                            {
                                "name": "er1",
                                "rule_id": "er1",
                                "handler": "regex_handler",
                                "fields": [],
                                "aliasing_pipeline": ["only_inline"],
                            }
                        ]
                    }
                }
            },
            "aliasing": {
                "config": {
                    "data": {
                        "aliasing_rules": [],
                        "pathways": {
                            "steps": [
                                {
                                    "mode": "sequential",
                                    "rules": [
                                        {
                                            "name": "only_inline",
                                            "handler": "character_substitution",
                                            "enabled": True,
                                            "priority": 10,
                                            "preserve_original": True,
                                            "config": {"substitutions": {"x": "y"}},
                                        }
                                    ],
                                }
                            ]
                        },
                    }
                }
            },
        }
        d = copy.deepcopy(doc)
        resolve_aliasing_pipeline_refs_in_scope_document(d)
        pipe = d["key_extraction"]["config"]["data"]["extraction_rules"][0]["aliasing_pipeline"]
        self.assertEqual(pipe[0]["name"], "only_inline")
        self.assertEqual(pipe[0]["handler"], "character_substitution")

    def test_pathways_sequence_expansion(self) -> None:
        doc = {
            "aliasing_rule_definitions": {
                "a": {"name": "a", "handler": "character_substitution", "config": {}},
                "b": {"name": "b", "handler": "character_substitution", "config": {}},
            },
            "aliasing_rule_sequences": {"ab": ["a", "b"]},
            "aliasing": {
                "config": {
                    "data": {
                        "pathways": {
                            "steps": [
                                {
                                    "mode": "sequential",
                                    "rules": [{"sequence": "ab"}],
                                }
                            ]
                        }
                    }
                }
            },
        }
        d = copy.deepcopy(doc)
        resolve_aliasing_pipeline_refs_in_scope_document(d)
        rules = d["aliasing"]["config"]["data"]["pathways"]["steps"][0]["rules"]
        self.assertEqual([r["name"] for r in rules], ["a", "b"])
        self.assertNotIn("aliasing_rule_sequences", d)


if __name__ == "__main__":
    unittest.main()
