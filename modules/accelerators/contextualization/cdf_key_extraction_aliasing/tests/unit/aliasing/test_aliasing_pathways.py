"""Tests for sequential vs parallel aliasing pathways."""

import copy
import sys
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[7]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.cdf_adapter import (
    _DEFAULT_ALIASING_VALIDATION,
    _convert_yaml_direct_to_aliasing_config,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingEngine,
)


def _val(**overrides):
    v = copy.deepcopy(_DEFAULT_ALIASING_VALIDATION)
    v.update(overrides)
    return v


def _char_rule(name: str, subst: dict, priority: int = 10) -> dict:
    return {
        "name": name,
        "handler": "character_substitution",
        "enabled": True,
        "priority": priority,
        "preserve_original": True,
        "config": {
            "substitutions": subst,
            "cascade_substitutions": False,
            "max_aliases_per_input": 50,
        },
    }


class TestAliasingPathways(unittest.TestCase):
    def test_parallel_branches_fork_from_same_snapshot(self):
        """Parallel branches do not see sibling outputs; sequential rules stack on one growing set."""
        parallel_cfg = {
            "pathways": {
                "steps": [
                    {
                        "mode": "parallel",
                        "branches": [
                            {"rules": [_char_rule("a_to_c", {"a": "c"}, priority=10)]},
                            {"rules": [_char_rule("b_to_d", {"b": "d"}, priority=10)]},
                        ],
                    }
                ]
            },
            "rules": [],
            "validation": _val(max_aliases_per_tag=50),
        }
        eng_p = AliasingEngine(parallel_cfg)
        res_p = eng_p.generate_aliases("ab", "asset", {})
        self.assertIn("ab", res_p.aliases)
        self.assertIn("cb", res_p.aliases)
        self.assertIn("ad", res_p.aliases)
        self.assertNotIn("cd", res_p.aliases)

        seq_cfg = {
            "rules": [
                _char_rule("a_to_c", {"a": "c"}, priority=10),
                _char_rule("b_to_d", {"b": "d"}, priority=20),
            ],
            "validation": _val(max_aliases_per_tag=50),
        }
        eng_s = AliasingEngine(seq_cfg)
        res_s = eng_s.generate_aliases("ab", "asset", {})
        self.assertIn("cd", res_s.aliases)

    def test_sequential_pathway_step_order(self):
        """Explicit sequential pathway uses list order (not priority inversion)."""
        cfg = {
            "pathways": {
                "steps": [
                    {
                        "mode": "sequential",
                        "rules": [
                            _char_rule("first_z", {"x": "z"}, priority=99),
                            _char_rule("second_y", {"y": "w"}, priority=1),
                        ],
                    }
                ]
            },
            "rules": [],
            "validation": _val(max_aliases_per_tag=50),
        }
        eng = AliasingEngine(cfg)
        res = eng.generate_aliases("xy", "asset", {})
        self.assertIn("xy", res.aliases)
        self.assertIn("zy", res.aliases)
        self.assertIn("zw", res.aliases)

    def test_convert_yaml_includes_pathways(self):
        scope = {
            "config": {
                "data": {
                    "aliasing_rules": [],
                    "pathways": {
                        "steps": [
                            {
                                "mode": "sequential",
                                "rules": [
                                    {
                                        "name": "r1",
                                        "type": "character_substitution",
                                        "enabled": True,
                                        "priority": 10,
                                        "preserve_original": True,
                                        "config": {"substitutions": {"_": "-"}},
                                    }
                                ],
                            }
                        ]
                    },
                    "validation": {},
                }
            }
        }
        ac = _convert_yaml_direct_to_aliasing_config(scope)
        self.assertIn("pathways", ac)
        self.assertEqual(ac["pathways"]["steps"][0]["mode"], "sequential")
        self.assertEqual(ac["pathways"]["steps"][0]["rules"][0]["handler"], "character_substitution")

    def test_pathway_input_previous_feeds_only_prior_handler_output(self):
        """input: previous uses transform() output of prior rule, not full merged set."""
        cfg = {
            "pathways": {
                "steps": [
                    {
                        "mode": "sequential",
                        "rules": [
                            {
                                "name": "upper_only",
                                "handler": "case_transformation",
                                "enabled": True,
                                "priority": 10,
                                "preserve_original": True,
                                "config": {"operation": "upper"},
                            },
                            {
                                "name": "dash_second",
                                "handler": "character_substitution",
                                "enabled": True,
                                "priority": 20,
                                "input": "previous",
                                "preserve_original": True,
                                "config": {
                                    "substitutions": {"-": "_"},
                                    "cascade_substitutions": False,
                                    "max_aliases_per_input": 50,
                                },
                            },
                        ],
                    }
                ]
            },
            "rules": [],
            "validation": _val(max_aliases_per_tag=50),
        }
        eng = AliasingEngine(cfg)
        res = eng.generate_aliases("a-b", "asset", {})
        self.assertIn("a-b", res.aliases)
        self.assertIn("A-B", res.aliases)
        self.assertIn("A_B", res.aliases)
        # Cumulative second rule would also substitute on "a-b" -> "a_b"; previous does not.
        self.assertNotIn("a_b", res.aliases)


if __name__ == "__main__":
    unittest.main()
