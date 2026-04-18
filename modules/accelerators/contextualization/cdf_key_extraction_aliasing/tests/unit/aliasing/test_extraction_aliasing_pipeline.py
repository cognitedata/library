"""Per-extraction-rule ``aliasing_pipeline`` tree execution on AliasingEngine."""

import copy
import sys
import unittest
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.cdf_adapter import (
    _DEFAULT_ALIASING_VALIDATION,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingEngine,
)


def _val(**kw):
    v = copy.deepcopy(_DEFAULT_ALIASING_VALIDATION)
    v.update(kw)
    return v


class TestExtractionAliasingPipeline(unittest.TestCase):
    def test_ordered_chain_feeds_forward(self) -> None:
        cfg = {
            "validation": _val(min_confidence=0.0, max_aliases_per_tag=100),
            "rules": [],
            "extraction_aliasing_pipelines": {
                "rule_a": [
                    {
                        "hierarchy": {
                            "mode": "ordered",
                            "children": [
                                {
                                    "name": "s1",
                                    "handler": "character_substitution",
                                    "config": {"substitutions": {"A": "B"}},
                                },
                                {
                                    "name": "s2",
                                    "handler": "character_substitution",
                                    "config": {"substitutions": {"B": "C"}},
                                },
                            ],
                        }
                    }
                ]
            },
        }
        eng = AliasingEngine(cfg)
        r = eng.generate_aliases(
            "AAX",
            "asset",
            context={"extraction_rule_name": "rule_a"},
        )
        self.assertIn("CCX", r.aliases)

    def test_concurrent_peers_do_not_cross_feed(self) -> None:
        cfg = {
            "validation": _val(min_confidence=0.0, max_aliases_per_tag=100),
            "rules": [],
            "extraction_aliasing_pipelines": {
                "rule_b": [
                    {
                        "hierarchy": {
                            "mode": "concurrent",
                            "children": [
                                {
                                    "name": "p1",
                                    "handler": "character_substitution",
                                    "priority": 10,
                                    "config": {"substitutions": {"X": "1"}},
                                },
                                {
                                    "name": "p2",
                                    "handler": "character_substitution",
                                    "priority": 20,
                                    "config": {"substitutions": {"X": "2"}},
                                },
                            ],
                        }
                    }
                ]
            },
        }
        eng = AliasingEngine(cfg)
        r = eng.generate_aliases(
            "XO",
            "asset",
            context={"extraction_rule_name": "rule_b"},
        )
        self.assertIn("1O", r.aliases)
        self.assertIn("2O", r.aliases)
        self.assertNotIn("12O", r.aliases)


if __name__ == "__main__":
    unittest.main()
