"""Tests for cdf_fn_common.pipeline_io."""

import sys
import unittest
from pathlib import Path

# Repo root (…/library) so `modules.accelerators…` imports resolve like other module tests.
_REPO_ROOT = Path(__file__).resolve().parents[6]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.pipeline_io import (
    parse_rule_pipeline_io,
    pipeline_io_dict_for_engine,
)


class TestPipelineIO(unittest.TestCase):
    def test_defaults(self):
        pin, pout, preserve = parse_rule_pipeline_io({})
        self.assertEqual(pin, "cumulative")
        self.assertEqual(pout, "merge")
        self.assertTrue(preserve)

    def test_output_replace_implies_preserve_false(self):
        pin, pout, preserve = parse_rule_pipeline_io({"output": "replace"})
        self.assertEqual(pout, "replace")
        self.assertFalse(preserve)

    def test_explicit_preserve_wins_over_output(self):
        pin, pout, preserve = parse_rule_pipeline_io(
            {"output": "replace", "preserve_original": True}
        )
        self.assertTrue(preserve)

    def test_engine_dict_merges_io(self):
        d = pipeline_io_dict_for_engine({"input": "previous", "output": "merge"})
        self.assertEqual(d["pipeline_input"], "previous")
        self.assertEqual(d["pipeline_output"], "merge")


if __name__ == "__main__":
    unittest.main()
