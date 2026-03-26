"""Unit tests for shared confidence scoring (regex / fixed-width helpers)."""

import sys
import unittest
from pathlib import Path

_LIB_ROOT = Path(__file__).resolve().parents[7]
sys.path.insert(0, str(_LIB_ROOT))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.confidence import (  # noqa: E402
    compute_confidence,
    compute_fixed_width_confidence,
)


class TestComputeConfidence(unittest.TestCase):
    def test_empty_returns_zero(self):
        self.assertEqual(compute_confidence("", "x"), 0.0)
        self.assertEqual(compute_confidence("src", ""), 0.0)

    def test_exact(self):
        self.assertEqual(compute_confidence("P-101", "P-101"), 1.0)

    def test_start_end_contains(self):
        self.assertEqual(compute_confidence("P-101-A", "P-101"), 0.90)  # start
        self.assertEqual(compute_confidence("MAIN-P-101", "P-101"), 0.90)  # end
        self.assertEqual(compute_confidence("X-P-101-Y", "P-101"), 0.80)  # contains

    def test_token_overlap_capped(self):
        # "tag" and "foo" in source as tokens — partial overlap
        self.assertLessEqual(
            compute_confidence("tag foo bar", "tag extra"), 0.70
        )

    def test_case_insensitive(self):
        self.assertEqual(compute_confidence("p-101", "P-101", case_sensitive=False), 1.0)


class TestComputeFixedWidthConfidence(unittest.TestCase):
    def test_base_and_required(self):
        v = compute_fixed_width_confidence("AB12", "string", required=True)
        self.assertGreaterEqual(v, 0.9)
        self.assertLessEqual(v, 1.0)


if __name__ == "__main__":
    unittest.main()
