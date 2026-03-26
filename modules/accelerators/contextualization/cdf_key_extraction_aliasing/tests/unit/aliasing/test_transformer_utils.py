"""Unit tests for fn_dm_aliasing.engine.transformer_utils helpers."""

import sys
import unittest
from pathlib import Path

_LIB_ROOT = Path(__file__).resolve().parents[7]
sys.path.insert(0, str(_LIB_ROOT))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.transformer_utils import (  # noqa: E402
    extract_equipment_number,
    extract_hierarchical_structure,
    extract_tag_structure,
    generate_separator_variants,
    normalize_separators,
)


class TestTransformerUtils(unittest.TestCase):
    def test_extract_equipment_number(self):
        self.assertEqual(extract_equipment_number("P-10001"), "10001")
        self.assertIsNone(extract_equipment_number("NO-NUMBER"))

    def test_extract_tag_structure(self):
        self.assertEqual(
            extract_tag_structure("P-10001")["number"],
            "10001",
        )
        self.assertIsNone(extract_tag_structure("invalid"))

    def test_generate_separator_variants(self):
        v = generate_separator_variants("P-10001")
        self.assertIn("P-10001", v)
        self.assertIn("P_10001", v)

    def test_normalize_separators(self):
        self.assertEqual(normalize_separators("P_10001"), "P-10001")

    def test_extract_hierarchical_structure(self):
        h = extract_hierarchical_structure("10-P-10001")
        self.assertIsNotNone(h)
        self.assertEqual(h["equipment"], "P")
        self.assertIsNone(extract_hierarchical_structure("P-10001"))


if __name__ == "__main__":
    unittest.main()
