"""Unit tests for RegexOptions / regex flags used by extraction."""

import re
import sys
import unittest
from pathlib import Path

_LIB_ROOT = Path(__file__).resolve().parents[7]
sys.path.insert(0, str(_LIB_ROOT))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.RegexMethodParameter import (  # noqa: E402
    RegexOptions,
)


class TestRegexOptions(unittest.TestCase):
    def test_default_flags(self):
        f = RegexOptions().to_regex_flags()
        self.assertEqual(f & re.UNICODE, re.UNICODE)

    def test_combined_flags(self):
        f = RegexOptions(
            multiline=True,
            dotall=True,
            ignore_case=True,
            unicode=True,
        ).to_regex_flags()
        self.assertEqual(f & re.MULTILINE, re.MULTILINE)
        self.assertEqual(f & re.DOTALL, re.DOTALL)
        self.assertEqual(f & re.IGNORECASE, re.IGNORECASE)


if __name__ == "__main__":
    unittest.main()
