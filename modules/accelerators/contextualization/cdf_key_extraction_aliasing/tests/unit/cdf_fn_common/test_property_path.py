"""Tests for dot-path property reads (nested dicts and JSON strings)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.property_path import (  # noqa: E402
    get_value_by_property_path,
)


class TestGetValueByPropertyPath(unittest.TestCase):
    def test_nested_dict(self):
        root = {"a": {"b": {"c": 42}}}
        self.assertEqual(get_value_by_property_path(root, "a.b.c"), 42)

    def test_json_string_mid_path(self):
        root = {"meta": '{"tag": "P-1", "unit": "A"}'}
        self.assertEqual(get_value_by_property_path(root, "meta.tag"), "P-1")

    def test_json_string_invalid_returns_none(self):
        root = {"meta": "not json"}
        self.assertIsNone(get_value_by_property_path(root, "meta.tag"))

    def test_parse_json_disabled(self):
        root = {"meta": '{"tag": "x"}'}
        self.assertIsNone(
            get_value_by_property_path(root, "meta.tag", parse_json_strings=False)
        )

    def test_missing_segment(self):
        self.assertIsNone(get_value_by_property_path({"a": {}}, "a.b"))

    def test_empty_path(self):
        self.assertIsNone(get_value_by_property_path({"a": 1}, ""))


if __name__ == "__main__":
    unittest.main()
