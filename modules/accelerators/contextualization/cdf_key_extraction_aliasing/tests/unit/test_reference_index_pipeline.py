"""Unit tests for fn_dm_reference_index pipeline helpers."""
import unittest

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_reference_index.pipeline import (
    REFERENCE_KIND_DOCUMENT,
    REFERENCE_KIND_FOREIGN_KEY,
    _merge_remove_entity_postings,
    _parse_reference_json,
    inverted_row_key,
    normalize_lookup_token,
    source_snapshot_row_key,
)


class TestReferenceIndexHelpers(unittest.TestCase):
    def test_normalize_lookup_token(self):
        self.assertEqual(normalize_lookup_token("  P-101  "), "p-101")

    def test_inverted_row_key_stable(self):
        a = inverted_row_key(normalize_lookup_token("X-1"))
        b = inverted_row_key(normalize_lookup_token("X-1"))
        self.assertEqual(a, b)
        self.assertTrue(a.startswith("t_"))

    def test_source_snapshot_row_key_stable(self):
        k = source_snapshot_row_key("sp", "ext1")
        self.assertTrue(k.startswith("ssrc_"))

    def test_parse_reference_json(self):
        self.assertEqual(_parse_reference_json(""), [])
        raw = '[{"value":"T1","confidence":0.9}]'
        out = _parse_reference_json(raw)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["value"], "T1")

    def test_merge_remove_entity_postings(self):
        postings = [
            {
                "source_external_id": "a",
                "source_instance_space": "s",
                "canonical_value": "v1",
                "reference_kind": REFERENCE_KIND_FOREIGN_KEY,
            },
            {
                "source_external_id": "b",
                "source_instance_space": "s",
                "canonical_value": "v2",
                "reference_kind": REFERENCE_KIND_FOREIGN_KEY,
            },
        ]
        rem = _merge_remove_entity_postings(postings, "a", "s")
        self.assertEqual(len(rem), 1)
        self.assertEqual(rem[0]["source_external_id"], "b")

    def test_reference_kind_constants(self):
        self.assertEqual(REFERENCE_KIND_FOREIGN_KEY, "foreign_key")
        self.assertEqual(REFERENCE_KIND_DOCUMENT, "document")


if __name__ == "__main__":
    unittest.main()
