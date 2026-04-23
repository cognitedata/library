"""Unit tests for fn_dm_reference_index pipeline helpers."""
import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.incremental_scope import (
    EXTERNAL_ID_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.pipeline import (
    FOREIGN_KEY_REFERENCES_JSON_COLUMN,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_reference_index.pipeline import (
    REFERENCE_KIND_DOCUMENT,
    REFERENCE_KIND_FOREIGN_KEY,
    _merge_remove_entity_postings,
    _parse_reference_json,
    inverted_row_key,
    normalize_lookup_token,
    persist_reference_index,
    source_snapshot_row_key,
)

_REFERENCE_INDEX_TEST_VALIDATION = {
    "max_aliases_per_tag": 50,
    "min_confidence": 0.01,
    "validation_rules": [
        {
            "name": "alias_shape_invalid",
            "priority": 0,
            "expression_match": "fullmatch",
            "match": {
                "expressions": [
                    {"pattern": r"^.{0,1}$", "description": "too short"},
                    {"pattern": r"^.{51,}$", "description": "too long"},
                    {"pattern": r"[^A-Za-z0-9]", "description": "non-alphanumeric"},
                ],
            },
            "confidence_modifier": {"mode": "explicit", "value": 0.0},
        },
    ],
}


def _fk_rows(*entities):
    fk_json = json.dumps([{"value": "TAG-1", "confidence": 0.9}])
    common = {
        RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
        "instance_space": "sp",
        "view_space": "cdf_cdm",
        "view_external_id": "CogniteFile",
        "view_version": "v1",
        FOREIGN_KEY_REFERENCES_JSON_COLUMN: fk_json,
    }
    return [
        SimpleNamespace(key=eid, columns={**common, EXTERNAL_ID_COLUMN: eid})
        for eid in entities
    ]


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

    def test_persist_reference_index_creates_target_raw_table(self):
        data = {
            "source_raw_db": "db_ke",
            "source_raw_table_key": "key_extraction_state",
            "reference_index_raw_db": "db_ke",
            "reference_index_raw_table": "reference_index",
            "config": {
                "config": {
                    "parameters": {"debug": True},
                    "data": {
                        "aliasing_rules": [],
                        "validation": _REFERENCE_INDEX_TEST_VALIDATION,
                    },
                }
            },
        }
        client = MagicMock()

        def _empty_source(*_a, **_k):
            return iter([[]])

        client.raw.rows.side_effect = _empty_source
        with patch(
            "modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_reference_index.pipeline.create_table_if_not_exists"
        ) as mock_ct:
            persist_reference_index(client, MagicMock(), data)
        mock_ct.assert_called_once()
        self.assertEqual(mock_ct.call_args[0][1], "db_ke")
        self.assertEqual(mock_ct.call_args[0][2], "reference_index")

    def test_skip_reference_index_ddl_skips_create(self):
        data = {
            "source_raw_db": "db_ke",
            "source_raw_table_key": "key_extraction_state",
            "reference_index_raw_db": "db_ke",
            "reference_index_raw_table": "reference_index",
            "skip_reference_index_ddl": True,
            "config": {
                "config": {
                    "parameters": {"debug": True},
                    "data": {
                        "aliasing_rules": [],
                        "validation": _REFERENCE_INDEX_TEST_VALIDATION,
                    },
                }
            },
        }
        client = MagicMock()
        client.raw.rows.side_effect = lambda *a, **k: iter([[]])
        with patch(
            "modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_reference_index.pipeline.create_table_if_not_exists"
        ) as mock_ct:
            persist_reference_index(client, MagicMock(), data)
        mock_ct.assert_not_called()

    def test_persist_batches_writes_chunked_insert_and_reuses_inv_cache(self):
        """Two entities share one lookup token: one inverted row, fewer retrieves, chunked flush."""
        config = {
            "config": {
                "parameters": {"debug": True},
                "data": {
                    "aliasing_rules": [],
                    "validation": _REFERENCE_INDEX_TEST_VALIDATION,
                },
            }
        }
        data = {
            "source_raw_db": "db_ke",
            "source_raw_table_key": "key_extraction_state",
            "reference_index_raw_db": "db_ke",
            "reference_index_raw_table": "reference_index",
            "reference_index_insert_batch_size": 2,
            "config": config,
        }
        row1, row2 = _fk_rows("f1", "f2")
        client = MagicMock()

        def _source_only(*_a, **_k):
            return iter([[row1, row2]])

        client.raw.rows.side_effect = _source_only
        client.raw.rows.retrieve.return_value = None
        with patch(
            "modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_reference_index.pipeline.create_table_if_not_exists"
        ):
            persist_reference_index(client, MagicMock(), data)
        self.assertEqual(client.raw.rows.retrieve.call_count, 3)
        self.assertEqual(client.raw.rows.insert.call_count, 2)
        self.assertEqual(data["reference_index_insert_batches"], 2)
        self.assertEqual(data["reference_index_source_list_chunks"], 1)
        chunks = [call.args[2] for call in client.raw.rows.insert.call_args_list]
        self.assertEqual(len(chunks[0]), 2)
        self.assertEqual(len(chunks[1]), 1)
        all_keys = {r.key for chunk in chunks for r in chunk}
        self.assertEqual(len(all_keys), 3)
        ik = inverted_row_key(normalize_lookup_token("TAG-1"))
        inv_chunk = next(r for chunk in chunks for r in chunk if r.key == ik)
        postings = json.loads(inv_chunk.columns["postings_json"])
        self.assertEqual(len(postings), 2)
        ext_ids = {p["source_external_id"] for p in postings}
        self.assertEqual(ext_ids, {"f1", "f2"})
        self.assertEqual(data["reference_index_inverted_writes"], 2)

    def test_prefetch_table_avoids_retrieve(self):
        row1, row2 = _fk_rows("f1", "f2")
        client = MagicMock()
        client.raw.rows.retrieve.return_value = None

        def _rows_router(db, table, **_kwargs):
            if table == "reference_index":
                return iter([[]])
            return iter([[row1, row2]])

        client.raw.rows.side_effect = _rows_router
        data = {
            "source_raw_db": "db_ke",
            "source_raw_table_key": "key_extraction_state",
            "reference_index_raw_db": "db_ke",
            "reference_index_raw_table": "reference_index",
            "reference_index_prefetch_table": True,
            "config": {
                "config": {
                    "parameters": {"debug": True},
                    "data": {
                        "aliasing_rules": [],
                        "validation": _REFERENCE_INDEX_TEST_VALIDATION,
                    },
                }
            },
        }
        with patch(
            "modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_reference_index.pipeline.create_table_if_not_exists"
        ):
            persist_reference_index(client, MagicMock(), data)
        client.raw.rows.retrieve.assert_not_called()

    def test_source_raw_pagination_two_chunks(self):
        row1, row2 = _fk_rows("f1", "f2")

        def _two_chunks(db, table, **_kwargs):
            if table == "reference_index":
                return iter([[]])
            return iter([[row1], [row2]])

        client = MagicMock()
        client.raw.rows.side_effect = _two_chunks
        client.raw.rows.retrieve.return_value = None
        data = {
            "source_raw_db": "db_ke",
            "source_raw_table_key": "key_extraction_state",
            "reference_index_raw_db": "db_ke",
            "reference_index_raw_table": "reference_index",
            "reference_index_prefetch_table": True,
            "source_raw_list_page_size": 1,
            "config": {
                "config": {
                    "parameters": {"debug": True},
                    "data": {
                        "aliasing_rules": [],
                        "validation": _REFERENCE_INDEX_TEST_VALIDATION,
                    },
                }
            },
        }
        with patch(
            "modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_reference_index.pipeline.create_table_if_not_exists"
        ):
            persist_reference_index(client, MagicMock(), data)
        self.assertEqual(data["reference_index_source_list_chunks"], 2)


if __name__ == "__main__":
    unittest.main()
