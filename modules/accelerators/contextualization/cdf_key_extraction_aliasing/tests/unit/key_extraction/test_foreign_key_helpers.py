"""Unit tests for foreign key deduplication and RAW FK map loading."""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

_LIB_ROOT = Path(__file__).resolve().parents[7]
sys.path.insert(0, str(_LIB_ROOT))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (  # noqa: E402
    ExtractionResult,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.pipeline import (  # noqa: E402
    FOREIGN_KEY_REFERENCES_JSON_COLUMN,
    _dedupe_foreign_key_references,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.DataStructures import (  # noqa: E402
    ExtractedKey,
    ExtractionMethod,
    ExtractionType,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_alias_persistence.pipeline import (  # noqa: E402
    _load_foreign_key_map_from_raw,
)


class TestDedupeForeignKeyReferences(unittest.TestCase):
    def test_keeps_higher_confidence(self):
        r = ExtractionResult(
            entity_id="e",
            entity_type="file",
            candidate_keys=[],
            foreign_key_references=[
                ExtractedKey(
                    value="T-1",
                    extraction_type=ExtractionType.FOREIGN_KEY_REFERENCE,
                    source_field="d",
                    confidence=0.5,
                    method=ExtractionMethod.REGEX,
                    rule_id="a",
                ),
                ExtractedKey(
                    value="T-1",
                    extraction_type=ExtractionType.FOREIGN_KEY_REFERENCE,
                    source_field="d",
                    confidence=0.9,
                    method=ExtractionMethod.REGEX,
                    rule_id="b",
                ),
            ],
        )
        out = _dedupe_foreign_key_references(r)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["value"], "T-1")
        self.assertEqual(out[0]["confidence"], 0.9)


class TestLoadForeignKeyMapFromRaw(unittest.TestCase):
    def test_parses_json_column(self):
        row = MagicMock()
        row.key = "file_ext_id"
        row.columns = {
            FOREIGN_KEY_REFERENCES_JSON_COLUMN: json.dumps(
                [{"value": "A-1", "confidence": 1.0}, {"value": "B-2", "confidence": 0.5}]
            )
        }
        client = MagicMock()
        client.raw.rows.list.return_value = [row]
        logger = MagicMock()
        m = _load_foreign_key_map_from_raw(
            client, "db", "tbl", logger, limit=10
        )
        self.assertEqual(m["file_ext_id"], ["A-1", "B-2"])


if __name__ == "__main__":
    unittest.main()
