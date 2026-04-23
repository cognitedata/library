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
    KeyExtractionEngine,
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
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="a",
                ),
                ExtractedKey(
                    value="T-1",
                    extraction_type=ExtractionType.FOREIGN_KEY_REFERENCE,
                    source_field="d",
                    confidence=0.9,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="b",
                ),
            ],
        )
        out = _dedupe_foreign_key_references(r)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["value"], "T-1")
        self.assertEqual(out[0]["confidence"], 0.9)


class TestExcludeSelfReferencingKeys(unittest.TestCase):
    """Foreign keys equal to a candidate value on the same instance are dropped."""

    def setUp(self):
        self.engine = KeyExtractionEngine(
            {
                "extraction_rules": [],
                "validation": {},
                "parameters": {"exclude_self_referencing_keys": True},
            }
        )

    def test_removes_fk_matching_any_candidate_keeps_others(self):
        tag = "45-TT-92506"
        r = ExtractionResult(
            entity_id="ts-1",
            entity_type="timeseries",
            candidate_keys=[
                ExtractedKey(
                    value=tag,
                    extraction_type=ExtractionType.CANDIDATE_KEY,
                    source_field="name",
                    confidence=1.0,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="cand",
                )
            ],
            foreign_key_references=[
                ExtractedKey(
                    value=tag,
                    extraction_type=ExtractionType.FOREIGN_KEY_REFERENCE,
                    source_field="name",
                    confidence=0.9,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="fk",
                ),
                ExtractedKey(
                    value="P-101",
                    extraction_type=ExtractionType.FOREIGN_KEY_REFERENCE,
                    source_field="name",
                    confidence=0.8,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="fk",
                ),
            ],
        )
        self.engine._exclude_self_referencing_keys(r)
        self.assertEqual(len(r.foreign_key_references), 1)
        self.assertEqual(r.foreign_key_references[0].value, "P-101")

    def test_no_candidates_leaves_foreign_keys_unchanged(self):
        r = ExtractionResult(
            entity_id="e",
            entity_type="asset",
            candidate_keys=[],
            foreign_key_references=[
                ExtractedKey(
                    value="X-100",
                    extraction_type=ExtractionType.FOREIGN_KEY_REFERENCE,
                    source_field="description",
                    confidence=1.0,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="a",
                ),
            ],
        )
        self.engine._exclude_self_referencing_keys(r)
        self.assertEqual(len(r.foreign_key_references), 1)

    def test_dedupe_pipeline_sees_filtered_fks(self):
        tag = "SAME-KEY"
        r = ExtractionResult(
            entity_id="e",
            entity_type="timeseries",
            candidate_keys=[
                ExtractedKey(
                    value=tag,
                    extraction_type=ExtractionType.CANDIDATE_KEY,
                    source_field="name",
                    confidence=1.0,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="c",
                )
            ],
            foreign_key_references=[
                ExtractedKey(
                    value=tag,
                    extraction_type=ExtractionType.FOREIGN_KEY_REFERENCE,
                    source_field="name",
                    confidence=0.5,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="f1",
                ),
                ExtractedKey(
                    value=tag,
                    extraction_type=ExtractionType.FOREIGN_KEY_REFERENCE,
                    source_field="name",
                    confidence=0.9,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="f2",
                ),
            ],
        )
        self.engine._exclude_self_referencing_keys(r)
        out = _dedupe_foreign_key_references(r)
        self.assertEqual(out, [])

    def test_skips_filter_when_timeseries_disabled_in_parameters(self):
        """Per-entity_type ``exclude_self_referencing_keys`` maps are no longer honored (``default`` only)."""
        engine = KeyExtractionEngine(
            {
                "extraction_rules": [],
                "validation": {},
                "parameters": {
                    "exclude_self_referencing_keys": {
                        "default": True,
                        "timeseries": False,
                    },
                },
            }
        )
        tag = "45-TT-92506"
        r = ExtractionResult(
            entity_id="ts-1",
            entity_type="timeseries",
            candidate_keys=[
                ExtractedKey(
                    value=tag,
                    extraction_type=ExtractionType.CANDIDATE_KEY,
                    source_field="name",
                    confidence=1.0,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="cand",
                )
            ],
            foreign_key_references=[
                ExtractedKey(
                    value=tag,
                    extraction_type=ExtractionType.FOREIGN_KEY_REFERENCE,
                    source_field="name",
                    confidence=0.9,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="fk",
                ),
            ],
        )
        engine._exclude_self_referencing_keys(r)
        self.assertEqual(len(r.foreign_key_references), 0)

        engine2 = KeyExtractionEngine(
            {
                "extraction_rules": [],
                "validation": {},
                "parameters": {"exclude_self_referencing_keys": {"default": False}},
            }
        )
        r2 = ExtractionResult(
            entity_id="ts-1",
            entity_type="timeseries",
            candidate_keys=list(r.candidate_keys),
            foreign_key_references=[
                ExtractedKey(
                    value=tag,
                    extraction_type=ExtractionType.FOREIGN_KEY_REFERENCE,
                    source_field="name",
                    confidence=0.9,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="fk",
                )
            ],
        )
        engine2._exclude_self_referencing_keys(r2)
        self.assertEqual(len(r2.foreign_key_references), 1)

    def test_source_override_false_keeps_fk_when_parameters_true(self):
        tag = "45-TT-92506"
        r = ExtractionResult(
            entity_id="ts-1",
            entity_type="timeseries",
            candidate_keys=[
                ExtractedKey(
                    value=tag,
                    extraction_type=ExtractionType.CANDIDATE_KEY,
                    source_field="name",
                    confidence=1.0,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="cand",
                )
            ],
            foreign_key_references=[
                ExtractedKey(
                    value=tag,
                    extraction_type=ExtractionType.FOREIGN_KEY_REFERENCE,
                    source_field="name",
                    confidence=0.9,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="fk",
                ),
            ],
        )
        self.engine._exclude_self_referencing_keys(r, source_override=False)
        self.assertEqual(len(r.foreign_key_references), 1)
        self.assertEqual(r.foreign_key_references[0].value, tag)

    def test_source_override_true_drops_fk_despite_parameters_timeseries_false(self):
        engine = KeyExtractionEngine(
            {
                "extraction_rules": [],
                "validation": {},
                "parameters": {
                    "exclude_self_referencing_keys": {
                        "default": True,
                        "timeseries": False,
                    },
                },
            }
        )
        tag = "SAME"
        r = ExtractionResult(
            entity_id="ts-1",
            entity_type="timeseries",
            candidate_keys=[
                ExtractedKey(
                    value=tag,
                    extraction_type=ExtractionType.CANDIDATE_KEY,
                    source_field="name",
                    confidence=1.0,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="c",
                )
            ],
            foreign_key_references=[
                ExtractedKey(
                    value=tag,
                    extraction_type=ExtractionType.FOREIGN_KEY_REFERENCE,
                    source_field="name",
                    confidence=0.9,
                    method=ExtractionMethod.REGEX_HANDLER,
                    rule_id="fk",
                ),
            ],
        )
        engine._exclude_self_referencing_keys(r, source_override=True)
        self.assertEqual(r.foreign_key_references, [])


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
