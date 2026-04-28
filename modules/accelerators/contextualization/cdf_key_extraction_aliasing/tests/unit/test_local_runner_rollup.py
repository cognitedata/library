"""Unit tests for local_runner extraction rollup used in CLI logging."""

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.local_runner.run import (
    _rollup_extraction_from_entities,
)


def test_rollup_empty():
    r = _rollup_extraction_from_entities({})
    assert r["entity_count"] == 0
    assert r["candidate_key_count"] == 0
    assert r["fk_ref_count"] == 0
    assert r["doc_ref_count"] == 0
    assert r["extraction_failed_count"] == 0


def test_rollup_mixed_entities():
    entities = {
        "e1": {
            "keys": {"name": {"T1": {"extraction_type": "candidate_key"}}},
            "foreign_key_references": [{"value": "FK1"}],
            "document_references": [{"value": "D1"}],
        },
        "e2": {
            "_extraction_failed": True,
            "keys": {},
            "foreign_key_references": [],
            "document_references": [],
        },
        "e3": {
            "keys": {
                "f": {
                    "A": {"extraction_type": "candidate_key"},
                    "B": {"extraction_type": "candidate_key"},
                }
            },
            "foreign_key_references": [{"value": "x"}, {"value": "y"}],
            "document_references": [],
        },
    }
    r = _rollup_extraction_from_entities(entities)
    assert r["entity_count"] == 3
    assert r["candidate_key_count"] == 3
    assert r["candidate_key_entities"] == 2
    assert r["fk_ref_count"] == 3
    assert r["entities_with_fk"] == 2
    assert r["doc_ref_count"] == 1
    assert r["entities_with_doc"] == 1
    assert r["extraction_failed_count"] == 1
