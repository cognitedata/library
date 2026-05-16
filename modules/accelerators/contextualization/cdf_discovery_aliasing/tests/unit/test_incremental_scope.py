"""Unit tests for incremental RAW scope helpers."""

import sys
from pathlib import Path

_FUNCS = Path(__file__).resolve().parents[3] / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.incremental_scope import (
    cohort_row_key,
    node_last_updated_time_ms,
    norm_workflow_status,
    scope_key_from_view_dict,
    scope_watermark_row_key,
)


def test_scope_key_stable():
    k1 = scope_key_from_view_dict(
        {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteFile",
            "view_version": "v1",
            "instance_space": "sp-x",
            "filters": [{"operator": "IN", "target_property": "mimeType", "values": ["application/pdf"]}],
        }
    )
    k2 = scope_key_from_view_dict(
        {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteFile",
            "view_version": "v1",
            "instance_space": "sp-x",
            "filters": [{"operator": "IN", "target_property": "mimeType", "values": ["application/pdf"]}],
        }
    )
    assert k1 == k2
    assert len(k1) == 32


def test_cohort_row_key_includes_scope():
    rid = "20260101T000000.000000Z"
    nid = "space:uuid-1"
    sk = "abc123"
    assert cohort_row_key(rid, nid, sk) == f"{rid}:{sk}:{nid}"


def test_norm_workflow_status():
    assert norm_workflow_status("  EXTRACTED ") == "extracted"
    assert norm_workflow_status(None) == ""


def test_watermark_key():
    assert scope_watermark_row_key("deadbeef").startswith("scope_wm_")


class _InstNested:
    def dump(self):
        return {"node": {"lastUpdatedTime": 1_700_000_000_000}}


def test_node_last_updated_time_ms_nested_dump():
    assert node_last_updated_time_ms(_InstNested()) == 1_700_000_000_000


def test_load_latest_hash_includes_detected_rows(monkeypatch) -> None:
    from cdf_fn_common.incremental_scope import (
        EXTRACTION_INPUTS_HASH_COLUMN,
        NODE_INSTANCE_ID_COLUMN,
        RECORD_KIND_COLUMN,
        RUN_ID_COLUMN,
        SCOPE_KEY_COLUMN,
        WORKFLOW_STATUS_COLUMN,
        WORKFLOW_STATUS_UPDATED_AT_COLUMN,
        load_latest_hash_by_node_for_scope,
    )

    class _R:
        def __init__(self, cols: dict) -> None:
            self.key = "k"
            self.columns = cols

    rows = [
        _R(
            {
                RECORD_KIND_COLUMN: "entity",
                SCOPE_KEY_COLUMN: "sk1",
                NODE_INSTANCE_ID_COLUMN: "sp:uuid-1",
                WORKFLOW_STATUS_COLUMN: "detected",
                WORKFLOW_STATUS_UPDATED_AT_COLUMN: "2020-01-02T00:00:00.000Z",
                RUN_ID_COLUMN: "run_b",
                EXTRACTION_INPUTS_HASH_COLUMN: "hash_newer",
            }
        ),
        _R(
            {
                RECORD_KIND_COLUMN: "entity",
                SCOPE_KEY_COLUMN: "sk1",
                NODE_INSTANCE_ID_COLUMN: "sp:uuid-1",
                WORKFLOW_STATUS_COLUMN: "extracted",
                WORKFLOW_STATUS_UPDATED_AT_COLUMN: "2020-01-01T00:00:00.000Z",
                RUN_ID_COLUMN: "run_a",
                EXTRACTION_INPUTS_HASH_COLUMN: "hash_older",
            }
        ),
    ]

    def _fake_iter(_client, _db, _tbl, chunk_size=2500):
        del _client, _db, _tbl, chunk_size
        yield from rows

    monkeypatch.setattr(
        "cdf_fn_common.incremental_scope.iter_raw_table_rows_chunked",
        _fake_iter,
    )
    out = load_latest_hash_by_node_for_scope(None, "db", "tbl", "sk1")
    assert out == {"sp:uuid-1": "hash_newer"}


def test_build_latest_hash_index_matches_load_latest_per_scope(monkeypatch) -> None:
    """Table-wide index in one scan matches per-scope slices from load_latest_hash_by_node_for_scope."""
    from cdf_fn_common.incremental_scope import (
        EXTRACTION_INPUTS_HASH_COLUMN,
        NODE_INSTANCE_ID_COLUMN,
        RECORD_KIND_COLUMN,
        RUN_ID_COLUMN,
        SCOPE_KEY_COLUMN,
        WORKFLOW_STATUS_COLUMN,
        WORKFLOW_STATUS_UPDATED_AT_COLUMN,
        build_latest_hash_index_for_table,
        load_latest_hash_by_node_for_scope,
    )

    class _R:
        def __init__(self, cols: dict) -> None:
            self.key = "k"
            self.columns = cols

    rows = [
        _R(
            {
                RECORD_KIND_COLUMN: "entity",
                SCOPE_KEY_COLUMN: "sk_a",
                NODE_INSTANCE_ID_COLUMN: "sp:node-1",
                WORKFLOW_STATUS_COLUMN: "extracted",
                WORKFLOW_STATUS_UPDATED_AT_COLUMN: "2020-01-01T00:00:00.000Z",
                RUN_ID_COLUMN: "r1",
                EXTRACTION_INPUTS_HASH_COLUMN: "hash_a1",
            }
        ),
        _R(
            {
                RECORD_KIND_COLUMN: "entity",
                SCOPE_KEY_COLUMN: "sk_b",
                NODE_INSTANCE_ID_COLUMN: "sp:node-2",
                WORKFLOW_STATUS_COLUMN: "persisted",
                WORKFLOW_STATUS_UPDATED_AT_COLUMN: "2020-01-02T00:00:00.000Z",
                RUN_ID_COLUMN: "r2",
                EXTRACTION_INPUTS_HASH_COLUMN: "hash_b1",
            }
        ),
    ]

    def _fake_iter(_client, _db, _tbl, chunk_size=2500):
        del _client, _db, _tbl, chunk_size
        yield from rows

    monkeypatch.setattr(
        "cdf_fn_common.incremental_scope.iter_raw_table_rows_chunked",
        _fake_iter,
    )
    full = build_latest_hash_index_for_table(None, "db", "tbl")
    assert load_latest_hash_by_node_for_scope(None, "db", "tbl", "sk_a") == full.get("sk_a", {})
    assert load_latest_hash_by_node_for_scope(None, "db", "tbl", "sk_b") == full.get("sk_b", {})
    assert full == {
        "sk_a": {"sp:node-1": "hash_a1"},
        "sk_b": {"sp:node-2": "hash_b1"},
    }


def test_inter_node_cohort_key_prefix() -> None:
    from cdf_fn_common.incremental_scope import inter_node_cohort_key_prefix

    assert inter_node_cohort_key_prefix("run1") == "run1:"
    assert inter_node_cohort_key_prefix("  x  ") == "x:"
    assert inter_node_cohort_key_prefix("") == ""


def test_iter_inter_node_strict_stops_after_key_prefix_span(monkeypatch) -> None:
    from cdf_fn_common.incremental_scope import iter_inter_node_raw_rows_for_filter_run

    class _R:
        def __init__(self, key: str) -> None:
            self.key = key
            self.columns = {}

    rows = [
        _R("aa"),
        _R("r9:scope:n1"),
        _R("r9:scope:n2"),
        _R("zz"),
        _R("r9:orphan_should_not_yield"),
    ]

    def _fake_iter(_client, _db, _tbl, chunk_size=2500):
        del _client, _db, _tbl, chunk_size
        yield from rows

    monkeypatch.setattr(
        "cdf_fn_common.incremental_scope.iter_raw_table_rows_chunked",
        _fake_iter,
    )
    out = list(
        iter_inter_node_raw_rows_for_filter_run(
            None, "d", "t", "r9", strict_key_prefix_only=True
        )
    )
    assert [r.key for r in out] == ["r9:scope:n1", "r9:scope:n2"]


def test_iter_inter_node_nonstrict_includes_run_id_without_prefix(monkeypatch) -> None:
    from cdf_fn_common.incremental_scope import (
        RUN_ID_COLUMN,
        iter_inter_node_raw_rows_for_filter_run,
    )

    class _R:
        def __init__(self, key: str, rid: str) -> None:
            self.key = key
            self.columns = {RUN_ID_COLUMN: rid}

    rows = [_R("legacy-key", "r9"), _R("r9:sk:n1", "r9")]

    def _fake_iter(_client, _db, _tbl, chunk_size=2500):
        del _client, _db, _tbl, chunk_size
        yield from rows

    monkeypatch.setattr(
        "cdf_fn_common.incremental_scope.iter_raw_table_rows_chunked",
        _fake_iter,
    )
    out = list(
        iter_inter_node_raw_rows_for_filter_run(
            None, "d", "t", "r9", strict_key_prefix_only=False
        )
    )
    assert {r.key for r in out} == {"legacy-key", "r9:sk:n1"}

