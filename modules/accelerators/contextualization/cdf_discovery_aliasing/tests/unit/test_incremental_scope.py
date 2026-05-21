"""Unit tests for incremental RAW scope helpers."""

import sys
from pathlib import Path

_FUNCS = Path(__file__).resolve().parents[3] / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.incremental_scope import (
    node_last_updated_time_ms,
    norm_workflow_status,
    scope_key_from_view_dict,
    scope_watermark_row_key,
)
from cdf_fn_common.cohort_storage import (
    instance_cohort_row_key,
    iter_cohort_entity_rows,
    node_cohort_table_name,
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


def test_instance_cohort_row_key_includes_scope():
    nid = "space:uuid-1"
    sk = "abc123"
    assert instance_cohort_row_key(nid, sk) == f"{sk}:{nid}"


def test_node_cohort_table_name_includes_run_and_node():
    rid = "20260101T000000.000000Z-abc123def456"
    tbl = node_cohort_table_name("discovery_state", rid, "tr")
    assert tbl.startswith("discovery_state__")
    assert tbl.endswith("__tr") or "_tr" in tbl


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


def test_iter_cohort_entity_rows_yields_entity_rows(monkeypatch) -> None:
    from cdf_fn_common.incremental_scope import RECORD_KIND_COLUMN

    class _R:
        def __init__(self, key: str, kind: str) -> None:
            self.key = key
            self.columns = {RECORD_KIND_COLUMN: kind}

    rows = [
        _R("scope:n1", "entity"),
        _R("wm", "watermark"),
    ]

    def _fake_iter(_client, _db, _tbl, chunk_size=2500):
        del _client, _db, _tbl, chunk_size
        yield from rows

    monkeypatch.setattr(
        "cdf_fn_common.cohort_storage.iter_raw_table_rows_chunked",
        _fake_iter,
    )
    out = list(iter_cohort_entity_rows(None, "d", "discovery_state__run__tr"))
    assert len(out) == 1
    assert out[0].key == "scope:n1"
