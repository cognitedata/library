"""Unit tests for incremental RAW scope helpers."""

import sys
from pathlib import Path

_FUNCS = Path(__file__).resolve().parents[2] / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.incremental_scope import (
    ListInstancesStats,
    list_all_instances,
    node_last_updated_time_ms,
    norm_workflow_status,
    scope_key_from_view_dict,
    scope_watermark_row_key,
    view_query_list_sort,
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


def test_watermark_key_with_workflow_scope():
    k = scope_watermark_row_key("deadbeef", "site_01")
    assert k.startswith("scope_wm_")
    assert k != scope_watermark_row_key("deadbeef")


class _InstNested:
    def dump(self):
        return {"node": {"lastUpdatedTime": 1_700_000_000_000}}


def test_node_last_updated_time_ms_nested_dump():
    assert node_last_updated_time_ms(_InstNested()) == 1_700_000_000_000


def test_view_query_list_sort_omitted_for_all_modes() -> None:
    """CDF rejects ``sort`` on ``node.lastUpdatedTime``; incremental uses Range filter only."""
    assert view_query_list_sort(incremental=False) is None
    assert view_query_list_sort(incremental=True) is None


def test_list_all_instances_collects_stats(monkeypatch) -> None:
    class _Batch:
        def __iter__(self):
            yield object()

    class _Client:
        class _DM:
            class _Instances:
                def __call__(self, **kwargs):
                    assert kwargs.get("chunk_size") == 1000
                    assert kwargs.get("limit") is None
                    assert kwargs.get("sort") is None
                    return iter([_Batch()])

            instances = _Instances()

        data_modeling = _DM()

    stats = ListInstancesStats()
    items = list(
        list_all_instances(
            _Client(),
            instance_type="node",
            space=None,
            sources=[],
            filter=None,
            sort=view_query_list_sort(incremental=False),
            stats_out=stats,
        )
    )
    assert len(items) == 1
    assert stats.page_count == 1
    assert stats.instances_yielded == 1
    assert stats.list_duration_sec >= 0


def test_list_all_instances_multipage_chunk_iterator() -> None:
    class _Batch:
        def __init__(self, n: int) -> None:
            self._n = n

        def __iter__(self):
            for _ in range(self._n):
                yield object()

    class _Client:
        class _DM:
            class _Instances:
                def __call__(self, **kwargs):
                    assert kwargs.get("chunk_size") == 2
                    assert kwargs.get("limit") is None
                    return iter([_Batch(2), _Batch(1)])

            instances = _Instances()

        data_modeling = _DM()

    stats = ListInstancesStats()
    items = list(
        list_all_instances(
            _Client(),
            instance_type="node",
            space=None,
            sources=[],
            filter=None,
            limit_per_page=2,
            stats_out=stats,
        )
    )
    assert len(items) == 3
    assert stats.page_count == 2
    assert stats.instances_yielded == 3


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
