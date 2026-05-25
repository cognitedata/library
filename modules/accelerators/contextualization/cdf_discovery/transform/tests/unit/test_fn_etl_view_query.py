"""Unit tests for fn_etl_view_query incremental index cache and skip path."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from fn_etl_view_query.handler import (  # noqa: E402
    _can_skip_hash_by_watermark,
    _load_hash_index,
    _resolve_full_hash_index,
    etl_handle_view_query,
)


def test_resolve_full_hash_index_deployed_dict_cache_builds_once() -> None:
    build_calls = {"n": 0}

    def _fake_build(_client, _db, _tbl, *, workflow_scope="", chunk_size=2500):
        del _client, chunk_size
        build_calls["n"] += 1
        return {
            "scope_a": {"n1": "h1"},
            "scope_b": {"n2": "h2"},
        }

    data: dict = {}
    client = MagicMock()
    with patch(
        "fn_etl_view_query.handler.build_latest_hash_index_for_table",
        _fake_build,
    ):
        full_a = _resolve_full_hash_index(
            client, data, raw_db="db", raw_table="tbl", workflow_scope="wf"
        )
        full_b = _resolve_full_hash_index(
            client, data, raw_db="db", raw_table="tbl", workflow_scope="wf"
        )
    assert full_a == full_b
    assert build_calls["n"] == 1
    assert _load_hash_index(
        client, data, raw_db="db", raw_table="tbl", scope_key="scope_a", workflow_scope="wf"
    ) == {"n1": "h1"}
    assert _load_hash_index(
        client, data, raw_db="db", raw_table="tbl", scope_key="scope_b", workflow_scope="wf"
    ) == {"n2": "h2"}


def test_resolve_full_hash_index_callable_cache() -> None:
    def _getter(_client, _db, _tbl, workflow_scope=""):
        return {"sk1": {"n1": f"h_{workflow_scope}"}}

    data = {"etl_raw_hash_index_cache": _getter}
    client = MagicMock()
    assert _load_hash_index(
        client, data, raw_db="d", raw_table="t", scope_key="sk1", workflow_scope="wf"
    ) == {"n1": "h_wf"}


def test_can_skip_hash_by_watermark() -> None:
    assert _can_skip_hash_by_watermark(
        hash_skip=True,
        listing_narrowed=True,
        wm_before=1000,
        lu=500,
        nid="n1",
        latest_by_node={"n1": "abc"},
    )
    assert not _can_skip_hash_by_watermark(
        hash_skip=True,
        listing_narrowed=True,
        wm_before=1000,
        lu=1500,
        nid="n1",
        latest_by_node={"n1": "abc"},
    )


def test_hash_skip_does_not_queue_incremental_upsert(monkeypatch) -> None:
    inst = SimpleNamespace(
        external_id="ext1",
        space="sp1",
        instance_id="uuid-1",
        properties={"cdf_cdm": {"CogniteAsset/v1": {"name": "A"}}},
        last_updated_time=500,
    )

    def _fake_query(_client, **kwargs):
        del kwargs
        yield inst

    upsert_calls: list = []

    monkeypatch.setattr(
        "fn_etl_view_query.handler.query_all_view_instances",
        _fake_query,
    )
    monkeypatch.setattr(
        "fn_etl_view_query.handler.read_watermark_high_ms",
        lambda *_a, **_k: 1000,
    )
    monkeypatch.setattr(
        "fn_etl_view_query.handler._load_hash_index",
        lambda *_a, **_k: {"sp1:ext1": "same_hash"},
    )
    monkeypatch.setattr(
        "fn_etl_view_query.handler.row_content_hash",
        lambda _props: "same_hash",
    )
    monkeypatch.setattr(
        "fn_etl_view_query.handler.upsert_incremental_entity_hashes_raw",
        lambda *_a, **kwargs: upsert_calls.append(kwargs),
    )
    monkeypatch.setattr(
        "fn_etl_view_query.handler.write_incremental_watermark_raw",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        "fn_etl_view_query.handler.maybe_handoff_predecessor_rows",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        "fn_etl_view_query.handler.resolve_incremental_state_sink",
        lambda *_a, **_k: ("db", "tbl__incremental"),
    )

    client = MagicMock()
    data = {
        "incremental_change_processing": True,
        "parameters": {"incremental_change_processing": True, "workflow_scope": "wf"},
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
        },
    }
    summary = etl_handle_view_query("fn_etl_view_query", data, client, None)
    assert summary["instances_listed"] == 1
    assert summary["instances_skipped_hash"] == 1
    assert summary["instances_written"] == 0
    assert summary["state_load_duration_sec"] >= 0
    assert summary["query_duration_sec"] >= 0
    assert summary["loop_duration_sec"] >= 0
    assert upsert_calls == []
