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
    etl_handle_view_query,
)
from cdf_fn_common.etl_incremental_hash import row_content_hash  # noqa: E402
from cdf_fn_common.etl_rule_inclusion_hash import compute_rule_inclusion_hash  # noqa: E402


def test_can_skip_hash_by_watermark() -> None:
    assert _can_skip_hash_by_watermark(
        hash_skip=True,
        listing_narrowed=True,
        wm_before=1000,
        lu=500,
        previous_hash="abc",
    )
    assert not _can_skip_hash_by_watermark(
        hash_skip=True,
        listing_narrowed=True,
        wm_before=1000,
        lu=1500,
        previous_hash="abc",
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
        "fn_etl_view_query.handler.incremental_state_table_exists",
        lambda *_a, **_k: True,
    )
    monkeypatch.setattr(
        "fn_etl_view_query.handler.load_incremental_hashes_for_nodes",
        lambda *_a, **_k: {"sp1:ext1": "same_hash", "sp1:uuid-1": "same_hash"},
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
    monkeypatch.setattr(
        "fn_etl_view_query.handler.load_query_checkpoint_state",
        lambda *_a, **_k: SimpleNamespace(rows_completed=0, is_complete=False, continuation_token=""),
    )
    monkeypatch.setattr(
        "fn_etl_view_query.handler.save_query_checkpoint_state",
        lambda *_a, **_k: None,
    )

    client = MagicMock()
    data = {
        "run_id": "00000000-0000-4000-8000-000000000004",
        "configuration": {
            "parameters": {"incremental_change_processing": True, "workflow_scope": "wf"}
        },
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


def test_view_query_uses_checkpoint_cursor_and_run_cap(monkeypatch) -> None:
    inst = SimpleNamespace(
        external_id="ext1",
        space="sp1",
        instance_id="uuid-1",
        properties={"cdf_cdm": {"CogniteAsset/v1": {"name": "A"}}},
        last_updated_time=500,
    )
    seen: dict[str, object] = {}

    def _fake_query(_client, **kwargs):
        seen["initial_cursor"] = kwargs.get("initial_cursor")
        seen["max_items"] = kwargs.get("max_items")
        stats_out = kwargs.get("stats_out")
        if stats_out is not None:
            stats_out.next_cursor = "cursor-next"
            stats_out.page_count = 1
            stats_out.instances_yielded = 1
        yield inst

    saved: dict[str, object] = {}
    monkeypatch.setattr("fn_etl_view_query.handler.query_all_view_instances", _fake_query)
    monkeypatch.setattr("fn_etl_view_query.handler.maybe_handoff_predecessor_rows", lambda *_a, **_k: None)
    monkeypatch.setattr("fn_etl_view_query.handler.load_query_checkpoint_state", lambda *_a, **_k: SimpleNamespace(rows_completed=0, is_complete=False, continuation_token="cursor-1"))
    monkeypatch.setattr("fn_etl_view_query.handler.save_query_checkpoint_state", lambda *_a, **kwargs: saved.update(kwargs))

    client = MagicMock()
    data = {
        "run_id": "00000000-0000-4000-8000-000000000004",
        "configuration": {"parameters": {"max_records_per_run": 1}},
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
        },
    }
    summary = etl_handle_view_query("fn_etl_view_query", data, client, None)
    assert summary["instances_written"] == 1
    assert seen["initial_cursor"] == "cursor-1"
    assert seen["max_items"] == 1
    assert saved["continuation_token"] == "cursor-next"


def test_view_query_lookup_full_scan_skips_checkpoint(monkeypatch) -> None:
    inst = SimpleNamespace(
        external_id="ext1",
        space="sp1",
        instance_id="uuid-1",
        properties={"cdf_cdm": {"CogniteAsset/v1": {"name": "A"}}},
        last_updated_time=500,
    )
    seen: dict[str, object] = {}

    def _fake_query(_client, **kwargs):
        seen["initial_cursor"] = kwargs.get("initial_cursor")
        seen["max_items"] = kwargs.get("max_items")
        yield inst

    monkeypatch.setattr("fn_etl_view_query.handler.query_all_view_instances", _fake_query)
    monkeypatch.setattr("fn_etl_view_query.handler.maybe_handoff_predecessor_rows", lambda *_a, **_k: None)

    load_calls = {"count": 0}

    def _load(*_a, **_k):
        load_calls["count"] += 1
        return SimpleNamespace(rows_completed=0, is_complete=False, continuation_token="cursor-1")

    monkeypatch.setattr("fn_etl_view_query.handler.load_query_checkpoint_state", _load)
    monkeypatch.setattr("fn_etl_view_query.handler.save_query_checkpoint_state", lambda *_a, **_k: None)

    client = MagicMock()
    data = {
        "run_id": "00000000-0000-4000-8000-000000000004",
        "configuration": {"parameters": {"max_records_per_run": 1}},
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
            "lookup_full_scan": True,
        },
    }
    summary = etl_handle_view_query("fn_etl_view_query", data, client, None)
    assert summary["instances_written"] == 1
    assert summary["lookup_full_scan"] is True
    assert load_calls["count"] == 0
    assert seen["initial_cursor"] is None
    assert seen["max_items"] == 0


def test_view_query_non_incremental_without_cap_skips_checkpoint(monkeypatch) -> None:
    inst = SimpleNamespace(
        external_id="ext1",
        space="sp1",
        instance_id="uuid-1",
        properties={"cdf_cdm": {"CogniteAsset/v1": {"name": "A"}}},
        last_updated_time=500,
    )
    seen: dict[str, object] = {}
    load_calls = {"count": 0}

    def _fake_query(_client, **kwargs):
        seen["initial_cursor"] = kwargs.get("initial_cursor")
        seen["max_items"] = kwargs.get("max_items")
        yield inst

    def _load(*_a, **_k):
        load_calls["count"] += 1
        return SimpleNamespace(rows_completed=10, is_complete=True, continuation_token="cursor-1")

    monkeypatch.setattr("fn_etl_view_query.handler.query_all_view_instances", _fake_query)
    monkeypatch.setattr("fn_etl_view_query.handler.maybe_handoff_predecessor_rows", lambda *_a, **_k: None)
    monkeypatch.setattr("fn_etl_view_query.handler.load_query_checkpoint_state", _load)
    monkeypatch.setattr("fn_etl_view_query.handler.save_query_checkpoint_state", lambda *_a, **_k: None)

    client = MagicMock()
    data = {
        "run_id": "00000000-0000-4000-8000-000000000004",
        "configuration": {"parameters": {"incremental_change_processing": False}},
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
        },
    }
    summary = etl_handle_view_query("fn_etl_view_query", data, client, None)
    assert summary["instances_written"] == 1
    assert summary["incremental_change_processing"] is False
    assert load_calls["count"] == 0
    assert seen["initial_cursor"] is None
    assert seen["max_items"] == 0


def test_rule_change_reprocesses_even_when_source_props_unchanged(monkeypatch) -> None:
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

    old_compiled = {
        "tasks": [
            {"id": "q1", "executable_kind": "query_view", "depends_on": [], "payload": {"config": {}}},
            {
                "id": "sc1",
                "executable_kind": "score",
                "depends_on": ["q1"],
                "payload": {"config": {"scoring_rules": [{"name": "rule", "priority": 1}]}},
            },
        ]
    }
    new_compiled = {
        "tasks": [
            {"id": "q1", "executable_kind": "query_view", "depends_on": [], "payload": {"config": {}}},
            {
                "id": "sc1",
                "executable_kind": "score",
                "depends_on": ["q1"],
                "payload": {"config": {"scoring_rules": [{"name": "rule", "priority": 2}]}},
            },
        ]
    }
    old_rule_hash, _old_ids = compute_rule_inclusion_hash(old_compiled, task_id="q1")
    prev_hash = row_content_hash(
        {
            "name": "A",
            "instance_space": "sp1",
            "_rule_inclusion_hash": old_rule_hash,
        }
    )

    monkeypatch.setattr("fn_etl_view_query.handler.query_all_view_instances", _fake_query)
    monkeypatch.setattr("fn_etl_view_query.handler.incremental_state_table_exists", lambda *_a, **_k: True)
    monkeypatch.setattr("fn_etl_view_query.handler.read_watermark_high_ms", lambda *_a, **_k: None)
    monkeypatch.setattr(
        "fn_etl_view_query.handler.load_incremental_hashes_for_nodes",
        lambda *_a, **_k: {"sp1:uuid-1": prev_hash},
    )
    monkeypatch.setattr("fn_etl_view_query.handler.upsert_incremental_entity_hashes_raw", lambda *_a, **_k: None)
    monkeypatch.setattr("fn_etl_view_query.handler.write_incremental_watermark_raw", lambda *_a, **_k: None)
    monkeypatch.setattr("fn_etl_view_query.handler.maybe_handoff_predecessor_rows", lambda *_a, **_k: None)
    monkeypatch.setattr("fn_etl_view_query.handler.resolve_incremental_state_sink", lambda *_a, **_k: ("db", "tbl"))
    monkeypatch.setattr(
        "fn_etl_view_query.handler.load_query_checkpoint_state",
        lambda *_a, **_k: SimpleNamespace(rows_completed=0, is_complete=False, continuation_token=""),
    )
    monkeypatch.setattr("fn_etl_view_query.handler.save_query_checkpoint_state", lambda *_a, **_k: None)

    client = MagicMock()
    data = {
        "task_id": "q1",
        "compiled_workflow": new_compiled,
        "run_id": "00000000-0000-4000-8000-000000000004",
        "configuration": {
            "parameters": {"incremental_change_processing": True, "workflow_scope": "wf"}
        },
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
        },
    }
    summary = etl_handle_view_query("fn_etl_view_query", data, client, None)
    assert summary["instances_listed"] == 1
    assert summary["instances_skipped_hash"] == 0
    assert summary["instances_written"] == 1


def test_missing_incremental_table_disables_per_row_state_reads(monkeypatch) -> None:
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

    load_calls = {"count": 0}

    def _load_hashes(*_a, **_k):
        load_calls["count"] += 1
        return {"sp1:uuid-1": "same_hash"}

    monkeypatch.setattr("fn_etl_view_query.handler.query_all_view_instances", _fake_query)
    monkeypatch.setattr("fn_etl_view_query.handler.incremental_state_table_exists", lambda *_a, **_k: False)
    monkeypatch.setattr("fn_etl_view_query.handler.read_watermark_high_ms", lambda *_a, **_k: 1000)
    monkeypatch.setattr("fn_etl_view_query.handler.load_incremental_hashes_for_nodes", _load_hashes)
    monkeypatch.setattr("fn_etl_view_query.handler.row_content_hash", lambda _props: "new_hash")
    monkeypatch.setattr("fn_etl_view_query.handler.upsert_incremental_entity_hashes_raw", lambda *_a, **_k: None)
    monkeypatch.setattr("fn_etl_view_query.handler.write_incremental_watermark_raw", lambda *_a, **_k: None)
    monkeypatch.setattr("fn_etl_view_query.handler.maybe_handoff_predecessor_rows", lambda *_a, **_k: None)
    monkeypatch.setattr("fn_etl_view_query.handler.resolve_incremental_state_sink", lambda *_a, **_k: ("db", "tbl"))
    monkeypatch.setattr(
        "fn_etl_view_query.handler.load_query_checkpoint_state",
        lambda *_a, **_k: SimpleNamespace(rows_completed=0, is_complete=False, continuation_token=""),
    )
    monkeypatch.setattr("fn_etl_view_query.handler.save_query_checkpoint_state", lambda *_a, **_k: None)

    client = MagicMock()
    data = {
        "run_id": "00000000-0000-4000-8000-000000000005",
        "configuration": {
            "parameters": {"incremental_change_processing": True, "workflow_scope": "wf"}
        },
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
        },
    }
    summary = etl_handle_view_query("fn_etl_view_query", data, client, None)
    assert summary["incremental_state_reads_enabled"] is False
    assert summary["instances_written"] == 1
    assert summary["instances_skipped_hash"] == 0
    assert load_calls["count"] == 0
