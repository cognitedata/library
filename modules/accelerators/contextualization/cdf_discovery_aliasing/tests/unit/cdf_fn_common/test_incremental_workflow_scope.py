"""Tests for workflow_scope resolution and multi-workflow incremental partitioning."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

_FUNCS = Path(__file__).resolve().parents[3] / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.incremental_scope import (
    build_latest_hash_index_for_table,
    incremental_state_table_name,
    scope_watermark_row_key,
)
from cdf_fn_common.incremental_workflow_scope import (
    DEFAULT_LOCAL_WORKFLOW_SCOPE,
    resolve_source_view_fingerprint,
    resolve_workflow_scope,
)


def test_resolve_workflow_scope_from_parameters():
    cfg = {}
    params = {"workflow_scope": "site_01"}
    assert resolve_workflow_scope(cfg, params) == "site_01"


def test_resolve_workflow_scope_from_scope_block():
    cfg = {"scope": {"id": "SITE_02", "name": "Site 2"}}
    assert resolve_workflow_scope(cfg, {}) == "SITE_02"


def test_resolve_workflow_scope_local_default():
    assert resolve_workflow_scope({}, {}) == DEFAULT_LOCAL_WORKFLOW_SCOPE


def test_resolve_source_view_fingerprint_defaults_to_scope_key():
    assert resolve_source_view_fingerprint({}, scope_key="abc123") == "abc123"
    assert resolve_source_view_fingerprint({"source_view_fingerprint": "fp"}, scope_key="abc") == "fp"


def test_watermark_key_includes_workflow_scope():
    k = scope_watermark_row_key("deadbeef", "site_01")
    assert k.startswith("scope_wm_")
    assert "site_01" in k or "site" in k


def test_incremental_state_table_name():
    assert incremental_state_table_name("discovery_state") == "discovery_state__incremental"


def test_hash_index_isolated_by_workflow_scope():
    row_a = MagicMock()
    row_a.columns = {
        "RECORD_KIND": "entity",
        "WORKFLOW_SCOPE": "wf_a",
        "SCOPE_KEY": "sk1",
        "NODE_INSTANCE_ID": "n1",
        "EXTRACTION_INPUTS_HASH": "hash_a",
        "WORKFLOW_STATUS": "detected",
        "RUN_ID": "r1",
    }
    row_b = MagicMock()
    row_b.columns = {
        "RECORD_KIND": "entity",
        "WORKFLOW_SCOPE": "wf_b",
        "SCOPE_KEY": "sk1",
        "NODE_INSTANCE_ID": "n1",
        "EXTRACTION_INPUTS_HASH": "hash_b",
        "WORKFLOW_STATUS": "detected",
        "RUN_ID": "r2",
    }
    client = MagicMock()
    client.raw.rows = lambda *a, **k: iter([[row_a, row_b]])

    idx_a = build_latest_hash_index_for_table(
        client, "db", "discovery_state__incremental", workflow_scope="wf_a"
    )
    idx_b = build_latest_hash_index_for_table(
        client, "db", "discovery_state__incremental", workflow_scope="wf_b"
    )
    assert idx_a["sk1"]["n1"] == "hash_a"
    assert idx_b["sk1"]["n1"] == "hash_b"
