"""Tests for local Kahn workflow executor helpers."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from local_runner.kahn_workflow_executor import _etl_raw_hash_index_getter  # noqa: E402


def test_etl_raw_hash_index_getter_builds_once_per_sink() -> None:
    build_calls = {"n": 0}

    def _fake_build(_client, _db, _tbl, *, workflow_scope="", chunk_size=2500):
        del _client, chunk_size
        build_calls["n"] += 1
        return {"sk": {"n1": f"h1_{workflow_scope}"}}

    shared: dict = {}
    with patch(
        "cdf_fn_common.etl_incremental_scope.build_latest_hash_index_for_table",
        _fake_build,
    ):
        getter = _etl_raw_hash_index_getter(shared)
        assert getter(None, "db1", "t1", "wf_a") == {"sk": {"n1": "h1_wf_a"}}
        assert getter(None, "db1", "t1", "wf_a") == {"sk": {"n1": "h1_wf_a"}}
        assert build_calls["n"] == 1
        getter(None, "db1", "t1", "wf_b")
        assert build_calls["n"] == 2
