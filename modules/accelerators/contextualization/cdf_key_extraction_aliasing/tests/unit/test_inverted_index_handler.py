"""Unit tests for fn_dm_inverted_index handler (enable gate)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_inverted_index import (
    handler as ri_handler,
)


@pytest.mark.parametrize(
    "enable_val",
    [False, None],
)
def test_handle_skips_persist_when_disabled_or_omitted(enable_val):
    client = MagicMock()
    data: dict = {"logLevel": "INFO"}
    if enable_val is not None:
        data["enable_inverted_index"] = enable_val
    with patch.object(ri_handler, "persist_inverted_index") as m_persist:
        out = ri_handler.handle(data, client=client)
    m_persist.assert_not_called()
    assert out["status"] == "succeeded"
    summary = out["summary"]
    assert summary.get("inverted_index_skipped") is True
    assert summary.get("inverted_index_skip_reason") == "enable_inverted_index_false"


def test_handle_runs_persist_when_enabled():
    client = MagicMock()
    data = {
        "logLevel": "INFO",
        "enable_inverted_index": True,
        "source_raw_db": "db",
        "source_raw_table_key": "ke_state",
        "inverted_index_raw_table": "ref_idx",
        "config": {
            "config": {
                "parameters": {"debug": True},
                "data": {"aliasing_rules": [], "validation": {}},
            }
        },
    }
    with patch.object(ri_handler, "persist_inverted_index") as m_persist:
        out = ri_handler.handle(data, client=client)
    m_persist.assert_called_once()
    assert out["status"] == "succeeded"
    assert out["summary"].get("inverted_index_skipped") is None
