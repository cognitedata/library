"""Tests for fn_runtime storage backend resolution."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[2]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))
if str(_MODULE_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODULE_ROOT))

from cdf_fn_common.fn_runtime import resolve_handler_payload  # noqa: E402


def test_resolve_handler_payload_raw_backend() -> None:
    resolved = resolve_handler_payload(
        {"config": {"index_storage_backend": "raw", "index_raw_database": "db_contextualization_idx"}}
    )
    assert resolved["overrides"]["storage_config"]["backend"] == "raw"
    assert resolved["overrides"]["storage_config"]["raw"]["database"] == "db_contextualization_idx"


def test_resolve_handler_payload_dm_backend() -> None:
    resolved = resolve_handler_payload(
        {
            "config": {
                "index_storage_backend": "dm",
                "index_schema_space": "contextualization_idx",
            }
        }
    )
    assert resolved["overrides"]["storage_config"]["backend"] == "dm"
    assert resolved["overrides"]["storage_config"]["dm"]["space"] == "contextualization_idx"
