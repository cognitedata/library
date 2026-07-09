"""Tests for index_entry_bridge storage location helpers."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE = Path(__file__).resolve().parents[3]
_FUNCTIONS = _MODULE / "functions"
if str(_FUNCTIONS) not in sys.path:
    sys.path.insert(0, str(_FUNCTIONS))

from cdf_fn_common.index_entry_bridge import (  # noqa: E402
    collect_contextualization_index_raw_tables,
    resolve_index_raw_sample_location,
)


def test_resolve_index_raw_sample_location_defaults() -> None:
    data = {
        "configuration": {
            "scope": {
                "path": [{"level": "site", "id": "SITE_01"}],
            },
        },
        "config": {
            "index_storage_backend": "raw",
            "index_raw_database": "db_contextualization_idx",
            "scope": {
                "enabled": True,
                "levels": ["site"],
                "scope_key_template": "site:{site}",
                "fallback_scope_key": "global",
            },
        },
    }
    loc = resolve_index_raw_sample_location(data)
    assert loc["storage_backend"] == "raw"
    assert loc["match_scope_key"] == "site:SITE_01"
    assert loc["raw_db"] == "db_contextualization_idx"
    assert loc["raw_table"].startswith("inverted_index__")


def test_collect_contextualization_index_raw_tables() -> None:
    data = {
        "configuration": {},
        "config": {
            "index_storage_backend": "raw",
            "index_raw_database": "db_contextualization_idx",
            "scope": {"enabled": True, "fallback_scope_key": "global"},
        },
    }
    task_cfg = data["config"]
    tables = collect_contextualization_index_raw_tables(data, task_cfg=task_cfg)
    assert tables == [("db_contextualization_idx", "inverted_index__global")]
