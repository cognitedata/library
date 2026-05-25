"""Unit tests for ETL merge orchestration."""

from __future__ import annotations

import pytest

from cdf_fn_common.etl_merge_orchestration import (
    etl_handle_merge_in_memory,
    validate_merge_config,
)


def test_validate_merge_config_requires_description_and_policies() -> None:
    with pytest.raises(ValueError, match="description"):
        validate_merge_config({})
    with pytest.raises(ValueError, match="field_policies"):
        validate_merge_config({"description": "merge branches"})


def test_merge_in_memory_combines_aliases() -> None:
    data = {
        "task_id": "m1",
        "config": {
            "description": "merge test",
            "field_policies": [
                {
                    "property": "aliases",
                    "strategy": "merge_list",
                    "merge_list": {"unique": True, "branch_order": "by_score"},
                }
            ],
        },
        "_predecessor_rows": [
            {
                "columns": {"NODE_INSTANCE_ID": "sp:1", "EXTERNAL_ID": "ext-1"},
                "properties": {"aliases": ["A"]},
            },
            {
                "columns": {"NODE_INSTANCE_ID": "sp:1", "EXTERNAL_ID": "ext-1"},
                "properties": {"aliases": ["B"]},
            },
        ],
    }
    summary = etl_handle_merge_in_memory(
        "fn_etl_merge",
        data,
        dict(data["config"]),
        task_id="m1",
        run_id="run-1",
        log=None,
    )
    assert summary["rows_written"] == 1
    out = data["_predecessor_rows"]
    assert len(out) == 1
    aliases = out[0]["properties"]["aliases"]
    assert "A" in aliases and "B" in aliases
