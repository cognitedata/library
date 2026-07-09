"""Unit tests for downstream query predecessor helpers."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.etl_query_predecessor import (  # noqa: E402
    predecessor_external_ids,
    raw_query_rows_from_predecessor_buffer,
    resolve_raw_query_source,
    should_restrict_view_query_to_predecessors,
)


def _compiled_with_upstream() -> dict:
    return {
        "compiled_workflow": {
            "tasks": [
                {"id": "upstream", "canvas_node_id": "node_up"},
                {"id": "raw_q", "depends_on": ["upstream"], "canvas_node_id": "node_raw"},
            ]
        },
        "run_id": "run-abc",
        "etl_task_row_buffers": {
            "upstream": [
                {
                    "columns": {"external_id": "E1", "node_instance_id": "sp:E1"},
                    "properties": {"name": "alpha"},
                },
                {
                    "columns": {"external_id": "E2", "node_instance_id": "sp:E2"},
                    "properties": {"name": "beta"},
                },
            ],
        },
    }


def test_predecessor_external_ids_dedupes() -> None:
    data = _compiled_with_upstream()
    ids = predecessor_external_ids(data, "raw_q")
    assert ids == ["E1", "E2"]


def test_should_restrict_view_query_defaults_true() -> None:
    assert should_restrict_view_query_to_predecessors({}) is True
    assert should_restrict_view_query_to_predecessors({"restrict_to_predecessors": False}) is False


def test_resolve_raw_query_source_from_predecessor_table() -> None:
    data = _compiled_with_upstream()
    data["configuration"] = {"raw_db": "db_discovery", "raw_table": "discovery_state"}
    loc = resolve_raw_query_source(data, "raw_q", {})
    assert loc is not None
    assert loc[0] == "db_discovery"
    assert "run-abc" in loc[1] or loc[1]  # table includes run segment
    assert loc[2] == "run-abc"


def test_resolve_raw_query_source_explicit_config_wins() -> None:
    data = _compiled_with_upstream()
    loc = resolve_raw_query_source(
        data,
        "raw_q",
        {"source_raw_db": "explicit_db", "source_raw_table": "explicit_tbl"},
    )
    assert loc is None


def test_raw_query_rows_from_predecessor_buffer() -> None:
    data = _compiled_with_upstream()
    rows, n_read = raw_query_rows_from_predecessor_buffer(
        data,
        "raw_q",
        filters=[{"operator": "EQUALS", "target_property": "name", "values": ["alpha"]}],
        read_limit=0,
    )
    assert n_read == 1
    assert len(rows) == 1
    assert rows[0]["columns"]["external_id"] == "E1"
