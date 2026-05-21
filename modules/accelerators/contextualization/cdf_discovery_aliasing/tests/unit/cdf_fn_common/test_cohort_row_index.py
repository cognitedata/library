"""Tests for in-memory cohort row index (no per-key RAW retrieve)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.cohort_storage import (  # noqa: E402
    build_cohort_row_index,
    get_or_build_cohort_row_index,
    instance_cohort_row_key,
    node_cohort_table_name,
)
from cdf_fn_common.discovery_query_shared import (  # noqa: E402
    PROPERTIES_JSON_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RUN_ID_COLUMN,
)
from cdf_fn_common.transform_cumulative_input import (  # noqa: E402
    props_for_key_at_location,
    resolve_cumulative_input_props,
)


class _FakeRow:
    def __init__(self, key: str, columns: Dict[str, Any]) -> None:
        self.key = key
        self.columns = columns


def _chunked_rows(rows: list[_FakeRow]):
    for r in rows:
        yield r


def test_build_cohort_row_index_matches_retrieve_props() -> None:
    key = instance_cohort_row_key("inst-1", "scope1")
    cols = {
        RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
        RUN_ID_COLUMN: "run-1",
        "SCOPE_KEY": "scope1",
        "NODE_INSTANCE_ID": "inst-1",
        PROPERTIES_JSON_COLUMN: json.dumps({"name": "P-101", "aliases": ["P-101"]}),
    }
    row = _FakeRow(key, cols)
    client = MagicMock()
    client.raw.rows = MagicMock(return_value=_chunked_rows([row]))
    client.raw.rows.retrieve = MagicMock(return_value=row)

    index = build_cohort_row_index(client, "db", "tbl")
    via_index = index[key].properties
    client.raw.rows.retrieve.assert_not_called()
    via_retrieve = props_for_key_at_location(client, "db", "tbl", key)
    assert via_index == via_retrieve


def test_resolve_cumulative_input_uses_table_indexes_without_retrieve() -> None:
    run_id = "20260101T120000.000000Z-abc123"
    writer = "n_writer"
    pred = "tr"
    base = "discovery_state"
    key = instance_cohort_row_key("inst-1", "scope1")
    pred_tbl = node_cohort_table_name(base, run_id, pred)
    writer_tbl = node_cohort_table_name(base, run_id, writer)
    cols = {
        RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
        RUN_ID_COLUMN: run_id,
        "SCOPE_KEY": "scope1",
        "NODE_INSTANCE_ID": "inst-1",
        PROPERTIES_JSON_COLUMN: json.dumps({"name": "P-101", "aliases": ["P-101"]}),
    }
    from cdf_fn_common.cohort_storage import CohortRowSnapshot

    snap = CohortRowSnapshot(columns=dict(cols), properties={"name": "P-101", "aliases": ["P-101"]})
    table_indexes = {
        ("db_discovery", pred_tbl): {key: snap},
        ("db_discovery", writer_tbl): {key: snap},
    }
    client = MagicMock()
    client.raw.rows.retrieve = MagicMock(side_effect=AssertionError("retrieve should not run"))

    anchor = dict(cols)
    cfg = {"steps": [{"handler_id": "trim_whitespace", "output_field": "aliases"}]}
    merged = resolve_cumulative_input_props(
        client,
        anchor,
        writer_canvas_node_id=writer,
        predecessor_canvas_node_ids=[pred],
        raw_db="db_discovery",
        base_table=base,
        run_id=run_id,
        cfg=cfg,
        table_indexes=table_indexes,
    )
    assert "P-101" in merged["aliases"]
    client.raw.rows.retrieve.assert_not_called()


def test_get_or_build_cohort_row_index_memoizing_cache_builds_once() -> None:
    build_calls = {"n": 0}
    key = instance_cohort_row_key("n1", "sk")
    cols = {
        RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
        "SCOPE_KEY": "sk",
        "NODE_INSTANCE_ID": "n1",
        PROPERTIES_JSON_COLUMN: "{}",
    }
    store: Dict[tuple[str, str], Any] = {}

    def memo_cache(_client, db: str, tbl: str) -> Dict[str, Any]:
        k = (db, tbl)
        if k not in store:
            build_calls["n"] += 1
            from cdf_fn_common.cohort_storage import CohortRowSnapshot

            store[k] = {key: CohortRowSnapshot(columns=cols, properties={})}
        return store[k]

    client = MagicMock()
    idx1 = get_or_build_cohort_row_index(client, "db", "t", index_cache=memo_cache)
    idx2 = get_or_build_cohort_row_index(client, "db", "t", index_cache=memo_cache)
    assert idx1 is idx2
    assert build_calls["n"] == 1
