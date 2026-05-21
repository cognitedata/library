"""Tests for per-run per-canvas-node cohort tables and instance row keys."""

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
    fan_in_cohort_props_by_instance,
    instance_cohort_row_key,
    node_cohort_table_name,
    predecessor_canvas_node_ids,
    run_node_table_prefix,
)


class _Row:
    def __init__(self, key: str, columns: Dict[str, Any]) -> None:
        self.key = key
        self.columns = columns


def _compiled_workflow_validate_two_preds() -> dict:
    return {
        "tasks": [
            {"id": "kea__vq", "canvas_node_id": "vq_eq", "depends_on": []},
            {"id": "kea__tr", "canvas_node_id": "tr", "depends_on": ["kea__vq"]},
            {
                "id": "kea__alias",
                "canvas_node_id": "n_alias",
                "depends_on": ["kea__vq"],
            },
            {
                "id": "kea__validate",
                "canvas_node_id": "n_validate",
                "depends_on": ["kea__tr", "kea__alias"],
            },
        ]
    }


def test_predecessor_canvas_node_ids_ordered_unique() -> None:
    cw = _compiled_workflow_validate_two_preds()
    data = {"compiled_workflow": cw}
    preds = predecessor_canvas_node_ids(data, "kea__validate")
    assert preds == ["tr", "n_alias"]


def test_fan_in_merges_disjoint_aliases_and_index_key(monkeypatch) -> None:
    scope = "scope1"
    inst = "sp1:uuid-1"
    run_id = "20260101T120000.000000Z-abc123"
    tables = {
        node_cohort_table_name("discovery_state", run_id, "tr"): [
            _Row(
                f"{scope}:{inst}",
                {
                    "RECORD_KIND": "entity",
                    "SCOPE_KEY": scope,
                    "NODE_INSTANCE_ID": inst,
                    "PROPERTIES_JSON": json.dumps({"aliases": ["A"]}),
                },
            )
        ],
        node_cohort_table_name("discovery_state", run_id, "n_alias"): [
            _Row(
                f"{scope}:{inst}",
                {
                    "RECORD_KIND": "entity",
                    "SCOPE_KEY": scope,
                    "NODE_INSTANCE_ID": inst,
                    "PROPERTIES_JSON": json.dumps({"indexKey": ["K1"]}),
                },
            )
        ],
    }

    def _fake_iter(_client, _db, raw_table, chunk_size=2500):
        del _client, _db, chunk_size
        for row in tables.get(raw_table, []):
            yield row

    monkeypatch.setattr(
        "cdf_fn_common.cohort_storage.iter_cohort_entity_rows",
        _fake_iter,
    )
    merged = list(
        fan_in_cohort_props_by_instance(
            MagicMock(), "db", "discovery_state", run_id, ["tr", "n_alias"]
        )
    )
    assert len(merged) == 1
    _cols, props = merged[0]
    assert set(props.get("aliases", [])) == {"A"}
    assert props.get("indexKey") == ["K1"]


def test_node_table_and_row_key() -> None:
    rid = "20260517T120000.000000Z-aaaaaaaaaaaa"
    tbl = node_cohort_table_name("discovery_state", rid, "tr")
    assert tbl.startswith("discovery_state__")
    prefix = run_node_table_prefix("discovery_state", rid)
    assert tbl.startswith(prefix)
    assert instance_cohort_row_key("sp1:x", "scope1") == "scope1:sp1:x"
