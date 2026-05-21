"""Tests for cumulative transform input merge and orchestration."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.cohort_storage import (  # noqa: E402
    instance_cohort_row_key,
    node_cohort_table_name,
)
from cdf_fn_common.discovery_query_shared import (  # noqa: E402
    PROPERTIES_JSON_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RUN_ID_COLUMN,
    build_entity_cohort_row,
)
from cdf_fn_common.property_merge import merge_property_dicts  # noqa: E402
from cdf_fn_common.transform_cumulative_input import (  # noqa: E402
    INPUT_MODE_REPLACE,
    cumulative_field_policies,
    parse_input_mode,
    resolve_cumulative_input_props,
)
from fn_dm_transform.engine.pipeline import transform_row_properties  # noqa: E402
from fn_dm_transform.engine.transform_steps import apply_transform_steps_to_props  # noqa: E402


def test_parse_input_mode_defaults_cumulative() -> None:
    assert parse_input_mode({}) == "cumulative"


def test_cumulative_field_policies_merge_list_on_output_fields() -> None:
    cfg = {
        "steps": [
            {"handler_id": "trim_whitespace", "output_field": "aliases"},
            {"handler_id": "trim_whitespace", "output_field": "indexKey"},
        ]
    }
    policies = cumulative_field_policies(cfg)
    assert policies["aliases"].strategy == "merge_list"
    assert policies["indexKey"].strategy == "merge_list"


def test_merge_property_dicts_combines_aliases() -> None:
    policies = cumulative_field_policies(
        {"steps": [{"handler_id": "trim_whitespace", "output_field": "aliases"}]}
    )
    merged = merge_property_dicts(
        [
            {"name": "P-101", "aliases": "P-101"},
            {"aliases": ["P-101A"]},
        ],
        policies,
    )
    assert set(merged["aliases"]) == {"P-101", "P-101A"}


def test_transform_default_output_mode_append() -> None:
    cfg = {
        "handler_id": "trim_whitespace",
        "fields": [{"field_name": "name"}],
        "output_field": "aliases",
        "output_template": "{name}",
        "trim_whitespace": {"mode": "ends_only"},
    }
    rows = transform_row_properties({"name": "TAG-1", "aliases": ["existing"]}, cfg)
    assert rows[0]["aliases"] == ["existing", "TAG-1"]


def test_transform_append_skips_duplicate_scalar() -> None:
    cfg = {
        "handler_id": "trim_whitespace",
        "fields": [{"field_name": "name"}],
        "output_field": "aliases",
        "output_template": "{name}",
        "trim_whitespace": {"mode": "ends_only"},
    }
    rows = transform_row_properties(
        {"name": "file.pdf", "aliases": ["file.pdf"]},
        cfg,
    )
    assert rows[0]["aliases"] == ["file.pdf"]


def test_transform_output_mode_overwrite_replaces_field() -> None:
    cfg = {
        "handler_id": "trim_whitespace",
        "fields": [{"field_name": "name"}],
        "output_field": "aliases",
        "output_template": "{name}",
        "output_mode": "overwrite",
        "trim_whitespace": {"mode": "ends_only"},
    }
    rows = transform_row_properties({"name": "TAG-1", "aliases": ["existing"]}, cfg)
    assert rows[0]["aliases"] == "TAG-1"


class _FakeRawRow:
    def __init__(self, key: str, columns: Dict[str, Any]) -> None:
        self.key = key
        self.columns = columns


class _FakeRawRows:
    def __init__(self, rows_by_table: Dict[str, Dict[str, _FakeRawRow]]) -> None:
        self._rows_by_table = rows_by_table

    def retrieve(self, db: str, table: str, key: str) -> _FakeRawRow:
        tbl = self._rows_by_table.get(table, {})
        if key not in tbl:
            raise KeyError(key)
        return tbl[key]


def _entity_row(
    *,
    key: str,
    run_id: str,
    props: Dict[str, Any],
    task_id: str = "kea__vq",
) -> _FakeRawRow:
    cols = {
        RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
        RUN_ID_COLUMN: run_id,
        PROPERTIES_JSON_COLUMN: json.dumps(props, sort_keys=True),
        "QUERY_TASK_ID": task_id,
    }
    return _FakeRawRow(key, cols)


def test_resolve_cumulative_input_merges_sink_with_predecessor() -> None:
    run_id = "20260101T120000.000000Z-abc123"
    writer = "n_writer"
    pred = "tr"
    base = "discovery_state"
    key = instance_cohort_row_key("inst-1", "scope1")
    pred_tbl = node_cohort_table_name(base, run_id, pred)
    writer_tbl = node_cohort_table_name(base, run_id, writer)
    row = _entity_row(
        key=key,
        run_id=run_id,
        props={"name": "P-101", "aliases": ["P-101"]},
    )
    client = MagicMock()
    client.raw.rows = _FakeRawRows({pred_tbl: {key: row}, writer_tbl: {key: row}})
    anchor = {
        RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
        RUN_ID_COLUMN: run_id,
        "SCOPE_KEY": "scope1",
        "NODE_INSTANCE_ID": "inst-1",
        PROPERTIES_JSON_COLUMN: json.dumps({"name": "P-101"}),
    }
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
    )
    assert "P-101" in merged["aliases"]


def test_resolve_cumulative_input_replace_skips_sink() -> None:
    run_id = "20260101T120000.000000Z-abc123"
    writer = "n_writer"
    base = "discovery_state"
    key = instance_cohort_row_key("inst-1", "scope1")
    writer_tbl = node_cohort_table_name(base, run_id, writer)
    client = MagicMock()
    client.raw.rows = _FakeRawRows(
        {
            writer_tbl: {
                key: _entity_row(
                    key=key,
                    run_id=run_id,
                    props={"name": "P-101", "aliases": ["from_sink"]},
                )
            }
        }
    )
    anchor = {
        RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
        RUN_ID_COLUMN: run_id,
        "SCOPE_KEY": "scope1",
        "NODE_INSTANCE_ID": "inst-1",
        PROPERTIES_JSON_COLUMN: json.dumps({"name": "P-101"}),
    }
    cfg = {
        "input_mode": INPUT_MODE_REPLACE,
        "steps": [{"handler_id": "trim_whitespace", "output_field": "aliases"}],
    }
    merged = resolve_cumulative_input_props(
        client,
        anchor,
        writer_canvas_node_id=writer,
        predecessor_canvas_node_ids=[],
        raw_db="db_discovery",
        base_table=base,
        run_id=run_id,
        cfg=cfg,
    )
    assert merged == {"name": "P-101"}


def test_two_pass_cumulative_transform_simulation() -> None:
    """Simulate two transforms: passthrough then tag append via resolve + apply."""
    run_id = "20260101T120000.000000Z-abc123"
    writer = "n_writer"
    base = "discovery_state"
    key = instance_cohort_row_key("inst-1", "scope1")
    writer_tbl = node_cohort_table_name(base, run_id, writer)
    anchor_cols = {
        RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
        RUN_ID_COLUMN: run_id,
        "SCOPE_KEY": "scope1",
        "NODE_INSTANCE_ID": "inst-1",
        PROPERTIES_JSON_COLUMN: json.dumps({"name": "P-101"}),
    }
    client = MagicMock()
    store: Dict[str, Dict[str, _FakeRawRow]] = {writer_tbl: {}}
    client.raw.rows = _FakeRawRows(store)
    passthrough_cfg = {
        "handler_id": "trim_whitespace",
        "fields": [{"field_name": "name"}],
        "output_field": "aliases",
        "output_template": "{name}",
        "trim_whitespace": {"mode": "ends_only"},
    }
    props1 = resolve_cumulative_input_props(
        client,
        anchor_cols,
        writer_canvas_node_id=writer,
        predecessor_canvas_node_ids=[],
        raw_db="db_discovery",
        base_table=base,
        run_id=run_id,
        cfg=passthrough_cfg,
    )
    out1 = apply_transform_steps_to_props(props1, passthrough_cfg)[0]
    row1 = build_entity_cohort_row(
        run_id=run_id,
        scope_key="scope1",
        canvas_node_id=writer,
        query_source="transform",
        node_instance_id="inst-1",
        external_id="inst-1",
        entity_type="asset",
        view_space="cdf_cdm",
        view_external_id="CogniteAsset",
        view_version="v1",
        properties=out1,
    )
    store[writer_tbl][key] = _FakeRawRow(key, row1["columns"])

    tag_cfg = {
        "handler_id": "trim_whitespace",
        "fields": [{"field_name": "name"}],
        "output_field": "aliases",
        "output_template": "{name}",
        "trim_whitespace": {"mode": "ends_only"},
    }
    props2 = resolve_cumulative_input_props(
        client,
        anchor_cols,
        writer_canvas_node_id=writer,
        predecessor_canvas_node_ids=[],
        raw_db="db_discovery",
        base_table=base,
        run_id=run_id,
        cfg=tag_cfg,
    )
    out2 = apply_transform_steps_to_props(props2, tag_cfg)[0]
    alias_vals = out2["aliases"]
    if isinstance(alias_vals, str):
        alias_vals = [alias_vals]
    assert alias_vals.count("P-101") == 1


def test_resolve_cumulative_input_dedupes_duplicate_aliases_in_single_branch() -> None:
    run_id = "20260101T120000.000000Z-abc123"
    writer = "n_writer"
    base = "discovery_state"
    key = instance_cohort_row_key("inst-1", "scope1")
    writer_tbl = node_cohort_table_name(base, run_id, writer)
    client = MagicMock()
    client.raw.rows = _FakeRawRows(
        {
            writer_tbl: {
                key: _entity_row(
                    key=key,
                    run_id=run_id,
                    props={"name": "file.pdf", "aliases": ["file.pdf", "file.pdf"]},
                )
            }
        }
    )
    anchor = {
        RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
        RUN_ID_COLUMN: run_id,
        "SCOPE_KEY": "scope1",
        "NODE_INSTANCE_ID": "inst-1",
        PROPERTIES_JSON_COLUMN: json.dumps(
            {"name": "file.pdf", "aliases": ["file.pdf", "file.pdf"]}
        ),
    }
    cfg = {"steps": [{"handler_id": "trim_whitespace", "output_field": "indexKey"}]}
    merged = resolve_cumulative_input_props(
        client,
        anchor,
        writer_canvas_node_id=writer,
        predecessor_canvas_node_ids=[],
        raw_db="db_discovery",
        base_table=base,
        run_id=run_id,
        cfg=cfg,
    )
    assert merged["aliases"] == ["file.pdf"]


def test_resolve_cumulative_input_dedupes_aliases_across_branches() -> None:
    policies = cumulative_field_policies(
        {"steps": [{"handler_id": "trim_whitespace", "output_field": "indexKey"}]}
    )
    merged = merge_property_dicts(
        [{"aliases": "file.pdf"}, {"aliases": ["file.pdf"]}],
        policies,
    )
    assert merged["aliases"] == ["file.pdf"]
