"""Unit tests for discovery save apply (view / raw / classic)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.discovery_save_apply import (  # noqa: E402
    _classic_build_update,
    _coerce_dm_list_property_value,
    _instance_space_and_external_id,
    _prepare_view_apply_properties,
    discovery_apply_classic_save,
    discovery_apply_view_save,
    run_discovery_save_with_status,
)
from cdf_fn_common.discovery_query_shared import INSTANCE_SPACE_COLUMN  # noqa: E402


def test_coerce_dm_list_property_value() -> None:
    assert _coerce_dm_list_property_value(["a", "b"]) == ["a", "b"]
    assert _coerce_dm_list_property_value([{"value": "a", "confidence": 0.9}]) == ["a"]
    assert _coerce_dm_list_property_value(["a", "a", "b", "b"]) == ["a", "b"]
    assert _coerce_dm_list_property_value("a,a,b") == ["a", "b"]
    assert _coerce_dm_list_property_value("TAG-1") == ["TAG-1"]
    assert _coerce_dm_list_property_value("") is None
    assert _prepare_view_apply_properties(
        {"aliases": "", "name": "n"},
        list_properties=frozenset({"aliases"}),
    ) == {"name": "n"}


def test_view_save_requires_view_external_id() -> None:
    with pytest.raises(ValueError, match="view_external_id"):
        discovery_apply_view_save(
            "fn_dm_view_save",
            {"task_id": "t1", "config": {"save_fan_in_mode": "none"}},
            MagicMock(),
            None,
        )


def test_view_save_requires_save_fan_in_mode() -> None:
    with pytest.raises(ValueError, match="save_fan_in_mode"):
        discovery_apply_view_save(
            "fn_dm_view_save",
            {
                "task_id": "t1",
                "config": {"view_external_id": "VE", "view_space": "vs", "view_version": "v1"},
            },
            MagicMock(),
            None,
        )


@patch("cdf_fn_common.discovery_save_apply.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.discovery_save_apply._iter_entity_rows_for_save")
def test_view_save_dry_run_counts_without_apply(mock_iter_rows: MagicMock, _mock_pred: MagicMock) -> None:
    run_id = "run_test_1"
    cols = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        "PROPERTIES_JSON": json.dumps({"name": "N", "description": "D"}),
    }
    mock_iter_rows.return_value = [(0, cols, {"name": "N", "description": "D"})]
    client = MagicMock()
    data = {
        "task_id": "save1",
        "run_id": run_id,
        "dry_run": True,
        "config": {
            "view_space": "vs",
            "view_external_id": "VE",
            "view_version": "v1",
            "instance_space": "sp",
            "save_fan_in_mode": "none",
        },
    }
    summary = discovery_apply_view_save("fn_dm_view_save", data, client, None)
    assert summary["instances_applied"] == 1
    assert summary["dry_run"] is True
    client.data_modeling.instances.apply.assert_not_called()


@patch("cdf_fn_common.discovery_save_apply.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.discovery_save_apply._iter_entity_rows_for_save")
def test_view_save_merge_per_instance_two_rows_one_instance(
    mock_iter_rows: MagicMock, _mock_pred: MagicMock
) -> None:
    run_id = "run_m_1"
    c1 = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "UPDATED_AT": "2020-01-01T00:00:00Z",
        "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        "PROPERTIES_JSON": json.dumps({"aliases": ["a"], "name": "old"}),
    }
    c2 = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "UPDATED_AT": "2021-01-01T00:00:00Z",
        "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        "PROPERTIES_JSON": json.dumps({"aliases": ["b"], "name": "newer"}),
    }
    mock_iter_rows.return_value = [
        (0, c1, {"aliases": ["a"], "name": "old"}),
        (0, c2, {"aliases": ["b"], "name": "newer"}),
    ]
    client = MagicMock()
    data = {
        "task_id": "save_m",
        "run_id": run_id,
        "dry_run": False,
        "config": {
            "view_space": "vs",
            "view_external_id": "VE",
            "view_version": "v1",
            "instance_space": "sp",
            "save_fan_in_mode": "merge_per_instance",
            "save_field_policies": [
                {
                    "property": "aliases",
                    "strategy": "merge_list",
                    "merge_list": {"unique": False, "branch_order": "by_score"},
                },
            ],
        },
    }
    discovery_apply_view_save("fn_dm_view_save", data, client, None)
    client.data_modeling.instances.apply.assert_called_once()
    call_kw = client.data_modeling.instances.apply.call_args[1]
    nodes = call_kw.get("nodes") or client.data_modeling.instances.apply.call_args[0][0]
    props = nodes[0].sources[0].properties
    assert "name" not in props
    assert props["aliases"] == ["b", "a"]


def test_instance_space_from_instance_space_column() -> None:
    cols = {
        "NODE_INSTANCE_ID": "11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        INSTANCE_SPACE_COLUMN: "sp-site",
    }
    inst_space, ext_id = _instance_space_and_external_id(cols, cfg={}, data={}, props={})
    assert inst_space == "sp-site"
    assert ext_id == "ext1"


@patch("cdf_fn_common.discovery_save_apply.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.discovery_save_apply._iter_entity_rows_for_save")
def test_view_save_merge_reports_gather_skipped_missing_identity(
    mock_iter_rows: MagicMock, _mock_pred: MagicMock
) -> None:
    run_id = "run_skip_1"
    cols = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "NODE_INSTANCE_ID": "11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        "PROPERTIES_JSON": json.dumps({"aliases": ["a"]}),
    }
    mock_iter_rows.return_value = [(0, cols, {"aliases": ["a"]})]
    data = {
        "task_id": "save_skip",
        "run_id": run_id,
        "dry_run": True,
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteDescribable",
            "view_version": "v1",
            "save_fan_in_mode": "merge_per_instance",
            "save_field_policies": [
                {
                    "property": "aliases",
                    "strategy": "merge_list",
                    "merge_list": {"unique": False, "branch_order": "by_score"},
                },
            ],
        },
    }
    summary = discovery_apply_view_save("fn_dm_view_save", data, MagicMock(), None)
    assert summary["gather_skipped_missing_identity"] == 1
    assert summary["instances_applied"] == 0


@patch("cdf_fn_common.discovery_save_apply.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.discovery_save_apply._iter_entity_rows_for_save")
def test_view_save_resolves_space_from_node_external_id_and_props(
    mock_iter_rows: MagicMock, _mock_pred: MagicMock
) -> None:
    run_id = "run_ext_1"
    cols = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "NODE_INSTANCE_ID": "my-space:asset-ext-id",
        "EXTERNAL_ID": "asset-ext-id",
        "PROPERTIES_JSON": json.dumps({"aliases": ["x"], "instance_space": "my-space"}),
    }
    mock_iter_rows.return_value = [(0, cols, {"aliases": ["x"], "instance_space": "my-space"})]
    client = MagicMock()
    data = {
        "task_id": "save1",
        "run_id": run_id,
        "dry_run": False,
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteDescribable",
            "view_version": "v1",
            "save_fan_in_mode": "merge_per_instance",
            "save_field_policies": [
                {
                    "property": "aliases",
                    "strategy": "merge_list",
                    "merge_list": {"unique": False, "branch_order": "by_score"},
                },
            ],
        },
    }
    discovery_apply_view_save("fn_dm_view_save", data, client, None)
    client.data_modeling.instances.apply.assert_called_once()
    nodes = client.data_modeling.instances.apply.call_args[1].get("nodes") or client.data_modeling.instances.apply.call_args[0][0]
    assert nodes[0].space == "my-space"
    assert nodes[0].external_id == "asset-ext-id"


@patch("cdf_fn_common.discovery_save_apply.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.discovery_save_apply._iter_entity_rows_for_save")
def test_view_save_coerces_string_aliases_to_list(
    mock_iter_rows: MagicMock, _mock_pred: MagicMock
) -> None:
    run_id = "run_str_aliases"
    cols = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        "PROPERTIES_JSON": json.dumps({"aliases": "TAG-1", "instance_space": "sp"}),
    }
    mock_iter_rows.return_value = [(0, cols, {"aliases": "TAG-1", "instance_space": "sp"})]
    client = MagicMock()
    data = {
        "task_id": "save1",
        "run_id": run_id,
        "dry_run": False,
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteDescribable",
            "view_version": "v1",
            "instance_space": "sp",
            "save_fan_in_mode": "merge_per_instance",
            "save_field_policies": [
                {
                    "property": "aliases",
                    "strategy": "merge_list",
                    "merge_list": {"unique": False, "branch_order": "by_score"},
                },
            ],
        },
    }
    discovery_apply_view_save("fn_dm_view_save", data, client, None)
    nodes = client.data_modeling.instances.apply.call_args[1].get("nodes") or client.data_modeling.instances.apply.call_args[0][0]
    assert nodes[0].sources[0].properties["aliases"] == ["TAG-1"]


@patch("cdf_fn_common.discovery_save_apply.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.discovery_save_apply._iter_entity_rows_for_save")
def test_classic_save_respects_fan_in(mock_iter_rows: MagicMock, _mock_pred: MagicMock) -> None:
    run_id = "run_cl_1"
    cols = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "EXTERNAL_ID": "asset_ext",
        "PROPERTIES_JSON": json.dumps({"name": "FullName", "description": "OnlyDesc"}),
    }
    mock_iter_rows.return_value = [(0, cols, {"name": "FullName", "description": "OnlyDesc"})]
    client = MagicMock()
    data = {
        "task_id": "csave1",
        "run_id": run_id,
        "dry_run": False,
        "config": {
            "resource_type": "assets",
            "save_fan_in_mode": "none",
        },
    }
    discovery_apply_classic_save("fn_dm_classic_save", data, client, None)
    client.assets.update.assert_called_once()


def test_run_discovery_save_with_status_requires_client() -> None:
    from cdf_fn_common.discovery_handler_result import DiscoveryPipelineError

    with pytest.raises(DiscoveryPipelineError, match="fn_dm_view_save failed"):
        run_discovery_save_with_status(
            "fn_dm_view_save",
            {
                "task_id": "x",
                "config": {"view_external_id": "V", "save_fan_in_mode": "none"},
            },
            None,
            discovery_apply_view_save,
        )


def test_classic_build_update_none_without_updatable_fields() -> None:
    assert _classic_build_update("assets", "e1", {}) is None


def test_classic_build_update_asset_when_name_present() -> None:
    from cognite.client.data_classes import AssetUpdate

    upd = _classic_build_update("assets", "e1", {"name": "hello"})
    assert isinstance(upd, AssetUpdate)
