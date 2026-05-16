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
    discovery_apply_classic_save,
    discovery_apply_view_save,
    run_discovery_save_with_status,
)


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
@patch("cdf_fn_common.discovery_save_apply.iter_inter_node_raw_rows_for_filter_run")
def test_view_save_dry_run_counts_without_apply(
    mock_iter_raw: MagicMock, _mock_pred: MagicMock
) -> None:
    run_id = "run_test_1"
    row = MagicMock()
    row.key = "rk"
    row.columns = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        "PROPERTIES_JSON": json.dumps({"name": "N", "description": "D"}),
    }
    mock_iter_raw.return_value = [row]
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
@patch("cdf_fn_common.discovery_save_apply.iter_inter_node_raw_rows_for_filter_run")
def test_view_save_merge_per_instance_two_rows_one_instance(
    mock_iter_raw: MagicMock, _mock_pred: MagicMock
) -> None:
    run_id = "run_m_1"
    r1 = MagicMock()
    r1.key = "k1"
    r1.columns = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "UPDATED_AT": "2020-01-01T00:00:00Z",
        "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        "PROPERTIES_JSON": json.dumps({"aliases": ["a"], "name": "old"}),
    }
    r2 = MagicMock()
    r2.key = "k2"
    r2.columns = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "UPDATED_AT": "2021-01-01T00:00:00Z",
        "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        "PROPERTIES_JSON": json.dumps({"aliases": ["b"], "name": "newer"}),
    }
    mock_iter_raw.return_value = [r1, r2]
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
    assert props["name"] == "newer"
    assert props["aliases"] == "b,a"


@patch("cdf_fn_common.discovery_save_apply.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.discovery_save_apply.iter_inter_node_raw_rows_for_filter_run")
def test_classic_save_respects_fan_in(
    mock_iter_raw: MagicMock, _mock_pred: MagicMock
) -> None:
    run_id = "run_cl_1"
    row = MagicMock()
    row.key = "rk"
    row.columns = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "EXTERNAL_ID": "asset_ext",
        "PROPERTIES_JSON": json.dumps({"name": "FullName", "description": "OnlyDesc"}),
    }
    mock_iter_raw.return_value = [row]
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
    out = run_discovery_save_with_status(
        "fn_dm_view_save",
        {
            "task_id": "x",
            "config": {"view_external_id": "V", "save_fan_in_mode": "none"},
        },
        None,
        discovery_apply_view_save,
    )
    assert out["status"] == "failure"


def test_classic_build_update_none_without_updatable_fields() -> None:
    assert _classic_build_update("assets", "e1", {}) is None


def test_classic_build_update_asset_when_name_present() -> None:
    from cognite.client.data_classes import AssetUpdate

    upd = _classic_build_update("assets", "e1", {"name": "hello"})
    assert isinstance(upd, AssetUpdate)
