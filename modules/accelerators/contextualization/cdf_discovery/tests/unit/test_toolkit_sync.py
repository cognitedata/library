import re
from pathlib import Path

import yaml

from governance_build.toolkit_sync import (
    is_toolkit_source_id_placeholder,
    merge_source_ids_into_default_config,
    resolve_group_source_id,
)


def test_is_toolkit_source_id_placeholder():
    assert is_toolkit_source_id_placeholder("{{ my_var }}")
    assert not is_toolkit_source_id_placeholder("00000000-0000-0000-0000-000000000001")


def test_resolve_group_source_id():
    g = {"source_ids": {"g1": "uuid-1"}}
    assert resolve_group_source_id(g, "g1") == "uuid-1"
    assert resolve_group_source_id(g, "other") == ""


def test_merge_source_ids_into_default_config(tmp_path):
    p = tmp_path / "default.config.yaml"
    p.write_text("groups:\n  global:\n    source_ids: {}\n", encoding="utf-8")
    assert merge_source_ids_into_default_config(
        p, {"grp": "00000000-0000-0000-0000-000000000099"}, dry_run=False
    )
    doc = yaml.safe_load(p.read_text(encoding="utf-8"))
    assert doc["groups"]["global"]["source_ids"]["grp"].startswith("00000000")


def test_merge_source_ids_into_toolkit_env_variables(tmp_path):
    p = tmp_path / "config.dev.yaml"
    p.write_text(
        "environment:\n"
        "  name: dev\n"
        "  project: test\n"
        "  validation-type: dev\n"
        "variables: {}\n",
        encoding="utf-8",
    )
    assert merge_source_ids_into_default_config(
        p, {"gp_asset_site_a_read": "00000000-0000-0000-0000-000000000123"}, dry_run=False
    )
    doc = yaml.safe_load(p.read_text(encoding="utf-8"))
    assert (
        doc["variables"]["modules"]["cdf_discovery"]["gp_asset_site_a_read"]
        == "00000000-0000-0000-0000-000000000123"
    )


def test_seed_toolkit_env_variables_when_source_id_missing(tmp_path):
    p = tmp_path / "config.dev.yaml"
    p.write_text(
        "environment:\n"
        "  name: dev\n"
        "  project: test\n"
        "  validation-type: dev\n"
        "variables: {}\n",
        encoding="utf-8",
    )
    assert merge_source_ids_into_default_config(
        p,
        {"gp_asset_site_a_read": "", "gp_timeseries_site_a_read": "{{ gp_timeseries_site_a_read }}"},
        dry_run=False,
    )
    doc = yaml.safe_load(p.read_text(encoding="utf-8"))
    assert doc["variables"]["modules"]["cdf_discovery"]["gp_asset_site_a_read"] == ""
    assert doc["variables"]["modules"]["cdf_discovery"]["gp_timeseries_site_a_read"] == ""


def test_seed_toolkit_env_variables_in_auth_scope_path(tmp_path):
    p = tmp_path / "config.dev.yaml"
    p.write_text(
        "environment:\n"
        "  name: dev\n"
        "  project: test\n"
        "  validation-type: dev\n"
        "variables: {}\n",
        encoding="utf-8",
    )
    assert merge_source_ids_into_default_config(
        p,
        {"gp_asset_site_b_read": "", "gp_asset_site_a_unit_02_read": ""},
        dry_run=False,
        rel_paths={
            "gp_asset_site_b_read": "auth/site_b/gp_asset_site_b_read.Group.yaml",
            "gp_asset_site_a_unit_02_read": "auth/site_a/unit_02/gp_asset_site_a_unit_02_read.Group.yaml",
        },
    )
    doc = yaml.safe_load(p.read_text(encoding="utf-8"))
    assert doc["variables"]["modules"]["cdf_discovery"]["site_b"]["gp_asset_site_b_read"] == ""
    assert (
        doc["variables"]["modules"]["cdf_discovery"]["site_a"]["unit_02"]["gp_asset_site_a_unit_02_read"]
        == ""
    )
