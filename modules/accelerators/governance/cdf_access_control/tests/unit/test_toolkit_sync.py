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
    g = {"source_ids": {"g1": "uuid-1"}, "sourceId": "fallback"}
    assert resolve_group_source_id(g, "g1") == "uuid-1"
    assert resolve_group_source_id(g, "other") == "fallback"


def test_merge_source_ids_into_default_config(tmp_path):
    p = tmp_path / "default.config.yaml"
    p.write_text("groups:\n  global:\n    source_ids: {}\n", encoding="utf-8")
    assert merge_source_ids_into_default_config(
        p, {"grp": "00000000-0000-0000-0000-000000000099"}, dry_run=False
    )
    doc = yaml.safe_load(p.read_text(encoding="utf-8"))
    assert doc["groups"]["global"]["source_ids"]["grp"].startswith("00000000")
