"""Config path registry and governance mirroring."""

from pathlib import Path

import yaml

from governance_build.config_sources import (
    PRIMARY_CONFIG,
    merge_governance_into_document,
    mirror_config_relpaths,
    mirror_governance_slice,
)


def test_mirror_relpaths_reads_default_config(tmp_path):
    (tmp_path / PRIMARY_CONFIG).write_text(
        "governance_ui:\n  mirror_config_paths:\n    - config.dev.yaml\n",
        encoding="utf-8",
    )
    assert mirror_config_relpaths(tmp_path) == ["config.dev.yaml"]


def test_merge_governance_preserves_other_keys():
    base = {"toolkit": {"x": 1}, "dimensions": {"a": 1}, "extra": 2}
    src = {
        "dimensions": {"b": 2},
        "groups": {"global": {"source_ids": {"from_src": "x"}}},
    }
    out = merge_governance_into_document(base, src)
    assert out["toolkit"] == {"x": 1}
    assert out["extra"] == 2
    assert out["dimensions"] == {"b": 2}
    assert out["groups"]["global"]["source_ids"] == {}
    assert "sourceId" not in out["groups"]["global"]


def test_mirror_writes_multiple_files(tmp_path):
    (tmp_path / PRIMARY_CONFIG).write_text(
        "governance_ui:\n  mirror_config_paths:\n    - config.dev.yaml\n",
        encoding="utf-8",
    )
    (tmp_path / "config.dev.yaml").write_text("dimensions: {}\n", encoding="utf-8")
    slice_doc = {"dimensions": {"x": {"type": "list", "items": [{"id": "a"}]}}}
    written, skipped = mirror_governance_slice(tmp_path, slice_doc, dry_run=False)
    assert "config.dev.yaml" in written
    dev = yaml.safe_load((tmp_path / "config.dev.yaml").read_text(encoding="utf-8"))
    assert "x" in dev["dimensions"]
