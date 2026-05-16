"""Rules for writing Space/Group YAML (skip deleted manifest paths unless --force)."""

from pathlib import Path

from governance_build.orchestrate import _emit_yaml_artifact


def test_emit_creates_when_no_prior_manifest(tmp_path):
    p = tmp_path / "spaces" / "a.Space.yaml"
    wrote, sync = _emit_yaml_artifact(
        p, "x: 1\n", "spaces/a.Space.yaml", dry_run=False, force=False, prev_manifest_rels=None
    )
    assert wrote is True
    assert sync is True
    assert p.read_text(encoding="utf-8") == "x: 1\n"


def test_emit_skips_missing_when_in_prior_manifest(tmp_path):
    p = tmp_path / "spaces" / "a.Space.yaml"
    rel = "spaces/a.Space.yaml"
    wrote, sync = _emit_yaml_artifact(
        p, "x: 1\n", rel, dry_run=False, force=False, prev_manifest_rels={rel}
    )
    assert wrote is False
    assert sync is False
    assert not p.exists()


def test_emit_force_restores_deleted(tmp_path):
    p = tmp_path / "spaces" / "a.Space.yaml"
    rel = "spaces/a.Space.yaml"
    wrote, sync = _emit_yaml_artifact(
        p, "x: 1\n", rel, dry_run=False, force=True, prev_manifest_rels={rel}
    )
    assert wrote is True
    assert sync is True
    assert p.is_file()
