from pathlib import Path

from governance_build.orchestrate import load_state, run_clean, save_state


def test_clean_removes_empty_parent_folders(tmp_path):
    rel = "spaces/site_a/foo.Space.yaml"
    p = tmp_path / rel
    p.parent.mkdir(parents=True)
    p.write_text("x: 1\n", encoding="utf-8")
    save_state(tmp_path, [rel], [])
    assert run_clean(tmp_path, dry_run=False, yes=True) == 0
    assert not p.is_file()
    assert load_state(tmp_path) is None
