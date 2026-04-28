"""Tests for workflow artifact discovery and ``--clean`` removal."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PKG = Path(__file__).resolve().parents[3]
_SCRIPTS = _PKG / "scripts"
for _p in (_PKG, _SCRIPTS):
    s = str(_p)
    if s not in sys.path:
        sys.path.insert(0, s)

from scope_build.builders.workflow_triggers import LEGACY_MONOLITHIC_NAME
from scope_build.orchestrate import main as orchestrate_main
from scope_build.workflow_clean import discover_workflow_artifact_paths, run_clean_workflow_artifacts


def _layout(module: Path, wf_base: str = "my_wf") -> None:
    wf = module / "workflows"
    wf.mkdir(parents=True)
    (wf / f"{wf_base}.root.Workflow.yaml").write_text("x", encoding="utf-8")
    sub = wf / "leaf"
    sub.mkdir()
    (sub / f"{wf_base}.leaf.WorkflowVersion.yaml").write_text("y", encoding="utf-8")
    (wf / LEGACY_MONOLITHIC_NAME).write_text("z", encoding="utf-8")
    (wf / f"cdf_{wf_base}_oldstuff.WorkflowTrigger.yaml").write_text("t", encoding="utf-8")
    (module / "workflow_template").mkdir()
    (module / "workflow_template" / "workflow.template.Workflow.yaml").write_text("{}", encoding="utf-8")
    (wf / "README.md").write_text("# workflows\n", encoding="utf-8")
    (wf / "other_scope.x.Workflow.yaml").write_text("keep", encoding="utf-8")


def test_discover_root_nested_and_legacy(tmp_path: Path) -> None:
    module = tmp_path / "mod"
    _layout(module, "my_wf")
    paths = discover_workflow_artifact_paths(module, "my_wf")
    assert len(paths) == 4
    names = {p.name for p in paths}
    assert "my_wf.root.Workflow.yaml" in names
    assert "my_wf.leaf.WorkflowVersion.yaml" in names
    assert LEGACY_MONOLITHIC_NAME in names
    assert "cdf_my_wf_oldstuff.WorkflowTrigger.yaml" in names


def test_discover_wf_base_underscore_root_glob(tmp_path: Path) -> None:
    """Root-only ``{wf_base}_*.WorkflowTrigger.yaml`` (legacy naming) is included."""
    module = tmp_path / "mod"
    wf = module / "workflows"
    wf.mkdir(parents=True)
    (wf / "my_wf_site01.WorkflowTrigger.yaml").write_text("t", encoding="utf-8")
    paths = discover_workflow_artifact_paths(module, "my_wf")
    assert len(paths) == 1
    assert paths[0].name == "my_wf_site01.WorkflowTrigger.yaml"


def test_discover_no_workflows_dir(tmp_path: Path) -> None:
    module = tmp_path / "mod"
    module.mkdir()
    assert discover_workflow_artifact_paths(module, "any") == []


def test_format_summary_truncates_over_50_files(tmp_path: Path) -> None:
    from scope_build.workflow_clean import _format_summary_lines

    module = tmp_path / "mod"
    wf = module / "workflows"
    wf.mkdir(parents=True)
    paths = []
    for i in range(55):
        p = wf / f"my_wf.s{i}.Workflow.yaml"
        p.write_text("x", encoding="utf-8")
        paths.append(p.resolve())
    lines, more = _format_summary_lines(paths, module)
    assert len(lines) == 50
    assert more == 5


def test_run_clean_assume_yes_removes_only_matches(tmp_path: Path) -> None:
    module = tmp_path / "mod"
    _layout(module, "my_wf")
    readme = module / "workflows" / "README.md"
    tmpl = module / "workflow_template" / "workflow.template.Workflow.yaml"
    other = module / "workflows" / "other_scope.x.Workflow.yaml"
    assert readme.is_file() and tmpl.is_file() and other.is_file()
    code = run_clean_workflow_artifacts(
        module,
        "my_wf",
        dry_run=False,
        assume_yes=True,
        stdin_isatty=False,
    )
    assert code == 0
    assert readme.is_file() and tmpl.is_file() and other.is_file()
    assert discover_workflow_artifact_paths(module, "my_wf") == []
    assert not (module / "workflows" / "leaf").is_dir()


def test_run_clean_dry_run_no_delete(tmp_path: Path) -> None:
    module = tmp_path / "mod"
    _layout(module, "my_wf")
    p = module / "workflows" / "my_wf.root.Workflow.yaml"
    code = run_clean_workflow_artifacts(
        module,
        "my_wf",
        dry_run=True,
        assume_yes=False,
        stdin_isatty=False,
    )
    assert code == 0
    assert p.is_file()


def test_run_clean_non_tty_without_yes_aborts(tmp_path: Path) -> None:
    module = tmp_path / "mod"
    _layout(module, "my_wf")
    p = module / "workflows" / "my_wf.root.Workflow.yaml"
    code = run_clean_workflow_artifacts(
        module,
        "my_wf",
        dry_run=False,
        assume_yes=False,
        stdin_isatty=False,
    )
    assert code == 1
    assert p.is_file()


def test_orchestrate_clean_rejected_with_check_workflow_triggers() -> None:
    assert orchestrate_main(["--clean", "--check-workflow-triggers"]) == 1


def test_orchestrate_clean_rejected_with_list_builders() -> None:
    assert orchestrate_main(["--clean", "--list-builders"]) == 1


def test_run_clean_input_confirms_with_yes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = tmp_path / "mod"
    _layout(module, "my_wf")
    p = module / "workflows" / "my_wf.root.Workflow.yaml"
    monkeypatch.setattr("builtins.input", lambda: "yes")
    code = run_clean_workflow_artifacts(
        module,
        "my_wf",
        dry_run=False,
        assume_yes=False,
        stdin_isatty=True,
    )
    assert code == 0
    assert not p.is_file()


def test_run_clean_input_refusal(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = tmp_path / "mod"
    _layout(module, "my_wf")
    p = module / "workflows" / "my_wf.root.Workflow.yaml"
    monkeypatch.setattr("builtins.input", lambda: "no")
    code = run_clean_workflow_artifacts(
        module,
        "my_wf",
        dry_run=False,
        assume_yes=False,
        stdin_isatty=True,
    )
    assert code == 1
    assert p.is_file()
