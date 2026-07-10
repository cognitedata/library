from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPTS = ROOT / "scripts"
for p in (str(ROOT), str(SCRIPTS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from types import SimpleNamespace

import deploy_workflow_cdf as deploy_script  # noqa: E402
import runtime_paths  # noqa: E402

DISCOVERY_ROOT = ROOT.parent.parent


def test_deploy_runs_build_before_artifact_validation(monkeypatch) -> None:
    calls: list[str] = []

    def fake_ensure_import_paths(_f):
        return DISCOVERY_ROOT

    def fake_transform_root_from_path(_f):
        return ROOT

    def fake_resolve_workflow_artifacts(_tr: Path, _wid: str, _suffix: str):
        return {
            "dir": DISCOVERY_ROOT / "workflows",
            "workflow": DISCOVERY_ROOT / "workflows" / "etl_test.Workflow.yaml",
            "workflow_version": DISCOVERY_ROOT / "workflows" / "etl_test.WorkflowVersion.yaml",
            "trigger": DISCOVERY_ROOT / "workflows" / "etl_test.WorkflowTrigger.yaml",
        }

    def fake_validate_artifacts(_paths):
        calls.append("validate")

    def fake_run_subprocess(_argv, *, cwd: Path, dry_run: bool) -> int:
        _ = cwd
        _ = dry_run
        calls.append("build")
        return 0

    monkeypatch.setattr(runtime_paths, "ensure_import_paths", fake_ensure_import_paths)
    monkeypatch.setattr(runtime_paths, "transform_root_from_path", fake_transform_root_from_path)
    monkeypatch.setattr(deploy_script, "_validate_artifacts", fake_validate_artifacts)
    monkeypatch.setattr(deploy_script, "_run_subprocess", fake_run_subprocess)

    # Avoid importing SDK-dependent deploy modules in this unit test.
    monkeypatch.setitem(
        sys.modules,
        "deploy_kea_functions_cdf_api",
        SimpleNamespace(
            DeployFunctionsMode=str,
            deploy_kea_functions=lambda *_args, **_kwargs: None,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "deploy_workflows_cdf_api",
        SimpleNamespace(deploy_workflows=lambda *_args, **_kwargs: None),
    )
    monkeypatch.setitem(
        sys.modules,
        "workflow_deploy_paths",
        SimpleNamespace(resolve_workflow_artifacts=fake_resolve_workflow_artifacts),
    )
    monkeypatch.setitem(
        sys.modules,
        "local_runner.client",
        SimpleNamespace(create_cognite_client=lambda: object()),
    )
    monkeypatch.setitem(
        sys.modules,
        "local_runner.env",
        SimpleNamespace(load_env=lambda: None),
    )

    rc = deploy_script.main(["--workflow", "test_workflow", "--dry-run"])
    assert rc == 0
    assert calls == ["build", "validate"]
