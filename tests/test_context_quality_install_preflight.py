"""Tests for the read-only preflight script of the context-quality-install skill."""

import json
import sys
from pathlib import Path

import pytest

SCRIPTS = (
    Path(__file__).resolve().parents[1]
    / "modules"
    / "dashboards"
    / "context_quality"
    / "skills"
    / "context-quality-install"
    / "scripts"
)
sys.path.insert(0, str(SCRIPTS))
import preflight  # pyright: ignore[reportMissingImports]

ENV_NAMES = (
    "CQ_FUNCTION_GROUP_SOURCE_ID",
    "CQ_DASHBOARD_GROUP_SOURCE_ID",
    "CQ_FUNCTION_CLIENT_ID",
    "CQ_FUNCTION_CLIENT_SECRET",
)
SECRET = "s3cr3t-value-that-must-not-leak"


def write_env(path: Path, values: dict[str, str]) -> None:
    path.write_text("".join(f"{name}={value}\n" for name, value in values.items()))


def write_app_json(app_dir: Path, project: str) -> None:
    app_dir.mkdir(parents=True, exist_ok=True)
    (app_dir / "app.json").write_text(json.dumps({"deployments": [{"org": "o", "project": project}]}))


@pytest.mark.parametrize(
    ("output", "expected"),
    [("CDF-Toolkit version: 0.8.191.", (0, 8, 191)), ("v20.11.1", (20, 11, 1)), ("no version here", None)],
)
def test_parse_version(output: str, expected: tuple[int, int, int] | None) -> None:
    assert preflight.parse_version(output) == expected


@pytest.mark.parametrize(
    ("output", "status"),
    [("CDF-Toolkit version: 0.8.191.", "PASS"), ("CDF-Toolkit version: 0.7.99.", "FAIL"), (None, "FAIL")],
)
def test_check_toolkit(output: str | None, status: str) -> None:
    assert preflight.check_toolkit(output).status == status


def test_record_views_passes_when_flag_is_set(tmp_path: Path) -> None:
    (tmp_path / "cdf.toml").write_text("[alpha_flags]\nrecord_views = true\n")
    assert preflight.check_record_views(tmp_path / "cdf.toml", (0, 8, 191)).status == "PASS"


def test_record_views_fails_on_0_8_without_flag(tmp_path: Path) -> None:
    (tmp_path / "cdf.toml").write_text("[cdf]\ndefault_env = 'dev'\n")
    assert preflight.check_record_views(tmp_path / "cdf.toml", (0, 8, 191)).status == "FAIL"


def test_record_views_is_not_checked_on_0_9(tmp_path: Path) -> None:
    (tmp_path / "cdf.toml").write_text("[cdf]\ndefault_env = 'dev'\n")
    assert preflight.check_record_views(tmp_path / "cdf.toml", (0, 9, 39)).status == "SKIP"


def test_record_views_fails_without_cdf_toml(tmp_path: Path) -> None:
    assert preflight.check_record_views(tmp_path / "cdf.toml", (0, 8, 191)).status == "FAIL"


@pytest.mark.parametrize(("output", "status"), [("v20.11.1", "PASS"), ("v18.19.0", "FAIL"), (None, "FAIL")])
def test_check_node(output: str | None, status: str) -> None:
    assert preflight.check_node(output).status == status


def test_env_vars_pass_when_all_are_set(tmp_path: Path) -> None:
    write_env(tmp_path / ".env", dict.fromkeys(ENV_NAMES, SECRET))
    check = preflight.check_env_vars(tmp_path / ".env", {})
    assert check.status == "PASS"
    assert SECRET not in check.detail


def test_env_vars_name_the_empty_one_without_printing_values(tmp_path: Path) -> None:
    values = dict.fromkeys(ENV_NAMES, SECRET)
    values["CQ_FUNCTION_CLIENT_SECRET"] = ""
    write_env(tmp_path / ".env", values)
    check = preflight.check_env_vars(tmp_path / ".env", {})
    assert check.status == "FAIL"
    assert "CQ_FUNCTION_CLIENT_SECRET" in check.detail
    assert SECRET not in check.detail


def test_env_vars_accept_the_process_environment(tmp_path: Path) -> None:
    check = preflight.check_env_vars(tmp_path / ".env", dict.fromkeys(ENV_NAMES, SECRET))
    assert check.status == "PASS"


def test_app_json_fails_on_placeholder_project(tmp_path: Path) -> None:
    write_app_json(tmp_path, "your-project")
    assert preflight.check_app_json(tmp_path).status == "FAIL"


def test_app_json_passes_with_a_project(tmp_path: Path) -> None:
    write_app_json(tmp_path, "acme-dev")
    assert preflight.check_app_json(tmp_path).status == "PASS"


def test_app_json_is_skipped_without_app_dir() -> None:
    assert preflight.check_app_json(None).status == "SKIP"


def test_main_prints_a_table_and_never_a_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "cdf.toml").write_text("[alpha_flags]\nrecord_views = true\n")
    write_env(tmp_path / ".env", dict.fromkeys(ENV_NAMES, SECRET))
    write_app_json(tmp_path / "app", "acme-dev")
    outputs = {"cdf": "CDF-Toolkit version: 0.8.191.", "node": "v20.11.1"}
    monkeypatch.setattr(preflight, "run_command", lambda command: outputs[command[-2]])
    for name in ENV_NAMES:
        monkeypatch.delenv(name, raising=False)

    exit_code = preflight.main(["--project-dir", str(tmp_path), "--app-dir", str(tmp_path / "app")])

    printed = capsys.readouterr().out
    assert exit_code == 0
    assert "PASS" in printed
    assert SECRET not in printed


def test_main_fails_when_a_check_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(preflight, "run_command", lambda command: None)
    assert preflight.main(["--project-dir", str(tmp_path)]) == 1


def test_backend_scope_does_not_need_node(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "cdf.toml").write_text("[alpha_flags]\nrecord_views = true\n")
    write_env(tmp_path / ".env", dict.fromkeys(ENV_NAMES, SECRET))
    outputs = {"cdf": "CDF-Toolkit version: 0.8.191.", "node": None}
    monkeypatch.setattr(preflight, "run_command", lambda command: outputs[command[-2]])
    for name in ENV_NAMES:
        monkeypatch.delenv(name, raising=False)

    exit_code = preflight.main(["--project-dir", str(tmp_path), "--scope", "backend"])

    assert exit_code == 0
    assert "Node" in capsys.readouterr().out


def test_app_scope_does_not_need_toolkit_or_record_views(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_app_json(tmp_path / "app", "acme-dev")
    outputs = {"cdf": None, "node": "v20.11.1"}
    monkeypatch.setattr(preflight, "run_command", lambda command: outputs[command[-2]])

    exit_code = preflight.main(["--project-dir", str(tmp_path), "--app-dir", str(tmp_path / "app"), "--scope", "app"])

    assert exit_code == 0


BAD_BYTES = b"\xff\xfe\x00not utf-8 \x80"


def test_unreadable_cdf_toml_fails_instead_of_crashing(tmp_path: Path) -> None:
    cdf_toml = tmp_path / "cdf.toml"
    cdf_toml.write_bytes(BAD_BYTES)
    check = preflight.check_record_views(cdf_toml, (0, 8, 191))
    assert check.status == "FAIL"
    assert "cdf.toml" in check.detail


def test_env_file_with_bad_encoding_counts_as_no_names_set(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_bytes(BAD_BYTES)
    assert preflight.names_set_in(env_file) == set()


def test_unreadable_app_json_fails_instead_of_crashing(tmp_path: Path) -> None:
    (tmp_path / "app.json").write_bytes(BAD_BYTES)
    check = preflight.check_app_json(tmp_path)
    assert check.status == "FAIL"
    assert "app.json" in check.detail
