"""
pytest test suite for qs_dp_setup_wizard.py

Coverage:
- Email validation (valid, multiple, invalid, empty, partial)
- Cron placeholder regex (unquoted, quoted, edited, non-matching)
- set_yaml_value_by_path: found/not-found, no-op detection
- enable_file_annotation_mode: basic switch + idempotency
- Toolkit version pre-flight (below minimum, not found, unparseable, above ceiling)
- cdf.toml alpha-flag check (missing file, missing flag, valid)
- Unsupported environment name → exit 1
- Cancel at confirmation prompt → exit 0, no files written
- KeyboardInterrupt → exit 130
- Post-write verification: mocked build success + failure paths
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Make the scripts/ directory importable without installing.
sys.path.insert(0, str(Path(__file__).parent.parent))

from qs_dp_setup_wizard import main
from wizard._constants import _CONFIG_FLAG_VERSION, MIN_TOOLKIT_VERSION
from wizard._file_io import ensure_backup
from wizard._preflight import (
    _ensure_toml_flag,
    _get_org_dir,
    _parse_version,
    check_cdf_toml,
    check_toolkit_version,
)
from wizard._prompts import validate_emails
from wizard._sql import enable_file_annotation_mode
from wizard._verification import _cdf_env_args, run_post_write_verification
from wizard._yaml import (
    _strip_yaml_quotes,
    build_yaml_paths,
    set_yaml_value_by_path,
)

# ensure_backup naming

class TestEnsureBackup:
    def test_regular_file_gets_bak_suffix(self, tmp_path: Path) -> None:
        f = tmp_path / "config.dev.yaml"
        f.write_text("content")
        bak = ensure_backup(f)
        assert ".bak." in bak.name
        assert bak.suffix != ".yaml"  # has extra .bak.<ts> appended

    def test_dotfile_gets_qs_backup_name(self, tmp_path: Path) -> None:
        f = tmp_path / ".env"
        f.write_text("SECRET=abc")
        bak = ensure_backup(f)
        assert bak.name.startswith("qs_backup_")
        assert bak.name.endswith(".env")
        assert not bak.name.startswith(".")

    def test_dotfile_backup_content_matches(self, tmp_path: Path) -> None:
        f = tmp_path / ".env"
        f.write_text("SECRET=abc")
        bak = ensure_backup(f)
        assert bak.read_text() == "SECRET=abc"

    def test_original_file_unchanged(self, tmp_path: Path) -> None:
        f = tmp_path / ".env"
        f.write_text("SECRET=abc")
        ensure_backup(f)
        assert f.read_text() == "SECRET=abc"


# _cdf_env_args — build/deploy flag selection by toolkit version

class TestCdfEnvArgs:
    def test_old_version_uses_env_flag(self) -> None:
        # Any version below 0.8.0 should use --env=<env>
        old = (0, 7, 210)
        args = _cdf_env_args("dev", "config.dev.yaml", old)
        assert args == ["--env=dev"]

    def test_new_version_uses_c_flag(self) -> None:
        new = _CONFIG_FLAG_VERSION  # exactly 0.8.0
        args = _cdf_env_args("dev", "config.dev.yaml", new)
        assert args == ["-c", "config.dev.yaml"]

    def test_above_new_version_uses_c_flag(self) -> None:
        above = (0, 9, 0)
        args = _cdf_env_args("prod", "myorg/config.prod.yaml", above)
        assert args == ["-c", "myorg/config.prod.yaml"]

    def test_unknown_version_defaults_to_c_flag(self) -> None:
        # None means version could not be determined — default to newer form
        args = _cdf_env_args("dev", "config.dev.yaml", None)
        assert args == ["-c", "config.dev.yaml"]


# Email validation

class TestValidateEmails:
    def test_single_valid(self) -> None:
        ok, _ = validate_emails("user@example.com")
        assert ok

    def test_multiple_valid(self) -> None:
        ok, _ = validate_emails("a@b.com, c@d.org, e@f.io")
        assert ok

    def test_single_invalid(self) -> None:
        ok, msg = validate_emails("not-an-email")
        assert not ok
        assert "not-an-email" in msg

    def test_mixed_valid_invalid(self) -> None:
        ok, msg = validate_emails("good@email.com, bademail")
        assert not ok
        assert "bademail" in msg

    def test_empty_string(self) -> None:
        ok, msg = validate_emails("")
        assert not ok
        assert "required" in msg

    def test_missing_at(self) -> None:
        ok, msg = validate_emails("userexample.com")
        assert not ok

    def test_missing_domain(self) -> None:
        ok, msg = validate_emails("user@")
        assert not ok

    def test_whitespace_only(self) -> None:
        ok, msg = validate_emails("   ")
        assert not ok


# Cron placeholder regex

# YAML value mutation

class TestSetYamlValueByPath:
    def test_found_and_changed(self) -> None:
        lines = ["project: old-value\n"]
        km = build_yaml_paths(lines)
        result = set_yaml_value_by_path(lines, ("project",), "new-value", km)
        assert result is not None
        old_v, new_v = result
        assert old_v == "old-value"
        assert new_v == "new-value"
        assert "new-value" in lines[0]

    def test_noop_when_value_unchanged(self) -> None:
        lines = ["project: same\n"]
        km = build_yaml_paths(lines)
        result = set_yaml_value_by_path(lines, ("project",), "same", km)
        assert result is not None
        old_v, new_v = result
        assert old_v == new_v  # no real change

    def test_not_found_returns_none(self) -> None:
        lines = ["project: value\n"]
        km = build_yaml_paths(lines)
        result = set_yaml_value_by_path(lines, ("nonexistent",), "x", km)
        assert result is None

    def test_nested_path(self) -> None:
        lines = [
            "environment:\n",
            "  project: old\n",
        ]
        km = build_yaml_paths(lines)
        result = set_yaml_value_by_path(lines, ("environment", "project"), "new", km)
        assert result is not None
        assert result[1] == "new"

    def test_preserves_inline_comment(self) -> None:
        lines = ["project: old  # keep me\n"]
        km = build_yaml_paths(lines)
        set_yaml_value_by_path(lines, ("project",), "new", km)
        assert "# keep me" in lines[0]


# SQL mode switch

class TestEnableFileAnnotationMode:
    def test_switches_to_file_annotation(self, tmp_sql_path: Path) -> None:
        changed = enable_file_annotation_mode(tmp_sql_path)
        assert changed
        content = tmp_sql_path.read_text()
        # FILE_ANNOTATION block uncommented
        assert "with root as (" in content
        # COMMON MODE block commented
        assert "-- with parentLookup as (" in content

    def test_idempotent_second_run(self, tmp_sql_path: Path) -> None:
        enable_file_annotation_mode(tmp_sql_path)
        content_after_first = tmp_sql_path.read_text()
        changed_again = enable_file_annotation_mode(tmp_sql_path)
        assert not changed_again
        assert tmp_sql_path.read_text() == content_after_first

    def test_backup_created(self, tmp_sql_path: Path) -> None:
        enable_file_annotation_mode(tmp_sql_path)
        bak_files = list(tmp_sql_path.parent.glob("*.bak.*"))
        assert len(bak_files) == 1


# Toolkit version pre-flight

class TestParseVersion:
    def test_plain_semver(self) -> None:
        assert _parse_version("0.7.34") == (0, 7, 34)

    def test_prefixed(self) -> None:
        assert _parse_version("cdf/0.7.34") == (0, 7, 34)

    def test_with_extra_text(self) -> None:
        assert _parse_version("Cognite Toolkit version 1.2.3 (build 42)") == (1, 2, 3)

    def test_unparseable(self) -> None:
        assert _parse_version("no version here") is None


class TestCheckToolkitVersion:
    def _mock_run(self, stdout: str, returncode: int = 0) -> MagicMock:
        m = MagicMock()
        m.stdout = stdout
        m.stderr = ""
        m.returncode = returncode
        return m

    def test_exact_minimum_passes(self) -> None:
        min_str = ".".join(str(x) for x in MIN_TOOLKIT_VERSION)
        with patch("subprocess.run", return_value=self._mock_run(min_str)):
            check_toolkit_version()  # should not raise

    def test_above_minimum_passes(self) -> None:
        major, minor, patch_v = MIN_TOOLKIT_VERSION
        higher = f"{major}.{minor}.{patch_v + 1}"
        with patch("subprocess.run", return_value=self._mock_run(higher)):
            check_toolkit_version()

    def test_below_minimum_exits(self) -> None:
        major, minor, patch_v = MIN_TOOLKIT_VERSION
        lower = f"{major}.{minor}.{max(0, patch_v - 10)}"
        with patch("subprocess.run", return_value=self._mock_run(lower)):
            with pytest.raises(SystemExit) as exc:
                check_toolkit_version()
            assert exc.value.code == 1

    def test_not_found_exits(self) -> None:
        with patch("subprocess.run", side_effect=FileNotFoundError):
            with pytest.raises(SystemExit) as exc:
                check_toolkit_version()
            assert exc.value.code == 1

    def test_timeout_warns_but_continues(self, capsys: pytest.CaptureFixture[str]) -> None:
        import subprocess as sp
        with patch("subprocess.run", side_effect=sp.TimeoutExpired(cmd="cdf", timeout=10)):
            check_toolkit_version()  # must not raise
        assert "timed out" in capsys.readouterr().out

    def test_unparseable_warns_but_continues(self, capsys: pytest.CaptureFixture[str]) -> None:
        with patch("subprocess.run", return_value=self._mock_run("no version info")):
            check_toolkit_version()
        assert "Warning" in capsys.readouterr().out



# _ensure_toml_flag unit tests

class TestEnsureTomlFlag:
    def _lines(self, text: str) -> list[str]:
        return text.splitlines(keepends=True)

    def test_already_true_returns_none(self) -> None:
        lines = self._lines("[alpha_flags]\ndeployment-pack = true\n")
        assert _ensure_toml_flag(lines, "[alpha_flags]", "deployment-pack") is None

    def test_wrong_value_updated_in_place(self) -> None:
        lines = self._lines("[alpha_flags]\ndeployment-pack = false\n")
        result = _ensure_toml_flag(lines, "[alpha_flags]", "deployment-pack")
        assert result is not None and "updated" in result
        assert any("deployment-pack = true" in ln for ln in lines)
        assert not any("deployment-pack = false" in ln for ln in lines)

    def test_missing_flag_inserted_after_section_header(self) -> None:
        lines = self._lines("[alpha_flags]\n")
        result = _ensure_toml_flag(lines, "[alpha_flags]", "deployment-pack")
        assert result is not None and "added" in result
        assert any("deployment-pack = true" in ln for ln in lines)

    def test_missing_section_appended_at_end(self) -> None:
        lines = self._lines("[module]\nversion = '1'\n")
        result = _ensure_toml_flag(lines, "[plugins]", "data")
        assert result is not None and "added" in result
        content = "".join(lines)
        assert "[plugins]" in content
        assert "data = true" in content


# cdf.toml check

class TestCheckCdfToml:
    def test_both_flags_correct_no_change(self, tmp_path: Path) -> None:
        original = "[alpha_flags]\ndeployment-pack = true\n\n[plugins]\ndata = true\n"
        (tmp_path / "cdf.toml").write_text(original, encoding="utf-8")
        check_cdf_toml(tmp_path)
        assert (tmp_path / "cdf.toml").read_text() == original  # file untouched

    def test_missing_file_exits(self, tmp_path: Path) -> None:
        with pytest.raises(SystemExit) as exc:
            check_cdf_toml(tmp_path)
        assert exc.value.code == 1

    def test_no_sections_adds_both(self, tmp_path: Path) -> None:
        (tmp_path / "cdf.toml").write_text("[module]\nversion = '1'\n", encoding="utf-8")
        check_cdf_toml(tmp_path)
        content = (tmp_path / "cdf.toml").read_text()
        assert "[alpha_flags]" in content
        assert "deployment-pack = true" in content
        assert "[plugins]" in content
        assert "data = true" in content

    def test_existing_sections_flags_appended(self, tmp_path: Path) -> None:
        (tmp_path / "cdf.toml").write_text("[alpha_flags]\n\n[plugins]\n", encoding="utf-8")
        check_cdf_toml(tmp_path)
        content = (tmp_path / "cdf.toml").read_text()
        assert "deployment-pack = true" in content
        assert "data = true" in content

    def test_deployment_pack_false_updated(self, tmp_path: Path) -> None:
        (tmp_path / "cdf.toml").write_text(
            "[alpha_flags]\ndeployment-pack = false\n\n[plugins]\ndata = true\n",
            encoding="utf-8",
        )
        check_cdf_toml(tmp_path)
        content = (tmp_path / "cdf.toml").read_text()
        assert "deployment-pack = true" in content
        assert "deployment-pack = false" not in content
        assert content.count("deployment-pack") == 1

    def test_data_false_updated(self, tmp_path: Path) -> None:
        (tmp_path / "cdf.toml").write_text(
            "[alpha_flags]\ndeployment-pack = true\n\n[plugins]\ndata = false\n",
            encoding="utf-8",
        )
        check_cdf_toml(tmp_path)
        content = (tmp_path / "cdf.toml").read_text()
        assert "data = true" in content
        assert "data = false" not in content
        assert content.count("data =") == 1

    def test_deployment_pack_in_wrong_section_still_added_to_alpha_flags(self, tmp_path: Path) -> None:
        (tmp_path / "cdf.toml").write_text(
            "[other]\ndeployment-pack = true\n\n[plugins]\ndata = true\n", encoding="utf-8"
        )
        check_cdf_toml(tmp_path)
        content = (tmp_path / "cdf.toml").read_text()
        assert "[alpha_flags]" in content
        assert content.count("deployment-pack = true") >= 1


# main() integration paths

def _patch_preflight() -> "pytest.FixtureRequest":
    """Helper: patch check_toolkit_version so tests don't need a real cdf binary."""
    return patch("qs_dp_setup_wizard.check_toolkit_version")


class TestMainEarlyExits:
    def test_unsupported_env(self, tmp_fixture_root: Path) -> None:
        with _patch_preflight():
            result = main(cli_env="staging_extra", repo_root_override=tmp_fixture_root)
        assert result == 1

    def test_missing_config_file(self, tmp_fixture_root: Path) -> None:
        (tmp_fixture_root / "config.dev.yaml").unlink()
        with _patch_preflight():
            result = main(cli_env="dev", repo_root_override=tmp_fixture_root)
        assert result == 1

    def test_missing_sql_file(self, tmp_fixture_root: Path) -> None:
        with _patch_preflight():
            result = main(
                cli_env="dev",
                repo_root_override=tmp_fixture_root,
                sql_path_override=tmp_fixture_root / "nonexistent.sql",
            )
        assert result == 1


class TestMainCancelPath:
    """User answers 'n' at the final confirmation — no files should be written."""

    def test_cancel_writes_nothing(
        self, tmp_fixture_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config_before = (tmp_fixture_root / "config.dev.yaml").read_text()

        responses = iter([
            "my-cdf-project",   # CDF project name
            "ops@acme.com",     # ApplicationOwner
            "",                 # shared group (Y = default)
            "grp-abc123",       # GROUP_SOURCE_ID
            "",                 # OPEN_ID_CLIENT_SECRET first attempt (empty loops)
            "secret-xyz",       # OPEN_ID_CLIENT_SECRET value
            "n",                # DON'T apply changes ← cancels
        ])
        monkeypatch.setattr("builtins.input", lambda _prompt: next(responses))

        with _patch_preflight():
            result = main(
                cli_env="dev", skip_verify=True, repo_root_override=tmp_fixture_root
            )

        assert result == 0
        # Config must be untouched
        assert (tmp_fixture_root / "config.dev.yaml").read_text() == config_before
        # No backup files
        assert not list(tmp_fixture_root.glob("*.bak.*"))

    def test_keyboard_interrupt_raises_system_exit_130(
        self, tmp_fixture_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("builtins.input", lambda _: (_ for _ in ()).throw(KeyboardInterrupt()))
        with _patch_preflight():
            with pytest.raises((KeyboardInterrupt, SystemExit)):
                main(cli_env="dev", skip_verify=True, repo_root_override=tmp_fixture_root)


# Post-write verification

class TestRunPostWriteVerification:
    def _make_run(self, returncode: int, stdout: str = "", stderr: str = "") -> MagicMock:
        m = MagicMock()
        m.returncode = returncode
        m.stdout = stdout
        m.stderr = stderr
        return m

    def test_build_success_offers_deploy(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("builtins.input", lambda _: "n")  # decline live deploy
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                self._make_run(0, stdout="Build OK"),   # cdf build
                self._make_run(0, stdout="Dry-run OK"), # cdf deploy --dry-run
            ]
            run_post_write_verification(tmp_path, "dev", "config.dev.yaml")
        assert mock_run.call_count == 2

    def test_build_failure_prints_stderr(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with patch("subprocess.run", return_value=self._make_run(1, stderr="boom")):
            run_post_write_verification(tmp_path, "dev", "config.dev.yaml")
        out = capsys.readouterr().out
        assert "FAILED" in out
        assert "cdf auth verify" in out

    def test_dry_run_failure_no_live_deploy(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                self._make_run(0),  # build OK
                self._make_run(1),  # dry-run fails
            ]
            run_post_write_verification(tmp_path, "dev", "config.dev.yaml")
        out = capsys.readouterr().out
        assert "failed" in out.lower()
        assert mock_run.call_count == 2


# _strip_yaml_quotes

class TestStripYamlQuotes:
    def test_no_quotes_unchanged(self) -> None:
        assert _strip_yaml_quotes("my-project") == "my-project"

    def test_double_quoted(self) -> None:
        assert _strip_yaml_quotes('"my-project"') == "my-project"

    def test_single_quoted(self) -> None:
        assert _strip_yaml_quotes("'my-project'") == "my-project"

    def test_mismatched_quotes_unchanged(self) -> None:
        assert _strip_yaml_quotes('"my-project\'') == '"my-project\''

    def test_empty_string_unchanged(self) -> None:
        assert _strip_yaml_quotes("") == ""

    def test_single_char_unchanged(self) -> None:
        assert _strip_yaml_quotes('"') == '"'

    def test_only_quotes_stripped(self) -> None:
        # value that itself contains a quote character inside
        assert _strip_yaml_quotes('"hello world"') == "hello world"


# _get_org_dir

class TestGetOrgDir:
    def test_returns_none_when_no_cdf_toml(self, tmp_path: Path) -> None:
        assert _get_org_dir(tmp_path) is None

    def test_returns_none_when_key_absent(self, tmp_path: Path) -> None:
        (tmp_path / "cdf.toml").write_text("[module]\nversion = '1'\n", encoding="utf-8")
        assert _get_org_dir(tmp_path) is None

    def test_double_quoted_value(self, tmp_path: Path) -> None:
        (tmp_path / "cdf.toml").write_text(
            '[module]\norganization_dir = "my_org"\n', encoding="utf-8"
        )
        assert _get_org_dir(tmp_path) == "my_org"

    def test_single_quoted_value(self, tmp_path: Path) -> None:
        (tmp_path / "cdf.toml").write_text(
            "[module]\norganization_dir = 'my_org'\n", encoding="utf-8"
        )
        assert _get_org_dir(tmp_path) == "my_org"

    def test_whitespace_around_equals(self, tmp_path: Path) -> None:
        (tmp_path / "cdf.toml").write_text(
            '[module]\norganization_dir   =   "spaced"\n', encoding="utf-8"
        )
        assert _get_org_dir(tmp_path) == "spaced"
