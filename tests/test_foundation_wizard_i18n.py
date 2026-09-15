"""Tests for setup_project.py's use of the ``t()`` message catalogue.

Covers the interactive prompt/summary functions most at risk of getting the
placeholder substitution wrong (attribute/index access, label composition,
--check mode messages): english-mode output stays unchanged, and japanese-mode
output uses the reviewed translations.
"""

import sys
from collections.abc import Callable
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_ROOT = REPO_ROOT / "modules" / "common" / "cdf_project_foundation"
SCRIPTS_DIR = MODULE_ROOT / "scripts"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import _i18n  # pyright: ignore[reportMissingImports]
import setup_project as sp  # pyright: ignore[reportMissingImports]


def _echo_input(*answers: str) -> Callable[[str], str]:
    """Fake ``input()`` that echoes its prompt to stdout, like the real builtin does."""
    it = iter(answers)

    def _fake(prompt: str = "") -> str:
        print(prompt, end="")
        return next(it)

    return _fake


class TestShowWizardReview:
    """Regression test: {project_names[env]} uses str.format() index syntax, which
    treats "env" as a literal key, not the loop variable — a real bug caught while
    migrating this call site."""

    def test_review_line_substitutes_the_correct_project_name_in_english(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        targets = {"dev": tmp_path / "config.dev.yaml"}
        sp._show_wizard_review(targets, {"dev": "acme-dev"}, env_dirty=False)
        out = capsys.readouterr().out
        assert "[create] config.dev.yaml  —  project: acme-dev" in out

    def test_review_line_substitutes_the_correct_project_name_in_japanese(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        targets = {"dev": tmp_path / "config.dev.yaml"}
        sp._show_wizard_review(targets, {"dev": "acme-dev"}, env_dirty=False)
        out = capsys.readouterr().out
        assert "[create] config.dev.yaml  —  プロジェクト: acme-dev" in out

    def test_env_dirty_line_in_japanese(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        sp._show_wizard_review({}, {}, env_dirty=True)
        assert "アクセスグループ Source ID を更新しました。" in capsys.readouterr().out


class TestPromptEnvironments:
    def test_continue_with_current_selection_in_english(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        (tmp_path / "config.dev.yaml").write_text("environment:\n  name: dev\n")
        monkeypatch.setattr("builtins.input", _echo_input("y"))
        result = sp._prompt_environments(tmp_path)
        assert result == ("dev",)
        assert "Continue with current selection (dev)?" in capsys.readouterr().out

    def test_continue_with_current_selection_in_japanese(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        (tmp_path / "config.dev.yaml").write_text("environment:\n  name: dev\n")
        monkeypatch.setattr("builtins.input", _echo_input("y"))
        result = sp._prompt_environments(tmp_path)
        assert result == ("dev",)
        assert "現在の選択（dev）で続行しますか？" in capsys.readouterr().out

    def test_custom_selection_prompts_include_env_name_in_japanese(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        (tmp_path / "config.dev.yaml").write_text("environment:\n  name: dev\n")
        # Decline "continue with current selection", then answer per-env include prompts.
        monkeypatch.setattr("builtins.input", _echo_input("n", "y", "n", "n"))
        result = sp._prompt_environments(tmp_path)
        assert result == ("dev",)
        out = capsys.readouterr().out
        assert "'dev' を含めますか？" in out
        assert "'test' を含めますか？" in out
        assert "'prod' を含めますか？" in out

    def test_menu_selection_all_three_in_english(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("builtins.input", _echo_input("1"))
        assert sp._prompt_environments(tmp_path) == ("dev", "test", "prod")

    def test_menu_custom_branch_prompts_include_environment_name_in_japanese(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        monkeypatch.setattr("builtins.input", _echo_input("4", "y", "n", "n"))
        result = sp._prompt_environments(tmp_path)
        assert result == ("dev",)
        out = capsys.readouterr().out
        assert "環境 'dev' を含めますか？" in out
        assert "環境 'test' を含めますか？" in out
        assert "環境 'prod' を含めますか？" in out


class TestPromptOwnerLabelComposition:
    def test_labels_are_english_by_default(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr("builtins.input", _echo_input("Jane Doe", ""))
        sp._prompt_owner("  Integration owner")
        out = capsys.readouterr().out
        assert "Integration owner name" in out
        assert "Integration owner email" in out

    def test_labels_are_translated_in_japanese(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        label = f"  {_i18n.t('Integration owner')}"
        monkeypatch.setattr("builtins.input", _echo_input("Jane Doe", ""))
        sp._prompt_owner(label)
        out = capsys.readouterr().out
        assert "インテグレーション管理者 名前" in out
        assert "インテグレーション管理者 メールアドレス" in out

    def test_invalid_email_warning_in_japanese(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        monkeypatch.setattr("builtins.input", _echo_input("Jane Doe", "not-an-email", ""))
        sp._prompt_owner("  Data owner")
        assert "メールアドレスが無効です。形式: name@domain.com" in capsys.readouterr().out


class TestPromptSiteAndProjectNames:
    def test_site_validation_error_in_japanese(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        monkeypatch.setattr("builtins.input", _echo_input("Invalid Site!", "oslo"))
        assert sp._prompt_site("") == "oslo"
        assert "小文字の英字、数字、ハイフン、アンダースコアのみを使用してください。" in capsys.readouterr().out

    def test_project_name_validation_error_in_japanese(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        monkeypatch.setattr("builtins.input", _echo_input("Not Valid!", "acme-dev"))
        result = sp._prompt_project_names(("dev",), {})
        assert result == {"dev": "acme-dev"}
        assert "小文字の英字、数字、ハイフンのみを使用してください（例: acme-dev）。" in capsys.readouterr().out


class TestRunCheck:
    def _scaffold(self, tmp_path: Path) -> None:
        space_rel = sp._CDM_INSTANCE_SPACE_REL_PATH
        (tmp_path / space_rel).parent.mkdir(parents=True)
        (tmp_path / space_rel).write_text("space: x\n")
        (tmp_path / "modules" / "sourcesystem" / "cdf_pi_extractor").mkdir(parents=True)

    def test_ok_message_in_english(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        self._scaffold(tmp_path)
        sp._run_check(None, repo_root=tmp_path)
        assert "OK: All config file(s) match variant 'cdm'. No stale auth files." in capsys.readouterr().out

    def test_ok_message_in_japanese_with_same_behavior(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        self._scaffold(tmp_path)
        sp._run_check(None, repo_root=tmp_path)  # must not raise SystemExit
        out = capsys.readouterr().out
        assert "OK: すべての設定ファイルがデータモデル 'cdm' と一致しています。不要な認証ファイルはありません。" in out

    def test_out_of_sync_error_exits_1_and_is_translated_in_japanese(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        self._scaffold(tmp_path)
        (tmp_path / "config.dev.yaml").write_text(
            "environment:\n  name: dev\n  project: acme-dev\nvariables:\n  modules: {}\n"
        )
        with pytest.raises(SystemExit) as exc_info:
            sp._run_check(None, repo_root=tmp_path)
        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert "エラー: 設定ファイルがデータモデル 'cdm' と一致していません:" in out
        assert "実行: python scripts/setup_project.py -y" in out
