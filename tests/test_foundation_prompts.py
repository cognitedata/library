"""Tests for the Foundation setup wizard's prompt helpers (_prompts.py).

Covers _prompts.py's use of the ``t()`` message catalogue: english-mode output
stays unchanged, and japanese-mode output uses the reviewed translations
(including placeholder substitution and untranslated y/n hints).
"""

import sys
from collections.abc import Callable
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "modules" / "common" / "cdf_project_foundation" / "scripts"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import _i18n  # pyright: ignore[reportMissingImports]
import _prompts  # pyright: ignore[reportMissingImports]


def _echo_input(*answers: str) -> Callable[[str], str]:
    """Fake ``input()`` that echoes its prompt to stdout, like the real builtin does."""
    it = iter(answers)

    def _fake(prompt: str = "") -> str:
        print(prompt, end="")
        return next(it)

    return _fake


class TestPromptYesNo:
    def test_default_yes_hint_is_english_by_default(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr("builtins.input", _echo_input(""))
        assert _prompts.prompt_yes_no("Proceed?", default=True) is True
        assert "[Y/n]" in capsys.readouterr().out

    def test_default_no_hint_is_english_by_default(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr("builtins.input", _echo_input(""))
        assert _prompts.prompt_yes_no("Proceed?", default=False) is False
        assert "[y/N]" in capsys.readouterr().out

    def test_yes_no_hints_stay_english_in_japanese_locale(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # [Y/n] / [y/N] are intentionally excluded from the catalogue — do not translate.
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        monkeypatch.setattr("builtins.input", _echo_input(""))
        _prompts.prompt_yes_no("Proceed?", default=True)
        assert "[Y/n]" in capsys.readouterr().out


class TestPromptChoice:
    def test_accepts_valid_choice(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("builtins.input", _echo_input("2"))
        assert _prompts.prompt_choice(["a", "b", "c"], default=1) == 2

    def test_invalid_choice_warns_and_retries_in_english(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr("builtins.input", _echo_input("abc", "1"))
        assert _prompts.prompt_choice(["a", "b"], default=1) == 1
        assert "Please enter a number between 1 and 2." in capsys.readouterr().out

    def test_invalid_choice_warns_in_japanese(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        monkeypatch.setattr("builtins.input", _echo_input("abc", "1"))
        _prompts.prompt_choice(["a", "b"], default=1)
        assert "1 から 2 の間の数字を入力してください。" in capsys.readouterr().out


class TestPromptEnvVar:
    def test_existing_value_kept_prints_english_found_message(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr("builtins.input", _echo_input("y"))
        env_vals = {"FOO": "secretvalue123"}
        _prompts.prompt_env_var("FOO", env_vals, [], {})
        out = capsys.readouterr().out
        assert "Found FOO in .env  (current: sec****)" in out
        assert env_vals["FOO"] == "secretvalue123"

    def test_existing_value_replaced_when_user_declines_to_keep(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("builtins.input", _echo_input("n", "newval"))
        env_vals = {"FOO": "secretvalue123"}
        _prompts.prompt_env_var("FOO", env_vals, [], {})
        assert env_vals["FOO"] == "newval"

    def test_missing_value_warns_and_creates_when_provided(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr("builtins.input", _echo_input("created-value"))
        env_vals: dict[str, str] = {}
        _prompts.prompt_env_var("BAR", env_vals, [], {})
        assert "BAR not in .env — will be created." in capsys.readouterr().out
        assert env_vals["BAR"] == "created-value"

    def test_missing_value_left_blank_skips_creation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("builtins.input", _echo_input(""))
        env_vals: dict[str, str] = {}
        _prompts.prompt_env_var("BAR", env_vals, [], {})
        assert "BAR" not in env_vals

    def test_found_and_keep_messages_in_japanese_locale(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        monkeypatch.setattr("builtins.input", _echo_input("y"))
        env_vals = {"FOO": "secretvalue123"}
        _prompts.prompt_env_var("FOO", env_vals, [], {})
        out = capsys.readouterr().out
        assert ".env に FOO が見つかりました（現在の値: sec****）" in out
        assert "既存の FOO を保持しますか？" in out

    def test_not_found_and_new_value_messages_in_japanese_locale(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        monkeypatch.setattr("builtins.input", _echo_input("created-value"))
        env_vals: dict[str, str] = {}
        _prompts.prompt_env_var("BAR", env_vals, [], {})
        out = capsys.readouterr().out
        assert "BAR は .env に存在しません — 新規作成されます。" in out
        assert "BAR の値を入力（空白でスキップ）" in out
        assert env_vals["BAR"] == "created-value"
