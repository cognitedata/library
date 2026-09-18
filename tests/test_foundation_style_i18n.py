"""Tests for CJK-aware terminal display-width handling in _style.py / _prompts.py.

Covers _display_width(), _banner()'s box sizing, _section()'s header rendering,
and prompt_choice()'s numbered-menu alignment — each exercised with an ASCII-only,
a Japanese-only, and a mixed EN/JA string, using real strings from
``japanese_localization_strings.csv`` / ``_messages_ja.py`` where practical.
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "modules" / "common" / "cdf_project_foundation" / "scripts"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from _prompts import prompt_choice  # pyright: ignore[reportMissingImports]
from _style import _banner, _display_width, _section  # pyright: ignore[reportMissingImports]


class TestDisplayWidth:
    def test_ascii_string_width_equals_len(self) -> None:
        assert _display_width("Environment Selection") == len("Environment Selection")

    def test_japanese_string_width_is_double_len(self) -> None:
        # 環境の選択 — 5 wide characters, each counts as 2 display columns.
        assert _display_width("環境の選択") == 10
        assert len("環境の選択") == 5

    def test_mixed_string_counts_wide_and_narrow_separately(self) -> None:
        # "AB" (2 narrow) + "環境" (2 wide) = 2 + 4 = 6 columns, 4 characters.
        assert _display_width("AB環境") == 6
        assert len("AB環境") == 4

    def test_empty_string_is_zero(self) -> None:
        assert _display_width("") == 0


class TestBannerWidth:
    def test_ascii_title_keeps_the_original_56_column_box(self, capsys: pytest.CaptureFixture[str]) -> None:
        _banner("Environment Selection")
        lines = capsys.readouterr().out.splitlines()
        assert lines[1] == "─" * 56
        assert lines[3] == "─" * 56
        assert "  Environment Selection" in lines[2]

    def test_short_japanese_title_does_not_shrink_the_box(self, capsys: pytest.CaptureFixture[str]) -> None:
        _banner("環境の選択")
        lines = capsys.readouterr().out.splitlines()
        assert lines[1] == "─" * 56
        assert lines[3] == "─" * 56
        assert "  環境の選択" in lines[2]

    def test_long_mixed_title_grows_the_box_to_fit(self, capsys: pytest.CaptureFixture[str]) -> None:
        # Real (reviewed) Demo-pack banner title — wider than the 56-column default
        # once the wide Japanese suffix is counted at 2 columns per character.
        title = "Foundation Deployment Pack Demo — プロジェクトセットアップ"
        _banner(title)
        lines = capsys.readouterr().out.splitlines()
        top, bottom = lines[1], lines[3]
        assert top == bottom
        assert len(top) > 56
        assert _display_width(top) >= _display_width(title) + 2
        assert f"  {title}" in lines[2]


class TestSectionRendering:
    def test_ascii_title(self, capsys: pytest.CaptureFixture[str]) -> None:
        _section("Environment Selection")
        assert "── Environment Selection ──" in capsys.readouterr().out

    def test_japanese_title(self, capsys: pytest.CaptureFixture[str]) -> None:
        _section("環境の選択")
        assert "── 環境の選択 ──" in capsys.readouterr().out

    def test_mixed_title(self, capsys: pytest.CaptureFixture[str]) -> None:
        _section("CFIHOS データモデル — データモデル管理者設定")
        assert "── CFIHOS データモデル — データモデル管理者設定 ──" in capsys.readouterr().out


class TestPromptChoiceAlignment:
    def test_ascii_options_all_start_in_the_same_column(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr("builtins.input", lambda _: "1")
        options = [
            "All three — dev, test, prod  (recommended)",
            "dev only",
            "dev + prod  (skip test / staging)",
            "Custom — choose individually",
        ]
        prompt_choice(options, default=1)
        lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
        prefixes = [line.split("]")[0] for line in lines]
        assert len({len(p) for p in prefixes}) == 1

    def test_japanese_options_all_start_in_the_same_column(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr("builtins.input", lambda _: "1")
        options = [
            "すべて — dev、test、prod（推奨）",
            "dev のみ",
            "dev + prod（test／staging をスキップ）",
            "カスタム — 個別に選択",
        ]
        prompt_choice(options, default=1)
        out = capsys.readouterr().out
        for opt in options:
            assert opt in out
        lines = [line for line in out.splitlines() if line.strip()]
        prefixes = [line.split("]")[0] for line in lines]
        assert len({len(p) for p in prefixes}) == 1

    def test_mixed_options_all_start_in_the_same_column(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr("builtins.input", lambda _: "1")
        options = [
            "Foundation プロジェクト（サンプルデータなし）",
            "サンプルデータ付きデモプロジェクト（変換とワークフローを再作成）",
        ]
        prompt_choice(options, default=1)
        out = capsys.readouterr().out
        for opt in options:
            assert opt in out
        lines = [line for line in out.splitlines() if line.strip()]
        prefixes = [line.split("]")[0] for line in lines]
        assert len({len(p) for p in prefixes}) == 1

    def test_index_column_widens_for_ten_or_more_options(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr("builtins.input", lambda _: "1")
        options = [f"option {i}" for i in range(1, 11)]
        prompt_choice(options, default=1)
        lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
        assert "[ 1]" in lines[0]
        assert "[10]" in lines[9]

    def test_ascii_only_menu_is_unchanged_single_digit_format(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr("builtins.input", lambda _: "1")
        prompt_choice(["a", "b", "c"], default=1)
        out = capsys.readouterr().out
        assert "[1]" in out
        assert "[2]" in out
        assert "[3]" in out
