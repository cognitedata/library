"""Tests for generate_actions.py's use of the ``t()`` message catalogue.

Covers write_file/remove_file's user-facing messages and the GitHub "Next steps"
checklist: english-mode output stays unchanged, and japanese-mode output uses the
reviewed translations. The Azure DevOps checklist is intentionally excluded — see
UNMIGRATED_STRINGS.md.
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_ROOT = REPO_ROOT / "modules" / "common" / "cdf_project_foundation"
SCRIPTS_DIR = MODULE_ROOT / "scripts"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import _i18n  # pyright: ignore[reportMissingImports]
import generate_actions  # pyright: ignore[reportMissingImports]


def _scaffold_dev_only_project(project_dir: Path) -> None:
    (project_dir / "cdf.toml").write_text(
        """
[modules]
version = "0.8.0"
""".strip(),
        encoding="utf-8",
    )
    modules = project_dir / "modules" / "common" / "cdf_project_foundation"
    modules.mkdir(parents=True)
    (modules / "module.toml").write_text(
        'id = "cdf_project_foundation"\npackage_id = "dp:foundation"\n',
        encoding="utf-8",
    )
    (project_dir / "config.dev.yaml").write_text(
        """
environment:
  name: dev
  project: acme-dev
""".lstrip(),
        encoding="utf-8",
    )


class TestWriteFile:
    def test_writes_new_file_and_prints_english_message(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        target = tmp_path / "out.txt"
        generate_actions.write_file(target, "content", force=False)
        assert target.read_text() == "content"
        assert f"Wrote {target}" in capsys.readouterr().out

    def test_writes_new_file_and_prints_japanese_message(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        target = tmp_path / "out.txt"
        generate_actions.write_file(target, "content", force=False)
        assert f"書き込みました: {target}" in capsys.readouterr().out

    def test_skip_message_when_overwrite_declined(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        target = tmp_path / "out.txt"
        target.write_text("old")
        monkeypatch.setattr("builtins.input", lambda _: "n")
        generate_actions.write_file(target, "new", force=False)
        assert target.read_text() == "old"
        assert f"Skipped {target}" in capsys.readouterr().out

    def test_skip_message_in_japanese_when_overwrite_declined(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        target = tmp_path / "out.txt"
        target.write_text("old")
        monkeypatch.setattr("builtins.input", lambda _: "n")
        generate_actions.write_file(target, "new", force=False)
        assert f"スキップしました: {target}" in capsys.readouterr().out

    def test_overwrite_prompt_echoes_japanese_text(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        target = tmp_path / "out.txt"
        target.write_text("old")

        def _fake_input(prompt: str = "") -> str:
            print(prompt, end="")
            return "y"

        monkeypatch.setattr("builtins.input", _fake_input)
        generate_actions.write_file(target, "new", force=False)
        out = capsys.readouterr().out
        assert f"{target} は既に存在します。上書きしますか？ [y/N]" in out
        assert target.read_text() == "new"


class TestRemoveFile:
    def test_prints_english_message(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        target = tmp_path / "gone.txt"
        target.write_text("x")
        generate_actions.remove_file(target)
        assert not target.exists()
        assert f"Removed {target}" in capsys.readouterr().out

    def test_prints_japanese_message(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        target = tmp_path / "gone.txt"
        target.write_text("x")
        generate_actions.remove_file(target)
        assert f"削除しました: {target}" in capsys.readouterr().out


class TestNextStepsChecklist:
    def test_github_checklist_is_english_by_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _scaffold_dev_only_project(tmp_path)
        monkeypatch.setattr(sys, "argv", ["generate_actions.py", "--force"])
        monkeypatch.chdir(tmp_path)

        generate_actions.main()

        out = capsys.readouterr().out
        assert "Next steps:" in out
        assert "1. Create GitHub Environments: dev-toolkit-credentials" in out
        assert "(see docs/FOUNDATION_CICD.md)" in out
        assert "2. Create and protect the branches used by the generated workflows" in out
        assert "3. Open a PR to dev to validate dry-run.yml" in out

    def test_github_checklist_is_japanese_when_locale_is_ja(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        _scaffold_dev_only_project(tmp_path)
        monkeypatch.setattr(sys, "argv", ["generate_actions.py", "--force"])
        monkeypatch.chdir(tmp_path)

        generate_actions.main()

        out = capsys.readouterr().out
        assert "次の手順:" in out
        assert "1. GitHub Environments を作成してください: dev-toolkit-credentials" in out
        assert "（docs/FOUNDATION_CICD.md を参照）" in out
        assert "2. 生成されたワークフローで使用されるブランチを作成し、ブランチ保護を設定してください。" in out
        assert "3. dry-run.yml を検証するため、dev への PR を作成してください" in out
