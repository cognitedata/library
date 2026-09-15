"""Locale-switch coverage for the wizard suite.

A catalogue change (translation edit, dropped placeholder, removed key) should
fail here without needing a dedicated new test per string. Covers:
  - the catalogue loads cleanly and every entry's placeholders match between the
    English key and its Japanese translation (catches e.g. a translator dropping
    a ``{var}``, which would silently hide a real value in Japanese mode);
  - every entry's placeholders can be filled without KeyError/IndexError;
  - a key missing from ``messages_ja`` falls back to the English key rather than
    failing;
  - a representative slice of the wizard's own call sites runs without error
    under both supported locales.
"""

import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "modules" / "common" / "cdf_project_foundation" / "scripts"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import _i18n  # pyright: ignore[reportMissingImports]
import setup_project as sp  # pyright: ignore[reportMissingImports]
from _messages_ja import messages_ja  # pyright: ignore[reportMissingImports]

_PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)")


def _placeholders(template: str) -> set[str]:
    """Top-level ``{name}`` placeholders in *template*.

    Ignores literal ``{{escaped}}`` braces (e.g. the ``{{search_space}}`` Toolkit
    template token) and attribute/index suffixes — ``{path.name}`` yields ``path``,
    ``{project_names[env]}`` yields ``project_names``.
    """
    stripped = template.replace("{{", "").replace("}}", "")
    return set(_PLACEHOLDER_RE.findall(stripped))


class _AnyAttr:
    """Stub that resolves any attribute/item/str access, so a template's
    placeholders can be filled without needing a real Path/dict for every
    possible call-site usage — this test only cares whether the *name* resolves,
    not the call site's own substitution logic (covered elsewhere per-call-site)."""

    def __getattr__(self, _name: str) -> "_AnyAttr":
        return self

    def __getitem__(self, _key: object) -> "_AnyAttr":
        return self

    def __format__(self, _spec: str) -> str:
        return "x"

    def __str__(self) -> str:
        return "x"


class TestCatalogueLoadsCleanly:
    def test_catalogue_is_non_empty_and_reloads_without_error(self) -> None:
        import importlib

        import _messages_ja as messages_ja_module

        importlib.reload(messages_ja_module)
        assert len(messages_ja_module.messages_ja) > 100

    def test_every_entry_is_a_non_empty_string_pair(self) -> None:
        for key, value in messages_ja.items():
            assert isinstance(key, str) and key
            assert isinstance(value, str) and value


class TestCataloguePlaceholderParity:
    def test_every_entry_has_matching_placeholders_between_english_and_japanese(self) -> None:
        mismatches = {
            key: (_placeholders(key), _placeholders(value))
            for key, value in messages_ja.items()
            if _placeholders(key) != _placeholders(value)
        }
        assert not mismatches, mismatches

    def test_every_entry_fills_without_keyerror_or_indexerror(self) -> None:
        for key, value in messages_ja.items():
            names = _placeholders(key) | _placeholders(value)
            if not names:
                continue
            dummy = {name: _AnyAttr() for name in names}
            key.format(**dummy)
            value.format(**dummy)

    @pytest.mark.parametrize("active_locale", ["en", "ja"])
    def test_t_resolves_every_catalogue_key_in_both_locales(
        self, active_locale: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", active_locale)
        for key in messages_ja:
            result = _i18n.t(key)
            assert isinstance(result, str) and result


class TestMissingKeyFallsBackToEnglish:
    def test_a_real_key_removed_from_the_catalogue_falls_back_to_english(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        patched = dict(messages_ja)
        del patched["Review"]
        monkeypatch.setattr(_i18n, "messages_ja", patched)
        assert _i18n.t("Review") == "Review"


@pytest.mark.parametrize("active_locale", ["en", "ja"])
class TestWizardCoreFlowUnderBothLocales:
    def test_wizard_header_renders_without_error(
        self,
        active_locale: str,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", active_locale)
        sp._print_wizard_header("cdm", tmp_path, [], "foundation")
        out = capsys.readouterr().out
        assert "Foundation Deployment Pack" in out  # product name — never translated
        assert ("プロジェクトセットアップ" in out) == (active_locale == "ja")
        assert ("Project Setup" in out) == (active_locale == "en")

    def test_environment_menu_renders_without_error(
        self, active_locale: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", active_locale)
        monkeypatch.setattr("builtins.input", lambda _: "1")
        assert sp._prompt_environments(tmp_path) == ("dev", "test", "prod")

    def test_run_check_ok_path_is_always_english_regardless_of_locale(
        self,
        active_locale: str,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", active_locale)
        space_rel = sp._CDM_INSTANCE_SPACE_REL_PATH
        (tmp_path / space_rel).parent.mkdir(parents=True)
        (tmp_path / space_rel).write_text("space: x\n")
        (tmp_path / "modules" / "sourcesystem" / "cdf_pi_extractor").mkdir(parents=True)
        sp._run_check(None, repo_root=tmp_path)
        assert "OK: All config file(s) match variant 'cdm'. No stale auth files." in capsys.readouterr().out
