"""Tests for the Foundation setup wizard's message catalogue (_i18n, _messages_ja)."""

import locale
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "modules" / "common" / "cdf_project_foundation" / "scripts"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import _i18n  # pyright: ignore[reportMissingImports]
from _messages_ja import messages_ja  # pyright: ignore[reportMissingImports]


class TestMessagesJaCatalogue:
    def test_catalogue_is_non_empty(self) -> None:
        assert len(messages_ja) > 100

    def test_catalogue_has_no_duplicate_or_empty_values(self) -> None:
        for key, value in messages_ja.items():
            assert key != ""
            assert value != ""

    def test_product_names_are_not_in_the_catalogue(self) -> None:
        # These stay English everywhere and are excluded, not identity-mapped.
        for excluded in ("Foundation Deployment Pack", "PI Extractor", "SAP Extractor"):
            assert excluded not in messages_ja

    def test_unchanged_literals_are_not_in_the_catalogue(self) -> None:
        assert "[Y/n]" not in messages_ja
        assert "[y/N]" not in messages_ja

    def test_known_key_maps_to_reviewed_translation(self) -> None:
        assert messages_ja["Review"] == "確認"
        assert messages_ja["Environment Selection"] == "環境の選択"


class TestT:
    def test_returns_key_verbatim_in_default_english_locale(self) -> None:
        assert _i18n.t("Review") == "Review"

    def test_unknown_key_returns_key_verbatim_without_raising(self) -> None:
        assert _i18n.t("Some string not in any catalogue") == "Some string not in any catalogue"

    def test_empty_key_returns_empty_string(self) -> None:
        assert _i18n.t("") == ""

    def test_returns_translation_when_locale_is_japanese(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        assert _i18n.t("Review") == "確認"

    def test_unknown_key_falls_back_to_english_in_japanese_locale(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        assert _i18n.t("Some string not in any catalogue") == "Some string not in any catalogue"


def _clear_locale_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("CDF_LOCALE", "LC_ALL", "LC_MESSAGES", "LANG"):
        monkeypatch.delenv(var, raising=False)


class TestParseLocaleCode:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("ja", "ja"),
            ("en", "en"),
            ("ja_JP.UTF-8", "ja"),
            ("en_US.UTF-8", "en"),
            ("en-US", "en"),
            ("JA", "ja"),
            ("  ja  ", "ja"),
            ("Japanese_Japan", "ja"),
            ("English_United States", "en"),
        ],
    )
    def test_recognizes_supported_locale_forms(self, value: str, expected: str) -> None:
        assert _i18n._parse_locale_code(value) == expected

    @pytest.mark.parametrize("value", [None, "", "C", "POSIX", "c", "fr_FR.UTF-8", "de"])
    def test_returns_none_for_unsupported_or_empty_values(self, value: str | None) -> None:
        assert _i18n._parse_locale_code(value) is None


class TestResolveLocale:
    def test_cdf_locale_takes_priority_over_everything(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setenv("CDF_LOCALE", "ja")
        monkeypatch.setenv("LC_ALL", "en_US.UTF-8")
        assert _i18n.resolve_locale() == "ja"

    def test_falls_back_to_lc_all_when_cdf_locale_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setenv("LC_ALL", "ja_JP.UTF-8")
        assert _i18n.resolve_locale() == "ja"

    def test_lc_all_takes_priority_over_lc_messages_and_lang(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setenv("LC_ALL", "ja_JP.UTF-8")
        monkeypatch.setenv("LC_MESSAGES", "en_US.UTF-8")
        monkeypatch.setenv("LANG", "en_US.UTF-8")
        assert _i18n.resolve_locale() == "ja"

    def test_lc_messages_takes_priority_over_lang(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setenv("LC_MESSAGES", "ja_JP.UTF-8")
        monkeypatch.setenv("LANG", "en_US.UTF-8")
        assert _i18n.resolve_locale() == "ja"

    def test_falls_back_to_lang_when_lc_vars_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setenv("LANG", "ja_JP.UTF-8")
        assert _i18n.resolve_locale() == "ja"

    def test_defaults_to_english_when_nothing_is_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        assert _i18n.resolve_locale() == "en"

    def test_unsupported_locale_falls_back_to_english_silently(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setenv("LANG", "fr_FR.UTF-8")
        assert _i18n.resolve_locale() == "en"

    def test_c_and_posix_locales_are_ignored_like_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setenv("LANG", "C")
        assert _i18n.resolve_locale() == "en"

    def test_windows_uses_locale_getlocale(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "win32")
        monkeypatch.setattr(locale, "getlocale", lambda: ("Japanese_Japan", "932"))
        assert _i18n.resolve_locale() == "ja"

    def test_windows_falls_back_to_english_when_getlocale_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "win32")

        def _raise() -> tuple[str | None, str | None]:
            raise ValueError("unknown locale")

        monkeypatch.setattr(locale, "getlocale", _raise)
        assert _i18n.resolve_locale() == "en"

    def test_windows_ignores_unix_env_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setenv("LANG", "ja_JP.UTF-8")
        monkeypatch.setattr(sys, "platform", "win32")
        monkeypatch.setattr(locale, "getlocale", lambda: (None, None))
        assert _i18n.resolve_locale() == "en"


class TestLocaleOverride:
    def test_forces_locale_within_the_block(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "en")
        with _i18n.locale_override("ja"):
            assert _i18n._LOCALE == "ja"
            assert _i18n.t("Review") == "確認"
        assert _i18n._LOCALE == "en"

    def test_restores_previous_locale_after_an_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_i18n, "_LOCALE", "ja")
        with pytest.raises(RuntimeError), _i18n.locale_override("en"):
            assert _i18n._LOCALE == "en"
            raise RuntimeError("boom")
        assert _i18n._LOCALE == "ja"
