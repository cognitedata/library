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
from _messages_ja import MESSAGES_JA  # pyright: ignore[reportMissingImports]


class TestMessagesJaCatalogue:
    def test_catalogue_is_non_empty(self) -> None:
        assert len(MESSAGES_JA) > 100

    def test_catalogue_has_no_duplicate_or_empty_values(self) -> None:
        for key, value in MESSAGES_JA.items():
            assert key != ""
            assert value != ""

    def test_product_names_are_not_in_the_catalogue(self) -> None:
        # These stay English everywhere and are excluded, not identity-mapped.
        for excluded in ("Foundation Deployment Pack", "PI Extractor", "SAP Extractor"):
            assert excluded not in MESSAGES_JA

    def test_unchanged_literals_are_not_in_the_catalogue(self) -> None:
        assert "[Y/n]" not in MESSAGES_JA
        assert "[y/N]" not in MESSAGES_JA

    def test_known_key_maps_to_reviewed_translation(self) -> None:
        assert MESSAGES_JA["Review"] == "確認"
        assert MESSAGES_JA["Environment Selection"] == "環境の選択"


class TestT:
    def test_returns_key_verbatim_in_default_english_locale(self) -> None:
        assert _i18n.t("Review") == "Review"

    def test_unknown_key_returns_key_verbatim_without_raising(self) -> None:
        assert _i18n.t("Some string not in any catalogue") == "Some string not in any catalogue"

    def test_empty_key_returns_empty_string(self) -> None:
        assert _i18n.t("") == ""

    def test_returns_translation_when_locale_is_japanese(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_i18n, "_locale", "ja")
        assert _i18n.t("Review") == "確認"

    def test_unknown_key_falls_back_to_english_in_japanese_locale(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_i18n, "_locale", "ja")
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
    """Env-var fallback path — exercised on ``sys.platform == "linux"`` so these
    stay deterministic regardless of the host OS running the suite (macOS/Windows
    have their own OS-native detection paths, tested separately below)."""

    def test_cdf_locale_takes_priority_over_everything(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setenv("CDF_LOCALE", "ja")
        monkeypatch.setenv("LC_ALL", "en_US.UTF-8")
        assert _i18n.resolve_locale() == "ja"

    def test_cdf_locale_takes_priority_over_macos_detection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "darwin")
        monkeypatch.setattr(_i18n, "_detect_macos_ui_language", lambda: "en")
        monkeypatch.setenv("CDF_LOCALE", "ja")
        assert _i18n.resolve_locale() == "ja"

    def test_cdf_locale_takes_priority_over_windows_detection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "win32")
        monkeypatch.setattr(_i18n, "_detect_windows_ui_language", lambda: "en")
        monkeypatch.setenv("CDF_LOCALE", "ja")
        assert _i18n.resolve_locale() == "ja"

    def test_falls_back_to_lc_all_when_cdf_locale_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setenv("LC_ALL", "ja_JP.UTF-8")
        assert _i18n.resolve_locale() == "ja"

    def test_lc_all_takes_priority_over_lc_messages_and_lang(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setenv("LC_ALL", "ja_JP.UTF-8")
        monkeypatch.setenv("LC_MESSAGES", "en_US.UTF-8")
        monkeypatch.setenv("LANG", "en_US.UTF-8")
        assert _i18n.resolve_locale() == "ja"

    def test_lc_messages_takes_priority_over_lang(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setenv("LC_MESSAGES", "ja_JP.UTF-8")
        monkeypatch.setenv("LANG", "en_US.UTF-8")
        assert _i18n.resolve_locale() == "ja"

    def test_falls_back_to_lang_when_lc_vars_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setenv("LANG", "ja_JP.UTF-8")
        assert _i18n.resolve_locale() == "ja"

    def test_defaults_to_english_when_nothing_is_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "linux")
        assert _i18n.resolve_locale() == "en"

    def test_unsupported_locale_falls_back_to_english_silently(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setenv("LANG", "fr_FR.UTF-8")
        assert _i18n.resolve_locale() == "en"

    def test_c_and_posix_locales_are_ignored_like_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setenv("LANG", "C")
        assert _i18n.resolve_locale() == "en"

    def test_windows_uses_ui_language_detection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "win32")
        monkeypatch.setattr(_i18n, "_detect_windows_ui_language", lambda: "ja")
        assert _i18n.resolve_locale() == "ja"

    def test_windows_falls_back_to_locale_getlocale_when_ui_detection_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "win32")
        monkeypatch.setattr(_i18n, "_detect_windows_ui_language", lambda: None)
        monkeypatch.setattr(locale, "getlocale", lambda: ("Japanese_Japan", "932"))
        assert _i18n.resolve_locale() == "ja"

    def test_windows_falls_back_to_english_when_everything_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "win32")
        monkeypatch.setattr(_i18n, "_detect_windows_ui_language", lambda: None)

        def _raise() -> tuple[str | None, str | None]:
            raise ValueError("unknown locale")

        monkeypatch.setattr(locale, "getlocale", _raise)
        assert _i18n.resolve_locale() == "en"

    def test_windows_ignores_unix_env_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setenv("LANG", "ja_JP.UTF-8")
        monkeypatch.setattr(sys, "platform", "win32")
        monkeypatch.setattr(_i18n, "_detect_windows_ui_language", lambda: None)
        monkeypatch.setattr(locale, "getlocale", lambda: (None, None))
        assert _i18n.resolve_locale() == "en"

    def test_macos_uses_ui_language_detection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "darwin")
        monkeypatch.setattr(_i18n, "_detect_macos_ui_language", lambda: "ja")
        monkeypatch.setenv("LANG", "en_US.UTF-8")
        assert _i18n.resolve_locale() == "ja"

    def test_macos_falls_back_to_env_vars_when_ui_detection_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "darwin")
        monkeypatch.setattr(_i18n, "_detect_macos_ui_language", lambda: None)
        monkeypatch.setenv("LANG", "ja_JP.UTF-8")
        assert _i18n.resolve_locale() == "ja"

    def test_macos_defaults_to_english_when_everything_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_locale_env(monkeypatch)
        monkeypatch.setattr(sys, "platform", "darwin")
        monkeypatch.setattr(_i18n, "_detect_macos_ui_language", lambda: None)
        assert _i18n.resolve_locale() == "en"


class TestDetectMacosUiLanguage:
    def test_returns_parsed_locale_from_apple_locale(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class _Result:
            returncode = 0
            stdout = "ja_JP\n"

        monkeypatch.setattr(_i18n.subprocess, "run", lambda *a, **kw: _Result())
        assert _i18n._detect_macos_ui_language() == "ja"

    def test_returns_none_when_command_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def _raise(*args: object, **kwargs: object) -> None:
            raise FileNotFoundError("no defaults binary")

        monkeypatch.setattr(_i18n.subprocess, "run", _raise)
        assert _i18n._detect_macos_ui_language() is None

    def test_returns_none_on_nonzero_exit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class _Result:
            returncode = 1
            stdout = ""

        monkeypatch.setattr(_i18n.subprocess, "run", lambda *a, **kw: _Result())
        assert _i18n._detect_macos_ui_language() is None

    def test_returns_none_on_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import subprocess as subprocess_module

        def _raise(*args: object, **kwargs: object) -> None:
            raise subprocess_module.TimeoutExpired(cmd="defaults", timeout=2)

        monkeypatch.setattr(_i18n.subprocess, "run", _raise)
        assert _i18n._detect_macos_ui_language() is None


class TestDetectWindowsUiLanguage:
    def test_returns_parsed_locale_from_langid(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class _FakeKernel32:
            @staticmethod
            def GetUserDefaultUILanguage() -> int:
                return 0x0411  # ja-JP

        class _FakeWindll:
            kernel32 = _FakeKernel32()

        monkeypatch.setattr(_i18n.ctypes, "windll", _FakeWindll(), raising=False)
        assert _i18n._detect_windows_ui_language() == "ja"

    def test_returns_none_when_windll_unavailable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delattr(_i18n.ctypes, "windll", raising=False)
        assert _i18n._detect_windows_ui_language() is None

    def test_returns_none_when_langid_unmapped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class _FakeKernel32:
            @staticmethod
            def GetUserDefaultUILanguage() -> int:
                return 0

        class _FakeWindll:
            kernel32 = _FakeKernel32()

        monkeypatch.setattr(_i18n.ctypes, "windll", _FakeWindll(), raising=False)
        assert _i18n._detect_windows_ui_language() is None


class TestLocaleOverride:
    def test_forces_locale_within_the_block(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_i18n, "_locale", "en")
        with _i18n.locale_override("ja"):
            assert _i18n._locale == "ja"
            assert _i18n.t("Review") == "確認"
        assert _i18n._locale == "en"

    def test_restores_previous_locale_after_an_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_i18n, "_locale", "ja")
        with pytest.raises(RuntimeError), _i18n.locale_override("en"):
            assert _i18n._locale == "en"
            raise RuntimeError("boom")
        assert _i18n._locale == "ja"
