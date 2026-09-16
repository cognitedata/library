"""Tests for the Foundation setup wizard's message catalogue (_i18n, _messages_ja)."""

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
        monkeypatch.setattr(_i18n, "_locale", "ja")
        assert _i18n.t("Review") == "確認"

    def test_unknown_key_falls_back_to_english_in_japanese_locale(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_i18n, "_locale", "ja")
        assert _i18n.t("Some string not in any catalogue") == "Some string not in any catalogue"
