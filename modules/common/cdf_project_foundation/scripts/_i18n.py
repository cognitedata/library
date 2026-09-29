"""English-keyed message catalogue lookup for the setup wizard.

``t()`` looks up strings by their canonical English source text, so the English
literal at each call site doubles as the catalogue key — no separate ID scheme to
maintain. The active locale is resolved once, at import time, from ``CDF_LOCALE``
or the terminal's locale environment — see ``resolve_locale()``.
"""

import contextlib
import ctypes
import locale as _locale_module
import os
import subprocess
import sys
from collections.abc import Iterator

from _messages_ja import MESSAGES_JA

SUPPORTED_LOCALES = ("en", "ja")

# locale.getlocale() on Windows commonly returns display names (e.g.
# "Japanese_Japan") rather than POSIX-style codes ("ja_JP") — map the language
# part of those names to the same supported codes.
_WINDOWS_LANGUAGE_ALIASES = {"japanese": "ja", "english": "en"}


def _parse_locale_code(value: str | None) -> str | None:
    """Extract a supported locale code from a POSIX/Windows locale string.

    Accepts forms like ``"ja"``, ``"ja_JP.UTF-8"``, ``"en-US"``, and Windows
    display names like ``"Japanese_Japan"``. Returns ``None`` for ``""``, ``"C"``,
    ``"POSIX"``, or anything not in ``SUPPORTED_LOCALES`` — never raises.
    """
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized in ("", "c", "posix"):
        return None
    lang = normalized.split(".")[0].split("_")[0].split("-")[0]
    if lang in SUPPORTED_LOCALES:
        return lang
    return _WINDOWS_LANGUAGE_ALIASES.get(lang)


def _detect_macos_ui_language() -> str | None:
    """Read the user's macOS UI language via ``defaults read -g AppleLocale``.
    """
    try:
        result = subprocess.run(
            ["/usr/bin/defaults", "read", "-g", "AppleLocale"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return _parse_locale_code(result.stdout.strip())


def _detect_windows_ui_language() -> str | None:
    """Read the user's Windows UI language via the Win32 API.
    """
    try:
        langid = ctypes.windll.kernel32.GetUserDefaultUILanguage()  # type: ignore[attr-defined]
    except (AttributeError, OSError):
        return None
    language_code = _locale_module.windows_locale.get(langid)
    return _parse_locale_code(language_code)


def resolve_locale() -> str:
    """Resolve the active locale for the wizard.

    Resolution order: the ``CDF_LOCALE`` environment variable (always wins), then
    the OS's own UI-language setting — ``AppleLocale`` on macOS,
    ``GetUserDefaultUILanguage`` on Windows — then, as a fallback for platforms
    with no separate "UI language" concept (Linux/CI/containers) or when OS
    detection fails, the POSIX locale env vars ``LC_ALL`` -> ``LC_MESSAGES`` ->
    ``LANG``. Falls back to ``"en"`` silently when nothing resolves to a supported
    locale — never raises, never prompts.
    """
    override = _parse_locale_code(os.environ.get("CDF_LOCALE"))
    if override:
        return override

    if sys.platform == "darwin":
        detected = _detect_macos_ui_language()
        if detected:
            return detected
    elif sys.platform == "win32":
        detected = _detect_windows_ui_language()
        if detected:
            return detected
        try:
            language_code, _ = _locale_module.getlocale()
        except ValueError:
            language_code = None
        parsed = _parse_locale_code(language_code)
        if parsed:
            return parsed
        return "en"

    for var in ("LC_ALL", "LC_MESSAGES", "LANG"):
        parsed = _parse_locale_code(os.environ.get(var))
        if parsed:
            return parsed
    return "en"


_locale = resolve_locale()


def t(key: str) -> str:
    """Translate *key* — the canonical English string — into the active locale.

    Returns *key* unchanged when the active locale is English, or when *key* has
    no entry in the catalogue for the active locale. Never raises on an unknown key.
    """
    if _locale != "ja":
        return key
    return MESSAGES_JA.get(key, key)


@contextlib.contextmanager
def locale_override(locale_code: str) -> Iterator[None]:
    """Temporarily force the active locale within the ``with`` block.

    Used by ``--check`` mode, whose output and exit codes must stay identical
    regardless of the caller's locale (CI/tooling consumes it, not a person).
    """
    global _locale
    previous = _locale
    _locale = locale_code
    try:
        yield
    finally:
        _locale = previous
