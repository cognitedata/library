"""English-keyed message catalogue lookup for the setup wizard.

``t()`` looks up strings by their canonical English source text, so the English
literal at each call site doubles as the catalogue key — no separate ID scheme to
maintain. Locale is not wired in yet; ``_locale`` is a fixed default until locale
detection (``CDF_LOCALE`` / terminal locale) is added on top of this module.
"""

from _messages_ja import messages_ja

_locale = "en"


def t(key: str) -> str:
    """Translate *key* — the canonical English string — into the active locale.

    Returns *key* unchanged when the active locale is English, or when *key* has
    no entry in the catalogue for the active locale. Never raises on an unknown key.
    """
    if _locale != "ja":
        return key
    return messages_ja.get(key, key)
