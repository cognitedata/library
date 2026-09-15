"""Terminal styling helpers for the setup wizard."""


import sys
import unicodedata

_USE_COLOR = sys.stdout.isatty()


def _display_width(text: str) -> int:
    """Terminal display width of *text*.

    East Asian Wide and Fullwidth characters (most Japanese kana/kanji and
    full-width punctuation) render as 2 terminal columns; everything else is 1.
    ``len()`` counts one column per character regardless of script, so it
    undercounts Japanese text and breaks fixed-width layout (box borders,
    padded menus) once the string contains CJK characters.
    """
    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1 for ch in text)


class _C:
    """ANSI escape codes — empty strings when stdout is not a TTY."""

    BOLD:   str = "\033[1m"  if _USE_COLOR else ""
    DIM:    str = "\033[2m"  if _USE_COLOR else ""
    GREEN:  str = "\033[32m" if _USE_COLOR else ""
    YELLOW: str = "\033[33m" if _USE_COLOR else ""
    CYAN:   str = "\033[36m" if _USE_COLOR else ""
    RED:    str = "\033[31m" if _USE_COLOR else ""
    RESET:  str = "\033[0m"  if _USE_COLOR else ""


# ── Print helpers ──────────────────────────────────────────────────────────────

def _banner(title: str) -> None:
    # The box is at least 56 columns wide (the original fixed width, kept for
    # short titles) and grows to fit longer titles — including Japanese ones,
    # where _display_width() counts wide characters as 2 columns, not 1.
    width = max(56, _display_width(title) + 2)
    line = "─" * width
    print(f"\n{_C.BOLD}{line}{_C.RESET}")
    print(f"{_C.BOLD}  {title}{_C.RESET}")
    print(f"{_C.BOLD}{line}{_C.RESET}")


def _section(title: str) -> None:
    print(f"\n{_C.CYAN}{_C.BOLD}── {title} ──{_C.RESET}")


def _ok(msg: str) -> None:
    print(f"  {_C.GREEN}✓{_C.RESET}  {msg}")


def _warn(msg: str) -> None:
    print(f"  {_C.YELLOW}⚠{_C.RESET}  {msg}")


def _hint(msg: str) -> None:
    print(f"  {_C.DIM}{msg}{_C.RESET}")
