"""Terminal styling helpers for the setup wizard."""


import sys

_USE_COLOR = sys.stdout.isatty()


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
    line = "─" * 56
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
