"""
Terminal styling using ANSI escape codes — stdlib only, no external packages.

Colour is automatically disabled when:
  - stdout is not a TTY (e.g. redirected to a file or captured in tests)
  - the ``NO_COLOR`` environment variable is set (https://no-color.org)
  - ``TERM=dumb``

All public helpers fall back to plain ``print()`` output in those cases, so
tests that capture stdout with ``capsys`` see uncoloured text and need no
changes.
"""
from __future__ import annotations

import os
import shutil
import sys


def _supports_color() -> bool:
    if not hasattr(sys.stdout, "isatty") or not sys.stdout.isatty():
        return False
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("TERM", "").lower() == "dumb":
        return False
    return True


_C = _supports_color()

# ANSI codes (empty strings when colour is disabled)
RESET  = "\033[0m"  if _C else ""
BOLD   = "\033[1m"  if _C else ""
DIM    = "\033[2m"  if _C else ""
RED    = "\033[91m" if _C else ""
GREEN  = "\033[92m" if _C else ""
YELLOW = "\033[93m" if _C else ""
CYAN   = "\033[96m" if _C else ""


def _width() -> int:
    """Return current terminal width, defaulting to 80."""
    return shutil.get_terminal_size((80, 24)).columns


# Structural elements

def section(title: str) -> None:
    """Print a coloured section divider that fills the terminal width.

    Plain-text fallback (no TTY / NO_COLOR):
        ── CDF Project ──────────────────────────────────────────────────

    Colour TTY:
        (same line, bold cyan)
    """
    prefix = "── "
    suffix = " "
    fill_len = max(0, _width() - len(prefix) - len(title) - len(suffix) - 2)
    fill = "─" * fill_len
    print(f"\n{BOLD}{CYAN}{prefix}{title}{suffix}{fill}{RESET}")


def banner(title: str) -> None:
    """Print a bold banner box around *title*.

    Plain-text (or colour) output:
        ╔══════════════════════════════════════════════════════╗
        ║  PENDING CHANGES — review before applying           ║
        ╚══════════════════════════════════════════════════════╝
    """
    box_w = min(_width() - 2, 68)   # total width including corner chars
    inner = box_w - 4               # space available for text (2 corners + 2 spaces)
    top    = "╔" + "═" * (box_w - 2) + "╗"
    middle = "║  " + title[:inner].ljust(inner) + "  ║"
    bottom = "╚" + "═" * (box_w - 2) + "╝"
    print(f"\n{BOLD}{top}")
    print(middle)
    print(f"{bottom}{RESET}")


# Message-level helpers

def error(message: str) -> None:
    """Print a red bold error message.  Multi-line strings are handled gracefully."""
    lines = message.splitlines()
    if not lines:
        return
    print(f"{RED}{BOLD}{lines[0]}{RESET}")
    for line in lines[1:]:
        print(f"{RED}{line}{RESET}")


def warning(message: str) -> None:
    """Print a yellow warning message."""
    lines = message.splitlines()
    if not lines:
        return
    print(f"{YELLOW}{lines[0]}{RESET}")
    for line in lines[1:]:
        print(f"{YELLOW}{line}{RESET}")


def success(message: str) -> None:
    """Print a green bold success message."""
    print(f"{GREEN}{BOLD}{message}{RESET}")


def hint(message: str) -> None:
    """Print a dimmed informational / hint line."""
    print(f"{DIM}{message}{RESET}")
