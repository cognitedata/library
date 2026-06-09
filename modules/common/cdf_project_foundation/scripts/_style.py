"""Terminal styling, change tracking, and change-table display for the setup wizard."""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from typing import Any

_USE_COLOR = sys.stdout.isatty()

# Strip ANSI escape codes when computing visible string length for column alignment.
_ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')


def _vlen(s: str) -> int:
    """Visible length of *s* — excludes ANSI escape sequences."""
    return len(_ANSI_RE.sub("", s))


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


# ── Change tracking ────────────────────────────────────────────────────────────

@dataclass
class ChangeRecord:
    label: str
    old_value: Any
    new_value: Any

    @property
    def changed(self) -> bool:
        return self.old_value != self.new_value


def _show_changes_table(records: list[ChangeRecord]) -> None:
    if not records:
        _hint("(no variables to update)")
        return

    # Compute column widths from plain (ANSI-stripped) text to keep alignment correct.
    col1 = max(_vlen(r.label) for r in records)
    col2 = max(_vlen(str(r.old_value or "")) for r in records)

    hdr = f"  {'Variable':<{col1}}  {'Current':<{col2}}  New"
    print(f"{_C.DIM}{hdr}{_C.RESET}")
    print(f"  {_C.DIM}{'─' * (col1 + col2 + 20)}{_C.RESET}")

    for r in records:
        marker = f"{_C.GREEN}→{_C.RESET}" if r.changed else f"{_C.DIM}={_C.RESET}"
        # Build display strings, then pad using visible length so ANSI codes don't skew columns.
        old_plain = str(r.old_value) if r.old_value is not None else "(not set)"
        new_plain = str(r.new_value)
        old_s = old_plain if r.old_value is not None else f"{_C.DIM}(not set){_C.RESET}"
        new_s = f"{_C.GREEN}{new_plain}{_C.RESET}" if r.changed else new_plain
        old_pad = " " * (col2 - _vlen(old_plain))
        print(f"  {r.label:<{col1}}  {old_s}{old_pad}  {marker} {new_s}")
