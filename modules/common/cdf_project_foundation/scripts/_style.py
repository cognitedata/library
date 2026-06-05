"""Terminal styling, change tracking, and change-table display for the setup wizard."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any

_USE_COLOR = sys.stdout.isatty()


class _C:
    """ANSI escape codes — empty strings when stdout is not a TTY."""

    BOLD   = "\033[1m"  if _USE_COLOR else ""
    DIM    = "\033[2m"  if _USE_COLOR else ""
    GREEN  = "\033[32m" if _USE_COLOR else ""
    YELLOW = "\033[33m" if _USE_COLOR else ""
    CYAN   = "\033[36m" if _USE_COLOR else ""
    RED    = "\033[31m" if _USE_COLOR else ""
    RESET  = "\033[0m"  if _USE_COLOR else ""


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
    col1 = max(len(r.label) for r in records)
    col2 = max(len(str(r.old_value or "")) for r in records)
    hdr = f"  {'Variable':<{col1}}  {'Current':<{col2}}  New"
    print(f"{_C.DIM}{hdr}{_C.RESET}")
    print(f"  {_C.DIM}{'─' * (col1 + col2 + 20)}{_C.RESET}")
    for r in records:
        marker = f"{_C.GREEN}→{_C.RESET}" if r.changed else f"{_C.DIM}={_C.RESET}"
        old_s = str(r.old_value) if r.old_value is not None else f"{_C.DIM}(not set){_C.RESET}"
        new_s = f"{_C.GREEN}{r.new_value}{_C.RESET}" if r.changed else str(r.new_value)
        print(f"  {r.label:<{col1}}  {old_s:<{col2}}  {marker} {new_s}")
