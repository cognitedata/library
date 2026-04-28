"""
User-interaction helpers for the Quickstart DP setup wizard.

Covers terminal prompts, email validation, secret masking, and the
pre-write change-table / .env summary display.
"""
from __future__ import annotations

from ._constants import _EMAIL_RE, ChangeRecord
from . import _style as style


# Prompt helpers

def prompt_text(message: str, default: str | None = None, allow_empty: bool = False) -> str:
    """Prompt for a non-empty string, optionally falling back to a default."""
    while True:
        suffix = f" [{default}]" if default is not None else ""
        value = input(f"{message}{suffix}: ").strip()
        if not value and default is not None:
            return default
        if value or allow_empty:
            return value
        style.error("  Error: input cannot be empty.")


def prompt_yes_no(message: str, default: bool = False) -> bool:
    """Prompt for a yes/no answer. Returns *default* on empty input."""
    choice_hint = "Y/n" if default else "y/N"
    while True:
        answer = input(f"{message} ({choice_hint}): ").strip().lower()
        if not answer:
            return default
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        style.error("  Error: please enter y/yes or n/no.")


def validate_emails(raw: str) -> tuple[bool, str]:
    """Validate one or more comma-separated email addresses.

    Returns ``(True, "")`` on success or ``(False, error_message)`` on failure.
    """
    emails = [e.strip() for e in raw.split(",") if e.strip()]
    if not emails:
        return False, "at least one email address is required."
    invalid = [e for e in emails if not _EMAIL_RE.fullmatch(e)]
    if invalid:
        return False, f"invalid email address(es): {', '.join(invalid)}"
    return True, ""


def prompt_email(message: str, default: str | None = None) -> str:
    """Prompt for one or more comma-separated email addresses with regex validation."""
    while True:
        suffix = f" [{default}]" if default is not None else ""
        raw = input(f"{message}{suffix}: ").strip()
        if not raw and default is not None:
            return default
        ok, err = validate_emails(raw)
        if ok:
            return raw
        style.error(f"  Error: {err}")


def mask_secret(value: str) -> str:
    """Return a masked version showing only the first and last two characters."""
    if len(value) <= 6:
        return "****"
    return f"{value[:2]}****{value[-2:]}"


# Pre-write summary display

def show_changes_table(records: list[ChangeRecord]) -> None:
    """Print a styled table of pending field changes.

    - Header row : bold
    - Changed rows : new value highlighted green
    - Unchanged rows : entire row dimmed, note in yellow
    """
    if not records:
        style.hint("  (no config changes)")
        return

    col_label = min(max(len(r.label) for r in records), 52)
    col_old   = min(max(len(r.old_val) for r in records), 28)
    col_new   = min(max(len(r.new_val) for r in records), 28)

    def _trunc(s: str, n: int) -> str:
        return s if len(s) <= n else s[:n - 1] + "…"

    # Header
    header = (
        f"  {style.BOLD}"
        f"{'Field':<{col_label}}  {'Old value':<{col_old}}  {'New value':<{col_new}}"
        f"{style.RESET}"
    )
    sep = f"  {style.DIM}" + "─" * (col_label + col_old + col_new + 4) + style.RESET
    print(header)
    print(sep)

    for r in records:
        label = _trunc(r.label, col_label)
        old_v = _trunc(r.old_val, col_old)
        new_v = _trunc(r.new_val, col_new)
        if r.changed:
            print(
                f"  {label:<{col_label}}"
                f"  {old_v:<{col_old}}"
                f"  {style.GREEN}{new_v:<{col_new}}{style.RESET}"
            )
        else:
            print(
                f"  {style.DIM}{label:<{col_label}}"
                f"  {old_v:<{col_old}}"
                f"  {new_v:<{col_new}}{style.RESET}"
                f"  {style.YELLOW}(unchanged){style.RESET}"
            )


def show_env_summary(original_values: dict[str, str], new_values: dict[str, str]) -> None:
    """Summarise .env changes without revealing secret values."""
    added   = [k for k in new_values if k not in original_values]
    changed = [
        k for k in new_values
        if k in original_values and new_values[k] != original_values[k]
    ]
    if not added and not changed:
        style.hint("  (no changes to .env)")
        return
    if added:
        print(f"  {style.GREEN}Keys to add{style.RESET}    : {', '.join(added)}")
    if changed:
        print(f"  {style.YELLOW}Keys to update{style.RESET} : {', '.join(changed)}")
    style.hint("  (secret values are never shown)")
