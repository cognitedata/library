"""Interactive prompt helpers for the setup wizard."""

from __future__ import annotations

from _env_io import upsert_env
from _style import _C, _warn


def prompt(msg: str, default: str | None = None) -> str:
    """Prompt for a string value, returning *default* on empty input."""
    prompt_str = f"  {msg} [{_C.CYAN}{default}{_C.RESET}]: " if default else f"  {msg}: "
    try:
        answer = input(prompt_str).strip()
    except EOFError:
        answer = ""
    return answer or (default or "")


def prompt_yes_no(msg: str, default: bool = True) -> bool:
    """Prompt for a yes/no answer, returning *default* on empty input."""
    hint = "[Y/n]" if default else "[y/N]"
    try:
        raw = input(f"  {msg} {hint}: ").strip().lower()
    except EOFError:
        raw = ""
    if not raw:
        return default
    return raw in ("y", "yes")


def prompt_choice(options: list[str], default: int = 1) -> int:
    """Display a numbered menu and return the 1-based index of the chosen option."""
    for i, opt in enumerate(options, 1):
        marker = f"{_C.CYAN}▶{_C.RESET}" if i == default else " "
        print(f"  {marker} [{i}] {opt}")
    while True:
        try:
            raw = input(f"  Choice [{default}]: ").strip()
        except EOFError:
            return default
        if not raw:
            return default
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return int(raw)
        _warn(f"Please enter a number between 1 and {len(options)}.")


def prompt_env_var(
    var: str,
    env_vals: dict[str, str],
    env_lines: list[str],
    env_key_idx: dict[str, int],
) -> None:
    """Ask the user for a ``.env`` variable value.

    Shows the masked current value when *var* already exists (keep/replace).
    Creates the variable from scratch when absent.
    Updates *env_lines*, *env_key_idx*, and *env_vals* in place.
    """
    existing = env_vals.get(var, "").strip()
    if existing:
        masked = existing[:3] + "****" if len(existing) > 6 else "****"
        print(f"  Found {_C.CYAN}{var}{_C.RESET} in .env  (current: {_C.DIM}{masked}{_C.RESET})")
        if not prompt_yes_no(f"  Keep existing {var}?", default=True):
            new_val = prompt(f"  New value for {var}")
            upsert_env(env_lines, env_key_idx, var, new_val)
            env_vals[var] = new_val
    else:
        _warn(f"{var} not in .env — will be created.")
        new_val = prompt(f"  Value for {var} (leave blank to skip)")
        if new_val:
            upsert_env(env_lines, env_key_idx, var, new_val)
            env_vals[var] = new_val
