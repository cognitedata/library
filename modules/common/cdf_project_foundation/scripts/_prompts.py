"""Interactive prompt helpers for the setup wizard."""


from _env_io import upsert_env
from _i18n import t
from _style import _C, _display_width, _warn


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
    hint = t("[Y/n]") if default else t("[y/N]")
    try:
        raw = input(f"  {msg} {hint}: ").strip().lower()
    except EOFError:
        raw = ""
    if not raw:
        return default
    return raw in ("y", "yes")


def prompt_choice(options: list[str], default: int = 1) -> int:
    """Display a numbered menu and return the 1-based index of the chosen option."""
    # Right-align the index so option text starts in the same column for every
    # row, using display width (not len()) so this stays correct once options
    # are Japanese text.
    index_width = _display_width(str(len(options)))
    for i, opt in enumerate(options, 1):
        marker = f"{_C.CYAN}▶{_C.RESET}" if i == default else " "
        index_str = str(i).rjust(index_width)
        print(f"  {marker} [{index_str}] {opt}")
    while True:
        try:
            raw = input(f"  {t('Choice [{default}]: ').format(default=default)}").strip()
        except EOFError:
            return default
        if not raw:
            return default
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return int(raw)
        _warn(t("Please enter a number between 1 and {n}.").format(n=len(options)))


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
        var_disp = f"{_C.CYAN}{var}{_C.RESET}"
        masked_disp = f"{_C.DIM}{masked}{_C.RESET}"
        print(f"  {t('Found {var} in .env  (current: {masked})').format(var=var_disp, masked=masked_disp)}")
        if not prompt_yes_no(f"  {t('Keep existing {var}?').format(var=var)}", default=True):
            new_val = prompt(f"  {t('New value for {var}').format(var=var)}")
            upsert_env(env_lines, env_key_idx, var, new_val)
            env_vals[var] = new_val
    else:
        _warn(t("{var} not in .env — will be created.").format(var=var))
        new_val = prompt(f"  {t('Value for {var} (leave blank to skip)').format(var=var)}")
        if new_val:
            upsert_env(env_lines, env_key_idx, var, new_val)
            env_vals[var] = new_val
