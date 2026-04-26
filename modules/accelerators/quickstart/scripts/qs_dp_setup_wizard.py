#!/usr/bin/env python3
"""
Interactive setup wizard for Quickstart Deployment Pack post-install edits.

What it does:
1) Pre-flight: verifies Toolkit version (≥ 0.7.210) and cdf.toml alpha flags
2) Enables FILE_ANNOTATION mode in asset.Transformation.sql
3) Updates config.<env>.yaml:
   - environment.project
   - cdf_entity_matching defaults from the how-to guide
   - cdf_file_annotation.ApplicationOwner
   - group source env references (single or module-specific)
4) Ensures required secrets exist in .env:
   - GROUP_SOURCE_ID (or per-module variants)
   - OPEN_ID_CLIENT_SECRET
5) (optional) Runs cdf build + cdf deploy --dry-run after writing

Run from the Toolkit project root:
    python modules/accelerators/quickstart/scripts/qs_dp_setup_wizard.py [--env dev] [--skip-verify]
"""
from __future__ import annotations

import argparse
from pathlib import Path

from wizard import _style as style

# Sub-module imports  (wizard/ package — each file owns one concern)
from wizard._constants import (
    APP_OWNER_YAML_PATH,
    ENTITY_MATCHING_UPDATES,
    ENV_PROJECT_YAML_PATH,
    ENV_VAR_GROUP_SOURCE_ID,
    ENV_VAR_OPEN_ID_CLIENT_SECRET,
    GROUP_TARGETS,
    VALID_ENVIRONMENTS,
    ChangeRecord,
)
from wizard._file_io import (
    ensure_backup,
    parse_env_file,
    read_lines,
    upsert_env_var,
    write_lines,
)
from wizard._messages import (
    BANNER_PENDING,
    ENV_SELECT_INTRO,
    GROUP_SOURCE_INTRO,
    GROUP_SOURCE_PER_MODULE_HINT,
    HINT_APP_OWNER_FORMAT,
    HINT_BACKUPS,
    HINT_CURRENT_VALUE,
    HINT_SQL_PENDING,
    MSG_DONE,
    PROMPT_APP_OWNER,
    PROMPT_APPLY,
    PROMPT_PROJECT,
    PROMPT_SHARED_GROUP,
    SEC_APP_OWNER,
    SEC_CDF_PROJECT,
    SEC_GROUP_SOURCE_IDS,
    SEC_OPENID_SECRET,
    WARN_ABORTED,
)
from wizard._preflight import (
    _check_gitignore,
    _get_org_dir,
    check_cdf_toml,
    check_toolkit_version,
)
from wizard._prompts import (
    mask_secret,
    prompt_email,
    prompt_text,
    prompt_yes_no,
    show_changes_table,
    show_env_summary,
)
from wizard._sql import enable_file_annotation_mode
from wizard._verification import run_post_write_verification
from wizard._yaml import (
    _strip_yaml_quotes,
    build_yaml_paths,
    get_yaml_current_value,
    quote_yaml_string,
    set_target_view_filter_values,
    set_yaml_value_by_path,
)

# Private helpers

def _prompt_env_var(
    var: str,
    env_lines: list[str],
    env_key_to_line: dict[str, int],
    env_values: dict[str, str],
) -> None:
    """Prompt to keep or replace an .env variable; updates *env_lines* and *env_values* in place.

    Shows the masked current value when the variable already exists and asks
    keep/replace.  Creates the variable from scratch when absent.
    """
    existing = env_values.get(var, "").strip()
    if existing:
        print(f"  Found {var} in .env (current: {style.CYAN}{mask_secret(existing)}{style.RESET})")
        if not prompt_yes_no(f"  Keep existing {var}?", default=True):
            new_val = prompt_text(f"  New {var}")
            upsert_env_var(env_lines, env_key_to_line, var, new_val)
            env_values[var] = new_val
    else:
        style.warning(f"  {var} not found in .env — it will be created.")
        new_val = prompt_text(f"  {var}")
        upsert_env_var(env_lines, env_key_to_line, var, new_val)
        env_values[var] = new_val


def select_env(repo_root: Path, cli_env: str | None) -> str:
    if cli_env:
        return cli_env
    print(ENV_SELECT_INTRO)
    return prompt_text("Target environment", default="dev").lower()


def find_repo_root(start_path: Path) -> Path:
    """Walk up from *start_path* looking for a directory containing both
    ``cdf.toml`` and ``modules/``.

    Falls back to a path derived from this script's known position in the
    project tree (``<root>/modules/accelerators/quickstart/scripts/``) when
    no ``cdf.toml`` ancestor is found — e.g. when developing inside the
    library repo that has no ``cdf.toml`` at its root.

    NOTE: start_path is resolved from ``__file__``, so this always finds the
    Toolkit project that contains this script regardless of the current working
    directory.
    """
    for candidate in [start_path, *start_path.parents]:
        if (candidate / "cdf.toml").exists() and (candidate / "modules").exists():
            return candidate
    # Fallback: the script lives at <root>/modules/accelerators/quickstart/scripts/
    # so parents[4] == <root>.
    script_derived = Path(__file__).resolve().parents[4]
    if (script_derived / "modules").exists():
        return script_derived
    raise RuntimeError(
        "Could not detect project root. Run this wizard from inside a Toolkit project "
        "containing cdf.toml and modules/."
    )


# Main

def main(
    cli_env: str | None = None,
    skip_verify: bool = False,
    repo_root_override: Path | None = None,
    sql_path_override: Path | None = None,
) -> int:
    # --- Pre-flight ---------------------------------------------------------
    toolkit_version = check_toolkit_version()

    repo_root = repo_root_override or find_repo_root(Path(__file__).resolve().parent)
    check_cdf_toml(repo_root)

    env = select_env(repo_root, cli_env).strip().lower()
    if env not in VALID_ENVIRONMENTS:
        style.error(f"  Error: unsupported environment '{env}'. Choose one of: {', '.join(sorted(VALID_ENVIRONMENTS))}.")
        return 1

    # Resolve config path — check org_dir prefix first, fall back to repo root.
    org_dir = _get_org_dir(repo_root)
    config_filename = f"config.{env}.yaml"
    if org_dir and (repo_root / org_dir / config_filename).exists():
        config_path = repo_root / org_dir / config_filename
        config_arg  = f"{org_dir}/{config_filename}"
    else:
        config_path = repo_root / config_filename
        config_arg  = config_filename

    env_path = repo_root / ".env"

    # Derive sql_path from this script's absolute location so it resolves
    # correctly regardless of the current working directory.
    # The script lives at <root>/modules/accelerators/quickstart/scripts/,
    # so parents[3] == <root>/modules/ and the SQL sits under sourcesystem/.
    sql_path = sql_path_override or (
        Path(__file__).resolve().parents[3]
        / "sourcesystem/cdf_sap_assets/transformations/population/asset.Transformation.sql"
    )

    if not config_path.exists():
        style.error(
            f"  Error: config file not found: {config_path}\n"
            "  Tip: create it first (for staging, add config.staging.yaml) and rerun."
        )
        return 1
    if not sql_path.exists():
        style.error(f"  Error: SQL file not found: {sql_path}")
        return 1

    print(f"\n{style.BOLD}Using config{style.RESET} : {config_path.name}")
    print(f"{style.BOLD}Environment {style.RESET} : {env}")
    _check_gitignore(repo_root)

    # --- Load files ---------------------------------------------------------
    config_lines = read_lines(config_path)
    env_lines, env_values, env_key_to_line = parse_env_file(env_path)
    original_env_values = dict(env_values)
    initial_key_line_map = build_yaml_paths(config_lines)

    # --- CDF project name ---------------------------------------------------
    style.section(SEC_CDF_PROJECT)
    current_project = get_yaml_current_value(config_lines, ENV_PROJECT_YAML_PATH, initial_key_line_map)
    if current_project:
        current_project = _strip_yaml_quotes(current_project)
        style.hint(HINT_CURRENT_VALUE.format(value=current_project))
    plain_project_name = prompt_text(PROMPT_PROJECT, default=current_project or None)
    project_name = quote_yaml_string(plain_project_name)

    # --- ApplicationOwner email(s) ------------------------------------------
    style.section(SEC_APP_OWNER)
    current_app_owner = get_yaml_current_value(config_lines, APP_OWNER_YAML_PATH, initial_key_line_map)
    if current_app_owner:
        current_app_owner = _strip_yaml_quotes(current_app_owner)
        style.hint(HINT_CURRENT_VALUE.format(value=current_app_owner))
    style.hint(HINT_APP_OWNER_FORMAT)
    app_owner = quote_yaml_string(
        prompt_email(PROMPT_APP_OWNER, default=current_app_owner or None)
    )

    # --- Group source strategy ----------------------------------------------
    n_modules = len(GROUP_TARGETS)
    module_names = ", ".join(t.module.split("/")[-1] for t in GROUP_TARGETS)
    style.section(SEC_GROUP_SOURCE_IDS)
    print(GROUP_SOURCE_INTRO.format(n=n_modules, module_names=module_names))
    print(
        f"  {style.BOLD}Option A{style.RESET} : one shared group for all — simpler to manage. (recommended)\n"
        f"  {style.BOLD}Option B{style.RESET} : one group per module — finer-grained access control.\n"
        f"    (when choosing B you will be prompted for each of the {n_modules} modules;\n"
        "     modules already set in .env will show their current value with keep/replace)\n"
    )
    use_same_group_everywhere = prompt_yes_no(PROMPT_SHARED_GROUP, default=True)

    group_env_by_target: dict[tuple[str, ...], str] = {}
    if use_same_group_everywhere:
        _prompt_env_var(ENV_VAR_GROUP_SOURCE_ID, env_lines, env_key_to_line, env_values)
        for target in GROUP_TARGETS:
            group_env_by_target[target.path] = ENV_VAR_GROUP_SOURCE_ID
    else:
        style.hint(GROUP_SOURCE_PER_MODULE_HINT)
        for target in GROUP_TARGETS:
            var = target.default_env_var
            print(f"  {style.BOLD}Module {style.RESET} : {target.module}")
            print(f"  {style.BOLD}Param  {style.RESET} : {target.label.split('.')[-1]}")
            style.hint(f"  Purpose : {target.description}")
            print(f"  {style.BOLD}Env var{style.RESET} : {var}")
            _prompt_env_var(var, env_lines, env_key_to_line, env_values)
            group_env_by_target[target.path] = var
            print()

    # --- OPEN_ID_CLIENT_SECRET ----------------------------------------------
    style.section(SEC_OPENID_SECRET)
    _prompt_env_var(ENV_VAR_OPEN_ID_CLIENT_SECRET, env_lines, env_key_to_line, env_values)

    # --- Apply changes to in-memory lines -----------------------------------
    key_line_map = build_yaml_paths(config_lines)
    records: list[ChangeRecord] = []

    def _apply(label: str, result: tuple[str, str] | None) -> bool:
        if result is None:
            style.warning(f"  Warning: could not find {label} in config.")
            return False
        old_v, new_v = result
        records.append(ChangeRecord(label, old_v, new_v))
        return True

    _apply(
        "environment.project",
        set_yaml_value_by_path(config_lines, ENV_PROJECT_YAML_PATH, project_name, key_line_map),
    )
    for path, value in ENTITY_MATCHING_UPDATES:
        _apply(
            ".".join(path[-2:]),
            set_yaml_value_by_path(config_lines, path, value, key_line_map),
        )
    _apply(
        "cdf_entity_matching.targetViewFilterValues",
        set_target_view_filter_values(config_lines, "root:ast_VAL", key_line_map),
    )
    _apply(
        "cdf_file_annotation.ApplicationOwner",
        set_yaml_value_by_path(config_lines, APP_OWNER_YAML_PATH, app_owner, key_line_map),
    )
    for target in GROUP_TARGETS:
        env_var = group_env_by_target[target.path]
        _apply(
            target.label,
            set_yaml_value_by_path(
                config_lines, target.path, f"${{{env_var}}}", key_line_map
            ),
        )

    # --- Show summary and confirm before writing ----------------------------
    real_changes = sum(1 for r in records if r.changed)

    style.banner(BANNER_PENDING)

    print(f"\n  {style.BOLD}[{config_path.name}]{style.RESET}  ({real_changes} field(s) will actually change)")
    show_changes_table(records)

    print(f"\n  {style.BOLD}[.env]{style.RESET}")
    show_env_summary(original_env_values, env_values)

    print(f"\n  {style.BOLD}[{sql_path.name}]{style.RESET}")
    style.hint(HINT_SQL_PENDING)

    print()
    if not prompt_yes_no(PROMPT_APPLY, default=True):
        style.warning(WARN_ABORTED)
        return 0

    # --- Write files (ACID: all backups created before any write) -----------
    if env_path.exists():
        ensure_backup(env_path)
    ensure_backup(config_path)
    ensure_backup(sql_path)

    if env_lines:
        write_lines(env_path, env_lines)
    write_lines(config_path, config_lines)
    sql_changed = enable_file_annotation_mode(sql_path, skip_backup=True)

    # --- Summary ------------------------------------------------------------
    style.success(MSG_DONE)
    print(f"  {style.BOLD}Config {style.RESET} : {config_path}  ({real_changes} field(s) changed)")
    print(f"  {style.BOLD}.env   {style.RESET} : {env_path}")
    print(
        f"  {style.BOLD}SQL    {style.RESET} : FILE_ANNOTATION mode "
        f"{'enabled' if sql_changed else 'was already enabled'} in {sql_path.name}"
    )
    style.hint(HINT_BACKUPS)

    if not skip_verify:
        run_post_write_verification(repo_root, env, config_arg, toolkit_version, plain_project_name, org_dir)

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Interactive Quickstart DP setup wizard. "
            "Configures exactly one environment (dev/prod/staging) per run."
        )
    )
    parser.add_argument(
        "--env",
        choices=sorted(VALID_ENVIRONMENTS),
        help="Target environment. If omitted, you will be prompted.",
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip the post-write cdf build / deploy verification step.",
    )
    _args = parser.parse_args()
    try:
        raise SystemExit(main(cli_env=_args.env, skip_verify=_args.skip_verify))
    except KeyboardInterrupt:
        print("\nCancelled by user.")
        raise SystemExit(130)
