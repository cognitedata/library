#!/usr/bin/env python3
"""
Interactive setup wizard for Quickstart Deployment Pack post-install edits.

What it does:
1) Enables FILE_ANNOTATION mode in asset.Transformation.sql
2) Updates config.<env>.yaml:
   - environment.project
   - cdf_entity_matching defaults from the how-to guide
   - cdf_file_annotation.ApplicationOwner
   - group source env references (single or module-specific)
   - optional cron placeholder updates
3) Ensures required secrets exist in .env:
   - GROUP_SOURCE_ID
   - OPEN_ID_CLIENT_SECRET
"""

from __future__ import annotations

import argparse
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


DEFAULT_CRON_PLACEHOLDER = "0 0 29 2 *"


@dataclass(frozen=True)
class GroupTarget:
    label: str
    path: Tuple[str, ...]
    default_env_var: str


GROUP_TARGETS: Sequence[GroupTarget] = (
    GroupTarget(
        label="cdf_ingestion.groupSourceId",
        path=("variables", "modules", "accelerators", "cdf_ingestion", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_INGESTION",
    ),
    GroupTarget(
        label="cdf_entity_matching.entity_matching_processing_group_source_id",
        path=(
            "variables",
            "modules",
            "accelerators",
            "contextualization",
            "cdf_entity_matching",
            "entity_matching_processing_group_source_id",
        ),
        default_env_var="GROUP_SOURCE_ID_ENTITY_MATCHING",
    ),
    GroupTarget(
        label="cdf_file_annotation.groupSourceId",
        path=(
            "variables",
            "modules",
            "accelerators",
            "contextualization",
            "cdf_file_annotation",
            "groupSourceId",
        ),
        default_env_var="GROUP_SOURCE_ID_FILE_ANNOTATION",
    ),
    GroupTarget(
        label="open_industrial_data_sync.groupSourceId",
        path=("variables", "modules", "accelerators", "open_industrial_data_sync", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_OID_SYNC",
    ),
    GroupTarget(
        label="rpt_quality.groupSourceId",
        path=("variables", "modules", "dashboards", "rpt_quality", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_QUALITY",
    ),
    GroupTarget(
        label="cdf_pi.groupSourceId",
        path=("variables", "modules", "sourcesystem", "cdf_pi", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_PI",
    ),
    GroupTarget(
        label="cdf_sap_assets.groupSourceId",
        path=("variables", "modules", "sourcesystem", "cdf_sap_assets", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_SAP_ASSETS",
    ),
    GroupTarget(
        label="cdf_sap_events.groupSourceId",
        path=("variables", "modules", "sourcesystem", "cdf_sap_events", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_SAP_EVENTS",
    ),
    GroupTarget(
        label="cdf_sharepoint.groupSourceId",
        path=("variables", "modules", "sourcesystem", "cdf_sharepoint", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_SHAREPOINT",
    ),
)


ENTITY_MATCHING_UPDATES: Sequence[Tuple[Tuple[str, ...], str]] = (
    (
        (
            "variables",
            "modules",
            "accelerators",
            "contextualization",
            "cdf_entity_matching",
            "targetViewSearchProperty",
        ),
        "aliases",
    ),
    (
        (
            "variables",
            "modules",
            "accelerators",
            "contextualization",
            "cdf_entity_matching",
            "AssetViewExternalId",
        ),
        "Asset",
    ),
    (
        (
            "variables",
            "modules",
            "accelerators",
            "contextualization",
            "cdf_entity_matching",
            "TimeSeriesViewExternalId",
        ),
        "Enterprise_TimeSeries",
    ),
    (
        (
            "variables",
            "modules",
            "accelerators",
            "contextualization",
            "cdf_entity_matching",
            "targetViewExternalId",
        ),
        "Asset",
    ),
    (
        (
            "variables",
            "modules",
            "accelerators",
            "contextualization",
            "cdf_entity_matching",
            "entityViewExternalId",
        ),
        "Enterprise_TimeSeries",
    ),
)


def prompt_text(message: str, default: Optional[str] = None, allow_empty: bool = False) -> str:
    while True:
        suffix = f" [{default}]" if default else ""
        value = input(f"{message}{suffix}: ").strip()
        if not value and default is not None:
            return default
        if value or allow_empty:
            return value
        print("Input cannot be empty.")


def prompt_yes_no(message: str, default: bool = False) -> bool:
    choice_hint = "Y/n" if default else "y/N"
    while True:
        answer = input(f"{message} ({choice_hint}): ").strip().lower()
        if not answer:
            return default
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please enter y/yes or n/no.")


def ensure_backup(path: Path) -> None:
    backup_path = path.with_suffix(path.suffix + ".bak")
    if not backup_path.exists():
        shutil.copy2(path, backup_path)


def read_lines(path: Path) -> List[str]:
    return path.read_text(encoding="utf-8").splitlines(keepends=True)


def write_lines(path: Path, lines: Sequence[str]) -> None:
    text = "".join(lines)
    path.write_text(text, encoding="utf-8")


def parse_env_file(path: Path) -> Tuple[List[str], Dict[str, str], Dict[str, int]]:
    if not path.exists():
        return [], {}, {}
    lines = read_lines(path)
    values: Dict[str, str] = {}
    key_to_line: Dict[str, int] = {}
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        values[key] = value.rstrip("\n")
        key_to_line[key] = idx
    return lines, values, key_to_line


def upsert_env_var(lines: List[str], key_to_line: Dict[str, int], key: str, value: str) -> None:
    new_line = f"{key}={value}\n"
    if key in key_to_line:
        lines[key_to_line[key]] = new_line
    else:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] = lines[-1] + "\n"
        lines.append(new_line)
        key_to_line[key] = len(lines) - 1


def yaml_key_match(line: str) -> Optional[Tuple[int, str]]:
    m = re.match(r"^(\s*)([A-Za-z0-9_]+):", line)
    if not m:
        return None
    return len(m.group(1)), m.group(2)


def build_yaml_paths(lines: Sequence[str]) -> Dict[Tuple[str, ...], int]:
    key_line_map: Dict[Tuple[str, ...], int] = {}
    stack: List[Tuple[int, str]] = []

    for idx, line in enumerate(lines):
        parsed = yaml_key_match(line)
        if not parsed:
            continue
        indent, key = parsed
        while stack and indent <= stack[-1][0]:
            stack.pop()
        current_path = tuple(k for _, k in stack) + (key,)
        key_line_map[current_path] = idx
        stack.append((indent, key))

    return key_line_map


def set_yaml_line_value(line: str, value: str) -> str:
    m = _YAML_LINE_RE.match(line.rstrip("\n"))
    if not m:
        raise ValueError(f"Cannot set value for YAML line: {line!r}")
    comment_part = m.group(3) or ""
    if comment_part.strip().startswith("#"):
        comment_part = f" {comment_part.strip()}"
    return f"{m.group(1)}{value}{comment_part}\n"


def set_yaml_value_by_path(
    lines: List[str],
    path: Tuple[str, ...],
    value: str,
    key_line_map: Optional[Dict[Tuple[str, ...], int]] = None,
) -> bool:
    if key_line_map is None:
        key_line_map = build_yaml_paths(lines)
    idx = key_line_map.get(path)
    if idx is None:
        return False
    lines[idx] = set_yaml_line_value(lines[idx], value)
    return True


def set_target_view_filter_values(
    lines: List[str],
    desired_value: str,
    key_line_map: Optional[Dict[Tuple[str, ...], int]] = None,
) -> bool:
    if key_line_map is None:
        key_line_map = build_yaml_paths(lines)
    base_path = (
        "variables",
        "modules",
        "accelerators",
        "contextualization",
        "cdf_entity_matching",
        "targetViewFilterValues",
    )
    idx = key_line_map.get(base_path)
    if idx is None:
        return False

    if idx + 1 >= len(lines):
        return False

    list_line = lines[idx + 1]
    list_match = re.match(r"^(\s*)-\s*.*$", list_line)
    if not list_match:
        return False
    indent = list_match.group(1)
    lines[idx + 1] = f"{indent}- {desired_value}\n"
    return True


def collect_cron_placeholder_lines(lines: Sequence[str]) -> List[Tuple[int, str]]:
    result: List[Tuple[int, str]] = []
    for idx, line in enumerate(lines):
        m = re.match(r"^(\s*)([A-Za-z0-9_]+):\s*0 0 29 2 \*\s*(?:#.*)?$", line)
        if not m:
            continue
        result.append((idx, m.group(2)))
    return result


_YAML_LINE_RE = re.compile(r"^(\s*[A-Za-z0-9_]+:\s*)([^#\n]*)(\s*(?:#.*)?)$")


def quote_yaml_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def enable_file_annotation_mode(sql_path: Path) -> bool:
    lines = read_lines(sql_path)
    updated = False

    common_marker_idx = next((i for i, line_text in enumerate(lines) if "[COMMON MODE]" in line_text), None)
    file_marker_idx = next((i for i, line_text in enumerate(lines) if "[FILE_ANNOTATION MODE]" in line_text), None)
    if common_marker_idx is None or file_marker_idx is None or common_marker_idx >= file_marker_idx:
        raise RuntimeError("Could not find expected mode markers in asset.Transformation.sql")

    common_sql_start = next(
        (i for i in range(common_marker_idx, file_marker_idx) if "with parentLookup as (" in lines[i]),
        None,
    )
    if common_sql_start is None:
        raise RuntimeError("Could not find COMMON MODE SQL block in asset.Transformation.sql")

    for i in range(common_sql_start, file_marker_idx):
        stripped = lines[i].strip()
        if not stripped or stripped.startswith("--"):
            continue
        indent = len(lines[i]) - len(lines[i].lstrip(" "))
        lines[i] = (" " * indent) + "-- " + lines[i].lstrip(" ")
        updated = True

    file_sql_start = next((i for i in range(file_marker_idx, len(lines)) if "with root as (" in lines[i]), None)
    if file_sql_start is None:
        raise RuntimeError("Could not find FILE_ANNOTATION MODE SQL block in asset.Transformation.sql")

    file_block_commented = lines[file_sql_start].lstrip().startswith("--")
    if file_block_commented:
        for i in range(file_sql_start, len(lines)):
            line = lines[i]
            stripped = line.lstrip()
            if stripped.startswith("-- "):
                prefix_len = len(line) - len(stripped)
                lines[i] = (" " * prefix_len) + stripped[3:]
                updated = True
            elif stripped.startswith("--"):
                prefix_len = len(line) - len(stripped)
                lines[i] = (" " * prefix_len) + stripped[2:]
                updated = True

    if updated:
        ensure_backup(sql_path)
        write_lines(sql_path, lines)
    return updated


def validate_env_var_name(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Z][A-Z0-9_]*", value))


def select_env(repo_root: Path, cli_env: Optional[str]) -> str:
    if cli_env:
        return cli_env
    return prompt_text("Select environment (dev/prod/staging)", default="dev").lower()


def _check_gitignore(repo_root: Path) -> None:
    gitignore = repo_root / ".gitignore"
    if gitignore.exists():
        content = gitignore.read_text(encoding="utf-8")
        if not any(
            line.strip() in {".env", "*.env"} for line in content.splitlines()
        ):
            print("Warning: .env does not appear to be listed in .gitignore — secrets may be committed accidentally.")


def main(cli_env: Optional[str] = None) -> int:
    repo_root = Path(__file__).resolve().parent.parent
    env = select_env(repo_root, cli_env).strip().lower()

    if env not in {"dev", "prod", "staging"}:
        print(f"Unsupported env: {env}. Use one of dev/prod/staging.")
        return 1

    config_path = repo_root / f"config.{env}.yaml"
    env_path = repo_root / ".env"
    sql_path = repo_root / "modules/sourcesystem/cdf_sap_assets/transformations/population/asset.Transformation.sql"

    if not config_path.exists():
        print(f"Missing config file: {config_path}")
        print("Tip: create it first (for staging, add config.staging.yaml) and rerun this script.")
        return 1
    if not sql_path.exists():
        print(f"Missing SQL file: {sql_path}")
        return 1

    print(f"Using config: {config_path.name}")
    _check_gitignore(repo_root)

    # Load files.
    config_lines = read_lines(config_path)
    env_lines, env_values, env_key_to_line = parse_env_file(env_path)

    # Prompt for basic inputs.
    project_name = quote_yaml_string(prompt_text("Enter CDF project name for this environment"))
    app_owner = quote_yaml_string(prompt_text("Enter ApplicationOwner email(s) for cdf_file_annotation"))

    # Group source strategy.
    print(
        "\nGroup source strategy: using one shared group is simpler."
        " If you answer 'no', you'll be asked for 9 separate group IDs."
    )
    use_same_group_everywhere = prompt_yes_no(
        "Use the same GROUP_SOURCE_ID for all modules?", default=True
    )

    group_env_by_target: Dict[Tuple[str, ...], str] = {}
    if use_same_group_everywhere:
        group_source_id = env_values.get("GROUP_SOURCE_ID", "").strip()
        if not group_source_id:
            group_source_id = prompt_text("Enter GROUP_SOURCE_ID")
            upsert_env_var(env_lines, env_key_to_line, "GROUP_SOURCE_ID", group_source_id)
            env_values["GROUP_SOURCE_ID"] = group_source_id

        for target in GROUP_TARGETS:
            group_env_by_target[target.path] = "GROUP_SOURCE_ID"
    else:
        for target in GROUP_TARGETS:
            env_var_name = target.default_env_var

            existing_val = env_values.get(env_var_name, "").strip()
            if existing_val:
                if prompt_yes_no(
                    f"{env_var_name} already exists for {target.label}. Overwrite value?",
                    default=False,
                ):
                    new_val = prompt_text(f"Enter value for {env_var_name}")
                    upsert_env_var(env_lines, env_key_to_line, env_var_name, new_val)
                    env_values[env_var_name] = new_val
            else:
                new_val = prompt_text(f"Enter value for {env_var_name}")
                upsert_env_var(env_lines, env_key_to_line, env_var_name, new_val)
                env_values[env_var_name] = new_val
            group_env_by_target[target.path] = env_var_name

    open_id_client_secret = env_values.get("OPEN_ID_CLIENT_SECRET", "").strip()
    if not open_id_client_secret:
        open_id_client_secret = prompt_text("Enter OPEN_ID_CLIENT_SECRET")
        upsert_env_var(env_lines, env_key_to_line, "OPEN_ID_CLIENT_SECRET", open_id_client_secret)
        env_values["OPEN_ID_CLIENT_SECRET"] = open_id_client_secret

    # Cron branch.
    cron_updates: Dict[int, str] = {}
    cron_lines = collect_cron_placeholder_lines(config_lines)
    if cron_lines:
        update_cron = prompt_yes_no(
            f"Found {len(cron_lines)} cron placeholders ({DEFAULT_CRON_PLACEHOLDER}). Update now?",
            default=False,
        )
        if update_cron:
            for line_idx, key in cron_lines:
                new_cron = prompt_text(
                    f"Cron for '{key}'",
                    default=DEFAULT_CRON_PLACEHOLDER,
                )
                cron_updates[line_idx] = quote_yaml_string(new_cron)

    # Apply config updates.
    ensure_backup(config_path)
    changed_paths: List[str] = []

    # Build the YAML path map once and reuse across all updates.
    key_line_map = build_yaml_paths(config_lines)

    if set_yaml_value_by_path(config_lines, ("environment", "project"), project_name, key_line_map):
        changed_paths.append("environment.project")
    else:
        print("Warning: Could not find environment.project")

    for path, value in ENTITY_MATCHING_UPDATES:
        if set_yaml_value_by_path(config_lines, path, value, key_line_map):
            changed_paths.append(".".join(path[-2:]))
        else:
            print(f"Warning: Could not find {'.'.join(path)}")

    if set_target_view_filter_values(config_lines, "root:ast_VAL", key_line_map):
        changed_paths.append("cdf_entity_matching.targetViewFilterValues")
    else:
        print("Warning: Could not update cdf_entity_matching.targetViewFilterValues")

    app_owner_path = (
        "variables",
        "modules",
        "accelerators",
        "contextualization",
        "cdf_file_annotation",
        "ApplicationOwner",
    )
    if set_yaml_value_by_path(config_lines, app_owner_path, app_owner, key_line_map):
        changed_paths.append("cdf_file_annotation.ApplicationOwner")
    else:
        print("Warning: Could not update cdf_file_annotation.ApplicationOwner")

    for target in GROUP_TARGETS:
        env_var = group_env_by_target[target.path]
        if set_yaml_value_by_path(config_lines, target.path, f"${{{env_var}}}", key_line_map):
            changed_paths.append(target.label)
        else:
            print(f"Warning: Could not update {target.label}")

    for line_idx, new_cron in cron_updates.items():
        config_lines[line_idx] = set_yaml_line_value(config_lines[line_idx], new_cron)
        changed_paths.append(f"cron:{line_idx + 1}")

    write_lines(config_path, config_lines)

    # Persist .env only when needed.
    if env_lines:
        ensure_backup(env_path) if env_path.exists() else None
        write_lines(env_path, env_lines)
    elif not env_path.exists():
        # Create .env if we added values from scratch.
        if env_values:
            write_lines(env_path, env_lines)

    # SQL mode switch.
    sql_changed = enable_file_annotation_mode(sql_path)

    print("\nDone.")
    print(f"- Updated: {config_path}")
    print(f"- Updated: {env_path}")
    print(f"- FILE_ANNOTATION mode {'enabled/verified' if sql_changed else 'already enabled'} in {sql_path}")
    if changed_paths:
        print(f"- Config fields touched: {len(changed_paths)}")
    print("- Backup files created with .bak suffix (first run).")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Interactive Quickstart DP setup wizard")
    parser.add_argument(
        "--env",
        choices=["dev", "prod", "staging"],
        help="Target environment for config.<env>.yaml",
    )
    _args = parser.parse_args()
    try:
        raise SystemExit(main(cli_env=_args.env))
    except KeyboardInterrupt:
        print("\nCancelled by user.")
        raise SystemExit(130)
