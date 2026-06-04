#!/usr/bin/env python3
"""Generate config.{dev,test,prod}.yaml for a Toolkit project using dp:foundation modules."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    logging.error("PyYAML is required: pip install pyyaml")
    sys.exit(1)

ENVIRONMENTS = ("dev", "test", "prod")


def resolve_modules_root(repo_root: Path, org_dir: str) -> Path:
    """Toolkit modules/ at repo root, or nested under the organization directory."""
    for candidate in (repo_root / "modules", repo_root / org_dir / "modules"):
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError(
        f"No modules directory found under {repo_root / 'modules'} "
        f"or {repo_root / org_dir / 'modules'}"
    )


def module_variable_key(module_path: str) -> str:
    return Path(module_path).name


def discover_foundation_module_paths(modules_root: Path, repo_root: Path | None = None) -> list[str]:
    """Resolve deployable dp:foundation module paths from packages.toml or module.toml scan."""
    root = repo_root or modules_root.parent
    packages_toml = root / "modules" / "packages.toml"
    if packages_toml.is_file():
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib  # type: ignore[no-redef]
        data = tomllib.loads(packages_toml.read_text(encoding="utf-8"))
        foundation = data.get("packages", {}).get("foundation", {})
        listed = foundation.get("modules") or []
        if listed:
            return list(listed)

    paths: list[str] = []
    for module_toml in sorted(modules_root.rglob("module.toml")):
        text = module_toml.read_text(encoding="utf-8")
        if 'package_id = "dp:foundation"' not in text:
            continue
        rel = module_toml.parent.relative_to(modules_root)
        paths.append(rel.as_posix())
    return paths


def load_default_config(modules_root: Path, module_path: str) -> dict[str, Any]:
    config_path = modules_root / module_path / "default.config.yaml"
    if not config_path.is_file():
        return {}
    try:
        with config_path.open(encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Failed to parse YAML in {config_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in {config_path}")
    return data


def selected_module_entries(module_paths: list[str]) -> list[str]:
    return [f"modules/{path}" for path in module_paths]


def build_config(
    env_name: str,
    enterprise: str,
    module_paths: list[str],
    modules_root: Path,
) -> dict[str, Any]:
    variables_modules: dict[str, dict[str, Any]] = {}
    flat_variables: dict[str, Any] = {}
    for module_path in module_paths:
        defaults = load_default_config(modules_root, module_path)
        if defaults:
            variables_modules[module_variable_key(module_path)] = defaults
            for key, val in defaults.items():
                if key in flat_variables and flat_variables[key] != val:
                    logging.warning(
                        "Conflict for variable '%s': %r is overwritten by %r from '%s'",
                        key,
                        flat_variables[key],
                        val,
                        module_path,
                    )
                flat_variables[key] = val

    return {
        "environment": {
            "name": env_name,
            "project": f"{enterprise}-{env_name}",
            "validation-type": "dev",
            "selected": selected_module_entries(module_paths),
        },
        "variables": {**flat_variables, "modules": variables_modules},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--enterprise", required=True, help="Enterprise slug, e.g. acme")
    parser.add_argument(
        "--org-dir",
        required=True,
        help="Toolkit organization directory (relative to repo root)",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root containing modules/ and org dir",
    )
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    org_dir = repo_root / args.org_dir
    modules_root = resolve_modules_root(repo_root, args.org_dir)

    module_paths = discover_foundation_module_paths(modules_root, repo_root)
    if not module_paths:
        print("No dp:foundation modules found under modules/", file=sys.stderr)
        sys.exit(1)

    org_dir.mkdir(parents=True, exist_ok=True)
    scripts_dir = org_dir / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    for env_name in ENVIRONMENTS:
        out_path = org_dir / f"config.{env_name}.yaml"
        payload = build_config(env_name, args.enterprise, module_paths, modules_root)
        header = (
            "# Generated by cdf_project_foundation/scripts/generate_env_configs.py\n"
            "# Re-run generate_actions.py or this script to refresh.\n"
        )
        with out_path.open("w", encoding="utf-8") as handle:
            handle.write(header)
            yaml.safe_dump(
                payload,
                handle,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )
        print(f"Wrote {out_path.relative_to(repo_root)}")


if __name__ == "__main__":
    main()
