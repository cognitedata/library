"""Shared CLI helpers for the Data Quality Toolkit module scripts."""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

_PLACEHOLDER = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")


def module_root() -> Path:
    """Return the Toolkit module directory that contains this scripts folder."""
    return Path(__file__).resolve().parents[1]


def _stringify_leaves(mapping: dict[str, object]) -> dict[str, str]:
    return {str(key): "" if value is None else str(value) for key, value in mapping.items()}


def flatten_toolkit_variables(config: dict[str, object]) -> dict[str, str]:
    """Flatten Toolkit config.<env>.yaml variables to {{ key }} names."""
    variables = config.get("variables")
    if not isinstance(variables, dict):
        return {}

    flattened: dict[str, str] = {}

    def walk(node: object) -> None:
        if not isinstance(node, dict):
            return
        leaf_values = {key: value for key, value in node.items() if not isinstance(value, dict)}
        nested = {key: value for key, value in node.items() if isinstance(value, dict)}
        if leaf_values and not nested:
            flattened.update(_stringify_leaves(leaf_values))
            return
        if nested:
            for child in nested.values():
                walk(child)
        if leaf_values:
            flattened.update(_stringify_leaves(leaf_values))

    top_level = {key: value for key, value in variables.items() if key != "modules"}
    walk(top_level)

    modules = variables.get("modules")
    if isinstance(modules, dict):
        walk(modules)
    elif modules is not None:
        flattened["modules"] = str(modules)

    return flattened


def load_default_pack_variables(module_dir: Path) -> dict[str, str]:
    """Load placeholder defaults from the module default.config.yaml."""
    path = module_dir / "default.config.yaml"
    if not path.is_file():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a mapping in {path}")
    return _stringify_leaves(data)


def substitute_placeholders(text: str, variables: dict[str, str]) -> str:
    """Replace ``{{ name }}`` tokens. Unknown names are left unchanged."""

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in variables:
            return variables[name]
        return match.group(0)

    return _PLACEHOLDER.sub(replace, text)


def unresolved_placeholders(text: str) -> list[str]:
    """Return unique placeholder names still present in *text*."""
    return sorted(set(_PLACEHOLDER.findall(text)))


def materialize_pack_yaml(
    source_dir: Path,
    dest_dir: Path,
    variables: dict[str, str],
    *,
    require_resolved: bool = True,
) -> Path:
    """Copy pack YAML into dest_dir with Toolkit placeholders substituted."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    unresolved: list[str] = []
    for src in sorted(source_dir.rglob("*")):
        if not src.is_file():
            continue
        if src.suffix.lower() not in {".yaml", ".yml", ".ttl"}:
            continue
        rel = src.relative_to(source_dir)
        if rel.parts and rel.parts[0] == "scripts":
            continue
        dest = dest_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        rendered = substitute_placeholders(src.read_text(encoding="utf-8"), variables)
        leftover = unresolved_placeholders(rendered)
        if leftover:
            unresolved.extend(f"{rel}: {name}" for name in leftover)
        dest.write_text(rendered, encoding="utf-8")
    if require_resolved and unresolved:
        details = "; ".join(unresolved)
        raise ValueError(f"Unresolved Toolkit placeholders after materialize: {details}")
    return dest_dir


def load_variables(toolkit_config: Path | None) -> dict[str, str]:
    """Merge default.config.yaml with an optional Toolkit config.<env>.yaml."""
    variables = load_default_pack_variables(module_root())
    if toolkit_config is None:
        return variables
    config_path = Path(toolkit_config)
    if not config_path.is_file():
        raise FileNotFoundError(f"Toolkit config not found: {config_path}")
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Expected a mapping in {config_path}")
    variables.update(flatten_toolkit_variables(loaded))
    return variables


def load_settings_raw(settings_path: Path) -> dict[str, object]:
    """Load settings.yaml as a plain dict."""
    data = yaml.safe_load(settings_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a mapping in {settings_path}")
    return data


def data_quality_space(settings_raw: dict[str, object]) -> str:
    """Resolve DMS space for DataQualitySettings / HistoricJobQueue."""
    config_space = settings_raw.get("config_space")
    if config_space:
        return str(config_space)
    records = settings_raw.get("records") or {}
    if isinstance(records, dict) and records.get("space"):
        return str(records["space"])
    raise ValueError("settings.yaml must set config_space or records.space for historic enqueue")


def function_secrets_from_toml(toml_path: Path, section: str = "cognite") -> dict[str, str] | None:
    """Build function secret dict from a Toolkit config.toml credentials file."""
    try:
        import toml
    except ImportError:  # pragma: no cover
        import tomli as toml  # type: ignore[no-redef]

    if not toml_path.is_file():
        return None
    data = toml.load(toml_path)
    cog = data.get(section, data) if section else data
    if not isinstance(cog, dict):
        return None
    client_id = cog.get("client_id")
    client_secret = cog.get("client_secret")
    if client_id and client_secret:
        return {"client-id": str(client_id), "client-secret": str(client_secret)}
    return None


def resolve_function_secrets(config_toml: Path) -> dict[str, str] | None:
    """Resolve orchestrator function secrets from TOML, then environment variables."""
    secrets = function_secrets_from_toml(config_toml)
    if secrets:
        return secrets
    client_id = os.getenv("COGNITE_CLIENT_ID") or os.getenv("IDP_CLIENT_ID")
    client_secret = os.getenv("COGNITE_CLIENT_SECRET") or os.getenv("IDP_CLIENT_SECRET")
    if client_id and client_secret:
        return {"client-id": client_id, "client-secret": client_secret}
    return None


def create_client_from_env():
    """Create a Cognite client from environment variables.

    Matches ``data-quality-validation-deploy/scripts/shared.py:create_client`` and
    supports Toolkit CI naming (``CDF_*`` / ``IDP_*``) as well as legacy ``COGNITE_*``.
    """
    from cognite.client import ClientConfig, CogniteClient
    from cognite.client.credentials import OAuthClientCredentials

    cluster = os.environ.get("CDF_CLUSTER") or os.environ.get("COGNITE_CLUSTER", "api")
    project = os.environ.get("CDF_PROJECT") or os.environ.get("COGNITE_PROJECT")
    client_id = os.environ.get("IDP_CLIENT_ID") or os.environ.get("COGNITE_CLIENT_ID")
    client_secret = os.environ.get("IDP_CLIENT_SECRET") or os.environ.get("COGNITE_CLIENT_SECRET")
    base_url = (
        os.environ.get("CDF_URL")
        or os.environ.get("COGNITE_BASE_URL")
        or f"https://{cluster}.cognitedata.com"
    )

    token_url = os.environ.get("IDP_TOKEN_URL") or os.environ.get("COGNITE_TOKEN_URL")
    tenant_id = os.environ.get("IDP_TENANT_ID") or os.environ.get("AZURE_TENANT_ID")

    if not all([project, client_id, client_secret]):
        raise ValueError(
            "Missing credentials in environment. Set CDF_PROJECT/COGNITE_PROJECT, "
            "IDP_CLIENT_ID/COGNITE_CLIENT_ID, and IDP_CLIENT_SECRET/COGNITE_CLIENT_SECRET."
        )

    if token_url:
        oauth_url = token_url
    elif tenant_id:
        oauth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    else:
        raise ValueError("Missing IDP_TOKEN_URL, COGNITE_TOKEN_URL, or IDP_TENANT_ID/AZURE_TENANT_ID")

    scopes_env = os.environ.get("IDP_SCOPES")
    if scopes_env:
        scopes = [scope.strip() for scope in scopes_env.split() if scope.strip()]
    else:
        scopes = [f"https://{cluster}.cognitedata.com/.default"]

    credentials = OAuthClientCredentials(
        token_url=oauth_url,
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes,
    )
    config = ClientConfig(
        client_name="dq-toolkit-runtime",
        project=project,
        credentials=credentials,
        base_url=base_url,
    )
    return CogniteClient(config)


def resolve_cognite_client(config_toml: Path | str):
    """Load client from optional TOML, otherwise from environment (deploy-repo / CI default)."""
    path = Path(config_toml)
    if path.is_file():
        from cognite_data_quality import load_cognite_client_from_toml

        return load_cognite_client_from_toml(path)
    return create_client_from_env()


@dataclass(frozen=True)
class MaterializedPack:
    """Paths to materialized module YAML used by deploy scripts."""

    settings_path: Path
    views_dir: Path | None
    timeseries_dir: Path | None
    variables: dict[str, str]


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add flags shared by deploy_infrastructure.py and deploy_pipeline.py."""
    parser.add_argument(
        "--config-toml",
        default="config.toml",
        help=(
            "Optional local TOML credentials file. When missing, uses environment variables "
            "(COGNITE_* / IDP_* / CDF_* — same as data-quality-validation-deploy and Toolkit CI)"
        ),
    )
    parser.add_argument(
        "--toolkit-config",
        default=None,
        help="Optional Toolkit config.<env>.yaml used to override default.config.yaml placeholders",
    )
    parser.add_argument(
        "--settings-path",
        default=None,
        help="Override settings.yaml (already substituted). Skips pack materialize when set.",
    )
    parser.add_argument(
        "--views-dir",
        default=None,
        help="Override views directory (already substituted). Used with --settings-path.",
    )
    parser.add_argument(
        "--timeseries-dir",
        default=None,
        help="Override timeseries directory (already substituted). Used with --settings-path.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview CDF writes without applying them")
    parser.add_argument("--force", action="store_true", help="Force redeployment of function and workflows")
    parser.add_argument("--force-function", action="store_true", help="Force function redeploy only")
    parser.add_argument("--force-workflows", action="store_true", help="Force workflow redeploy only")


def resolve_materialized_pack(args: argparse.Namespace, dest_dir: Path) -> MaterializedPack:
    """Materialize pack YAML and return settings, views, and timeseries directories."""
    if args.settings_path:
        settings_path = Path(args.settings_path)
        views_dir = Path(args.views_dir) if args.views_dir else None
        timeseries_dir = Path(args.timeseries_dir) if args.timeseries_dir else None
        if timeseries_dir is None:
            candidate = settings_path.parent / "timeseries"
            timeseries_dir = candidate if candidate.is_dir() else None
        if views_dir is None:
            candidate = settings_path.parent / "views"
            views_dir = candidate if candidate.is_dir() else None
        return MaterializedPack(settings_path, views_dir, timeseries_dir, {})

    variables = load_variables(Path(args.toolkit_config) if args.toolkit_config else None)
    materialize_pack_yaml(module_root(), dest_dir, variables)
    views = dest_dir / "views"
    timeseries = dest_dir / "timeseries"
    return MaterializedPack(
        settings_path=dest_dir / "settings.yaml",
        views_dir=views if views.is_dir() else None,
        timeseries_dir=timeseries if timeseries.is_dir() else None,
        variables=variables,
    )


def resolve_settings_and_views(
    args: argparse.Namespace, dest_dir: Path
) -> tuple[Path, Path | None, dict[str, str]]:
    """Backward-compatible wrapper around :func:`resolve_materialized_pack`."""
    pack = resolve_materialized_pack(args, dest_dir)
    return pack.settings_path, pack.views_dir, pack.variables
