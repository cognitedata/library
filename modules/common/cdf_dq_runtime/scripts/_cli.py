"""Shared CLI helpers for the Data Quality Toolkit module scripts."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tomllib
import urllib.parse
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.exceptions import CogniteAPIError

_PLACEHOLDER = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")


def find_toolkit_project_root(start: Path | None = None) -> Path | None:
    """Return the nearest ancestor directory that contains ``cdf.toml``."""
    current = (start or Path.cwd()).resolve()
    for directory in (current, *current.parents):
        if (directory / "cdf.toml").is_file():
            return directory
    return None


def resolve_env_file_path(env_file: Path | str | None = None) -> Path | None:
    """Resolve a Toolkit ``.env`` file path.

    Resolution order:
    1. Explicit ``--env-file`` path
    2. ``.env`` next to ``cdf.toml`` (walk up from cwd)
    3. Nearest ``.env`` walking up from cwd
    """
    if env_file is not None:
        path = Path(env_file).expanduser().resolve()
        return path if path.is_file() else None

    project_root = find_toolkit_project_root()
    if project_root is not None:
        candidate = project_root / ".env"
        if candidate.is_file():
            return candidate

    current = Path.cwd().resolve()
    for directory in (current, *current.parents):
        candidate = directory / ".env"
        if candidate.is_file():
            return candidate
    return None


def _parse_env_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None
    if stripped.startswith("export "):
        stripped = stripped.removeprefix("export ").lstrip()
    key, _, value = stripped.partition("=")
    key = key.strip()
    if not key:
        return None
    value = value.strip()
    if value.startswith(('"', "'")):
        quote = value[0]
        end_quote_idx = value.find(quote, 1)
        if end_quote_idx != -1:
            value = value[1:end_quote_idx]
    else:
        value = value.split("#", 1)[0].strip()
    return key, value


def load_project_env(env_file: Path | str | None = None) -> Path | None:
    """Load Toolkit ``.env`` values into ``os.environ`` without overriding existing keys."""
    path = resolve_env_file_path(env_file)
    if path is None:
        return None

    for line in path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_env_line(line)
        if parsed is None:
            continue
        key, value = parsed
        if key not in os.environ:
            os.environ[key] = value

    return path


def module_root() -> Path:
    """Return the Toolkit module directory that contains this scripts folder."""
    return Path(__file__).resolve().parents[1]


def _stringify_leaves(mapping: dict[str, object]) -> dict[str, str]:
    return {str(key): "" if value is None else str(value) for key, value in mapping.items()}


MODULE_CONFIG_PATH = ("modules", "common", "cdf_dq_runtime")


def module_toolkit_variables(
    config: dict[str, object], module_path: tuple[str, ...] = MODULE_CONFIG_PATH
) -> dict[str, str]:
    """Resolve Toolkit config.<env>.yaml variables for this module, like ``cdf build`` does.

    Leaf values on each level of ``variables`` along *module_path* are inherited, and deeper
    levels win. Variables of sibling modules are ignored, so generic names such as
    ``schemaSpace`` set for other modules do not leak into this one.
    """
    node = config.get("variables")
    resolved: dict[str, str] = {}
    for key in (None, *module_path):
        if key is not None:
            node = node.get(key) if isinstance(node, dict) else None
        if not isinstance(node, dict):
            break
        resolved.update(_stringify_leaves({k: v for k, v in node.items() if not isinstance(v, dict)}))
    return resolved


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
    dest_dir_resolved = dest_dir.resolve()
    unresolved: list[str] = []
    for src in sorted(source_dir.rglob("*")):
        # Skip output tree when dest_dir is inside source_dir (e.g. ./build).
        if src.resolve().is_relative_to(dest_dir_resolved):
            continue
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
    variables.update(module_toolkit_variables(loaded))
    return variables


@dataclass(frozen=True)
class RecordsConfig:
    """Records section from settings.yaml (space + stream for validation results)."""

    space: str
    stream_id: str


@dataclass(frozen=True)
class RuntimeSettings:
    """Typed subset of settings.yaml used by the deploy scripts."""

    records: RecordsConfig | None = None
    config_space: str | None = None
    config_source: str | None = None
    data_product_sync_cron: str | None = None

    @property
    def effective_config_space(self) -> str:
        if self.config_space:
            return self.config_space
        if self.records:
            return self.records.space
        raise ValueError("settings.yaml must set config_space or records.space")


def load_settings_raw(settings_path: Path) -> RuntimeSettings:
    """Load settings.yaml into a typed RuntimeSettings structure."""
    data = yaml.safe_load(settings_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a mapping in {settings_path}")
    records: RecordsConfig | None = None
    records_data = data.get("records")
    if isinstance(records_data, dict) and records_data.get("space") and records_data.get("stream_id"):
        records = RecordsConfig(
            space=str(records_data["space"]),
            stream_id=str(records_data["stream_id"]),
        )
    return RuntimeSettings(
        records=records,
        config_space=str(data["config_space"]) if data.get("config_space") else None,
        config_source=str(data["config_source"]) if data.get("config_source") else None,
        data_product_sync_cron=str(data["data_product_sync_cron"]) if data.get("data_product_sync_cron") else None,
    )


def data_quality_space(settings: RuntimeSettings) -> str:
    """Resolve DMS space for DataQualitySettings / HistoricJobQueue."""
    try:
        return settings.effective_config_space
    except ValueError as exc:
        raise ValueError("settings.yaml must set config_space or records.space for historic enqueue") from exc


def records_stream_id(settings: RuntimeSettings) -> str:
    """Resolve the records stream external ID from settings.yaml."""
    if settings.records:
        return settings.records.stream_id
    raise ValueError("settings.yaml must set records.stream_id")


def _streams_capability_error(stream_id: str, action: str, exc: CogniteAPIError) -> PermissionError:
    return PermissionError(
        f"Cannot {action} records stream '{stream_id}': deploy identity lacks Streams capabilities. "
        f"Grant streamsAcl:READ and streamsAcl:CREATE (scope all) on the group for the "
        f"credentials in .env / IDP_* used by deploy_infrastructure.py. Original error: {exc}"
    )


# Capability requirements from README Access control (install vs function runtime).
INSTALL_CAPABILITIES: dict[str, frozenset[str]] = {
    "dataProductsAcl": frozenset({"CREATE", "READ", "UPDATE", "DELETE"}),
    "ruleSetsAcl": frozenset({"CREATE", "READ", "UPDATE", "DELETE"}),
    "streamsAcl": frozenset({"READ", "CREATE", "DELETE"}),
}

RUNTIME_CAPABILITIES: dict[str, frozenset[str]] = {
    "streamsAcl": frozenset({"READ"}),
    "streamRecordsAcl": frozenset({"READ", "WRITE"}),
}


@dataclass(frozen=True)
class AccessCheckReport:
    """Result of comparing token capabilities to README Access control requirements."""

    install_missing: list[str]
    runtime_missing: list[str]
    runtime_checked: bool
    same_identity: bool

    @property
    def ok(self) -> bool:
        if self.install_missing:
            return False
        return not (self.runtime_checked and self.runtime_missing)


def collect_granted_actions(capabilities_dump: list[dict[str, object]]) -> dict[str, set[str]]:
    """Build ACL name → granted actions from a ``token.inspect`` capabilities dump."""
    granted: dict[str, set[str]] = {}
    for item in capabilities_dump:
        if not isinstance(item, dict):
            continue
        for key, value in item.items():
            if key in {"projectScope", "project_scope"}:
                continue
            if not isinstance(value, dict):
                continue
            actions = value.get("actions") or []
            if not isinstance(actions, list):
                continue
            granted.setdefault(str(key), set()).update(str(action).upper() for action in actions)
    return granted


def missing_capabilities(
    granted: dict[str, set[str]],
    required: dict[str, frozenset[str]],
) -> list[str]:
    """Return sorted ``acl:ACTION`` entries that are required but not granted."""
    missing: list[str] = []
    for acl, actions in required.items():
        have = granted.get(acl, set())
        for action in sorted(actions):
            if action not in have:
                missing.append(f"{acl}:{action}")
    return missing


def inspect_granted_actions(client: CogniteClient) -> dict[str, set[str]]:
    """Return ACL → actions for the current token via ``iam.token.inspect``."""
    inspection = client.iam.token.inspect()
    dump = inspection.capabilities.dump()
    if not isinstance(dump, list):
        raise TypeError("token.inspect capabilities.dump() did not return a list")
    return collect_granted_actions(dump)


def create_client_from_credentials(
    client_id: str,
    client_secret: str,
    project: str | None = None,
    base_url: str | None = None,
) -> CogniteClient:
    """Create a Cognite client using explicit client credentials and env project settings."""
    from cognite.client import ClientConfig, CogniteClient
    from cognite.client.credentials import OAuthClientCredentials

    cluster = os.environ.get("CDF_CLUSTER") or os.environ.get("COGNITE_CLUSTER", "api")
    project = project or os.environ.get("CDF_PROJECT") or os.environ.get("COGNITE_PROJECT")
    base_url = (
        base_url
        or os.environ.get("CDF_URL")
        or os.environ.get("COGNITE_BASE_URL")
        or f"https://{cluster}.cognitedata.com"
    )
    token_url = os.environ.get("IDP_TOKEN_URL") or os.environ.get("COGNITE_TOKEN_URL")
    tenant_id = os.environ.get("IDP_TENANT_ID") or os.environ.get("AZURE_TENANT_ID")

    if not project:
        raise ValueError("Missing CDF_PROJECT/COGNITE_PROJECT for function-identity access check")
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
        client_name="dq-toolkit-access-check",
        project=project,
        credentials=credentials,
        base_url=base_url,
    )
    return CogniteClient(config)


def _deploy_client_id() -> str | None:
    return os.environ.get("IDP_CLIENT_ID") or os.environ.get("COGNITE_CLIENT_ID")


def verify_dq_access(
    install_client: CogniteClient,
    function_secrets: dict[str, str] | None,
    *,
    runtime_client: CogniteClient | None = None,
    deploy_client_id: str | None = None,
) -> AccessCheckReport:
    """Check install and function-runtime identities against README Access control.

    Args:
        install_client: Client used by ``deploy_infrastructure.py`` (``.env`` / ``IDP_*``).
        function_secrets: Function secrets dict with ``client-id`` / ``client-secret``.
        runtime_client: Optional pre-built client for the function identity (tests).
        deploy_client_id: Optional deploy client id for same-identity detection (tests).

    Returns:
        AccessCheckReport with missing ``acl:ACTION`` entries per role.
    """
    install_granted = inspect_granted_actions(install_client)
    install_missing = missing_capabilities(install_granted, INSTALL_CAPABILITIES)

    deploy_id = deploy_client_id if deploy_client_id is not None else _deploy_client_id()
    runtime_id = function_secrets.get("client-id") if function_secrets else None
    runtime_secret = function_secrets.get("client-secret") if function_secrets else None
    same_identity = bool(runtime_id and deploy_id and runtime_id == deploy_id)

    if not runtime_id or not runtime_secret:
        return AccessCheckReport(
            install_missing=install_missing,
            runtime_missing=[],
            runtime_checked=False,
            same_identity=False,
        )

    if same_identity and runtime_client is None:
        runtime_granted = install_granted
    elif runtime_client is not None:
        runtime_granted = inspect_granted_actions(runtime_client)
    else:
        runtime_granted = inspect_granted_actions(
            create_client_from_credentials(
                runtime_id,
                runtime_secret,
                project=install_client.config.project,
                base_url=install_client.config.base_url,
            )
        )

    runtime_missing = missing_capabilities(runtime_granted, RUNTIME_CAPABILITIES)
    return AccessCheckReport(
        install_missing=install_missing,
        runtime_missing=runtime_missing,
        runtime_checked=True,
        same_identity=same_identity,
    )


def print_access_check_report(report: AccessCheckReport) -> None:
    """Print a human-readable access check summary to stdout/stderr."""
    print("\nAccess check (README Access control)")
    if report.install_missing:
        print("  Install identity: MISSING", file=sys.stderr)
        for item in report.install_missing:
            print(f"    - {item}", file=sys.stderr)
    else:
        print("  Install identity: OK")

    if not report.runtime_checked:
        print(
            "  Function runtime identity: SKIPPED (no client-id/client-secret function secrets)",
            file=sys.stderr,
        )
        return

    same = " (same as install)" if report.same_identity else ""
    if report.runtime_missing:
        print(f"  Function runtime identity{same}: MISSING", file=sys.stderr)
        for item in report.runtime_missing:
            print(f"    - {item}", file=sys.stderr)
    else:
        print(f"  Function runtime identity{same}: OK")


def ensure_records_stream(
    client: CogniteClient,
    stream_id: str,
    *,
    dry_run: bool = False,
    template: str = "BasicArchive",
) -> str:
    """Ensure the records stream exists; create an immutable archive stream if missing.

    Uses the Streams REST API (``GET/POST /streams``) so it works with cognite-sdk
    versions that do not yet expose ``client.data_modeling.streams``.

    Args:
        client: Authenticated Cognite client.
        stream_id: Records stream external ID (``records.stream_id`` / ``recordsStreamId``).
        dry_run: When True, do not create; return ``would_create`` if missing.
        template: Stream creation template. ``BasicArchive`` is immutable (append-only).

    Returns:
        ``exists``, ``created``, or ``would_create``.

    Raises:
        PermissionError: Deploy identity is missing ``streamsAcl:READ`` / ``CREATE``.
    """
    from cognite.client.exceptions import CogniteAPIError

    if not stream_id.strip():
        raise ValueError("records stream_id must be a non-empty string")

    project = client.config.project
    encoded_stream_id = urllib.parse.quote(stream_id, safe="")
    stream_path = f"/api/v1/projects/{project}/streams/{encoded_stream_id}"
    try:
        client.get(stream_path)
        return "exists"
    except CogniteAPIError as exc:
        if exc.code == 403:
            raise _streams_capability_error(stream_id, "read", exc) from exc
        if exc.code != 404:
            raise

    if dry_run:
        return "would_create"

    try:
        client.post(
            f"/api/v1/projects/{project}/streams",
            json={
                "items": [
                    {
                        "externalId": stream_id,
                        "settings": {"template": {"name": template}},
                    }
                ]
            },
        )
    except CogniteAPIError as exc:
        if exc.code == 409:
            return "exists"
        if exc.code == 403:
            raise _streams_capability_error(stream_id, "create", exc) from exc
        raise
    return "created"


def function_secrets_from_toml(toml_path: Path, section: str = "cognite") -> dict[str, str] | None:
    """Build function secret dict from a Toolkit config.toml credentials file."""
    if not toml_path.is_file():
        return None
    try:
        with toml_path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError:
        return None
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


def create_client_from_env() -> CogniteClient:
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
    base_url = os.environ.get("CDF_URL") or os.environ.get("COGNITE_BASE_URL") or f"https://{cluster}.cognitedata.com"

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


def resolve_cognite_client(config_toml: Path | str) -> CogniteClient:
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


def configure_deploy_cli() -> None:
    """Reduce noisy third-party warnings for operator-facing deploy scripts."""
    warnings.filterwarnings(
        "ignore",
        message=".*httpx module is deprecated.*",
        category=DeprecationWarning,
    )
    warnings.filterwarnings("ignore", module=r"authlib\..*", category=DeprecationWarning)
    try:
        from cognite.client.config import global_config

        global_config.disable_pypi_version_check = True
    except ImportError:
        # cognite-sdk is only required when a deploy script creates a client.
        pass


def print_deploy_context(
    *,
    project: str | None,
    package_version: str,
    credentials_source: str | None,
    toolkit_config: Path | str | None,
    data_product_sync_cron: str | None = None,
) -> None:
    """Print a compact pre-deploy context block."""
    print("Data quality deploy")
    if project:
        print(f"  Project:     {project}")
    print(f"  Package:     cognite-data-quality=={package_version}")
    if credentials_source:
        print(f"  Credentials: {credentials_source}")
    if toolkit_config:
        print(f"  Toolkit:     {toolkit_config}")
    if data_product_sync_cron:
        print(f"  DP sync:     cron {data_product_sync_cron}")


def _workflow_status_counts(workflows: list[object]) -> tuple[int, int]:
    deployed = 0
    skipped = 0
    for item in workflows:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status", "")).lower()
        if status == "deployed":
            deployed += 1
        elif status == "skipped":
            skipped += 1
    return deployed, skipped


def _print_workflow_group(label: str, workflows: list[object]) -> None:
    if not workflows:
        return
    deployed, skipped = _workflow_status_counts(workflows)
    print(f"\n{label}: {len(workflows)}")
    print(f"  deployed: {deployed}")
    if skipped:
        print(f"  skipped:  {skipped}")
    for item in workflows:
        if not isinstance(item, dict):
            continue
        workflow_id = item.get("workflow_external_id") or item.get("workflow")
        trigger = item.get("trigger")
        status = str(item.get("status", "unknown")).lower()
        if workflow_id:
            line = f"  - {workflow_id} ({status})"
            if trigger:
                line += f", trigger {trigger}"
            print(line)


def print_infrastructure_summary(results: dict[str, object], *, dry_run: bool = False) -> None:
    """Print a human-readable deployment summary."""
    print(f"\n{'=' * 60}")
    print("DEPLOYMENT SUMMARY")
    print(f"{'=' * 60}")

    function_results = results.get("functions", {})
    if isinstance(function_results, dict):
        func_result = function_results.get("function", {})
        if isinstance(func_result, dict):
            name = func_result.get("function", "N/A")
            status = str(func_result.get("status", "unknown")).lower()
            print(f"\nFunction: {name} ({status})")

    external = results.get("external_dataproduct_workflows", [])
    if isinstance(external, list) and external:
        print("\nExternal DataProducts:")
        for item in external:
            if not isinstance(item, dict):
                continue
            dp_name = item.get("dataProduct", "unknown")
            dp_version = item.get("version", "?")
            status = str(item.get("status", "unknown")).lower()
            views = item.get("views")
            views_suffix = f", {views} views" if views is not None else ""
            print(f"  - {dp_name} @ {dp_version} ({status}{views_suffix})")
            nested = item.get("workflows", [])
            if isinstance(nested, list):
                for workflow in nested:
                    if not isinstance(workflow, dict):
                        continue
                    for key, label in (
                        ("workflow_external_id", "sync-cursor"),
                        ("uniqueness_workflow_external_id", "uniqueness"),
                        ("edge_existence_workflow_external_id", "edge-existence"),
                    ):
                        workflow_id = workflow.get(key)
                        if workflow_id:
                            print(f"      {label}: {workflow_id}")

    for label, key in (
        ("Instance workflows", "workflows"),
        ("Time series workflows", "timeseries_workflows"),
        ("Rule engine sync", "rule_engine_result_sync_workflows"),
        ("RAW table workflows", "raw_tables"),
    ):
        workflows = results.get(key, [])
        if isinstance(workflows, list):
            _print_workflow_group(label, workflows)

    _print_workflow_group("data_product_sync", results.get("data_product_sync", []))
    _print_workflow_group("historic_queue_manager", results.get("historic_queue_manager", []))

    historic_enqueue = results.get("historic_enqueue")
    if isinstance(historic_enqueue, dict):
        print(
            f"\nHistoric enqueue: {historic_enqueue.get('total_enqueued', 0)} enqueued, "
            f"{historic_enqueue.get('total_skipped', 0)} skipped"
        )

    if dry_run:
        print("\n[DRY RUN] No changes were made")


def print_verbose_results(results: dict[str, object]) -> None:
    """Print full deployment results as formatted JSON."""
    print(json.dumps(results, indent=2, default=str))


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add flags shared by deploy_infrastructure.py and deploy_pipeline.py."""
    parser.add_argument(
        "--env-file",
        default=None,
        help=(
            "Optional .env file with CDF credentials (default: .env next to cdf.toml, "
            "or nearest .env walking up from cwd). Existing environment variables win."
        ),
    )
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
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print materialized pack paths and full JSON results",
    )


def resolve_materialized_pack(args: argparse.Namespace, dest_dir: Path) -> MaterializedPack:
    """Materialize pack YAML and return settings, views, and timeseries directories."""
    if args.settings_path:
        settings_path = Path(args.settings_path)
        views_dir = Path(args.views_dir) if args.views_dir else None
        timeseries_dir = Path(args.timeseries_dir) if args.timeseries_dir else None
        if timeseries_dir is None:
            candidate = settings_path.parent / "timeseries_config"
            timeseries_dir = candidate if candidate.is_dir() else None
        if views_dir is None:
            candidate = settings_path.parent / "views"
            views_dir = candidate if candidate.is_dir() else None
        return MaterializedPack(settings_path, views_dir, timeseries_dir, {})

    variables = load_variables(Path(args.toolkit_config) if args.toolkit_config else None)
    materialize_pack_yaml(module_root(), dest_dir, variables)
    views = dest_dir / "views"
    timeseries = dest_dir / "timeseries_config"
    return MaterializedPack(
        settings_path=dest_dir / "settings.yaml",
        views_dir=views if views.is_dir() else None,
        timeseries_dir=timeseries if timeseries.is_dir() else None,
        variables=variables,
    )


def resolve_settings_and_views(args: argparse.Namespace, dest_dir: Path) -> tuple[Path, Path | None, dict[str, str]]:
    """Backward-compatible wrapper around :func:`resolve_materialized_pack`."""
    pack = resolve_materialized_pack(args, dest_dir)
    return pack.settings_path, pack.views_dir, pack.variables
