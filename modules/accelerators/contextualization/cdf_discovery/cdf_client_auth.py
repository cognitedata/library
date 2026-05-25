"""Create CogniteClient from environment (API key, token, OAuth, or interactive OAuth)."""

from __future__ import annotations

import os
import sys
from typing import Any
from urllib.parse import urlparse


def _env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value and str(value).strip():
            return str(value).strip()
    return None


def _resolve_project() -> str | None:
    return _env("COGNITE_PROJECT", "CDF_PROJECT", "PROJECT")


def _resolve_base_url() -> str | None:
    base_url = _env("COGNITE_BASE_URL", "BASE_URL", "CDF_BASE_URL", "CDF_URL")
    if not base_url:
        cluster = _env("CDF_CLUSTER", "COGNITE_CLUSTER")
        if cluster:
            base_url = f"https://{cluster}.cognitedata.com"
    if base_url and not base_url.startswith("http"):
        base_url = f"https://{base_url}.cognitedata.com"
    return base_url


def _cluster_from_base_url(base_url: str) -> str | None:
    host = (urlparse(base_url).hostname or "").strip()
    if host.endswith(".cognitedata.com"):
        return host[: -len(".cognitedata.com")]
    return None


def _resolve_login_flow() -> str:
    return (_env("LOGIN_FLOW") or "").lower()


def _resolve_scopes(base_url: str) -> list[str]:
    scopes_str = _env("COGNITE_SCOPES", "SCOPES", "IDP_SCOPES") or ""
    scopes = [s for s in scopes_str.split() if s]
    if scopes:
        return scopes
    return [f"{base_url.rstrip('/')}/.default"]


def _interactive_oauth_available() -> bool:
    if _resolve_login_flow() in ("client_credentials", "token", "api_key", "device_code"):
        return False
    if _env("COGNITE_API_KEY", "API_KEY", "CDF_API_KEY"):
        return False
    if _env("COGNITE_TOKEN", "CDF_TOKEN"):
        return False
    tenant_id = _env("COGNITE_TENANT_ID", "TENANT_ID", "IDP_TENANT_ID")
    client_id = _env("COGNITE_CLIENT_ID", "CLIENT_ID", "IDP_CLIENT_ID")
    project = _resolve_project()
    base_url = _resolve_base_url()
    if not all([tenant_id, client_id, project, base_url]):
        return False
    client_secret = _env("COGNITE_CLIENT_SECRET", "CLIENT_SECRET", "IDP_CLIENT_SECRET")
    token_url = _env("COGNITE_TOKEN_URL", "TOKEN_URL", "IDP_TOKEN_URL")
    flow = _resolve_login_flow()
    if flow == "interactive":
        return True
    if client_secret and token_url:
        return False
    return True


def auth_mode_from_env() -> str:
    """Return ``api_key``, ``token``, ``oauth``, or ``interactive`` without exposing secrets."""
    if _env("COGNITE_API_KEY", "API_KEY", "CDF_API_KEY"):
        return "api_key"
    if _env("COGNITE_TOKEN", "CDF_TOKEN"):
        return "token"
    if _interactive_oauth_available():
        return "interactive"
    return "oauth"


def _client_config(
    *,
    client_name: str,
    project: str,
    base_url: str,
    credentials: Any,
) -> Any:
    from cognite.client import ClientConfig

    return ClientConfig(
        client_name=client_name,
        project=project,
        base_url=base_url,
        credentials=credentials,
    )


def _create_interactive_client(*, client_name: str) -> Any:
    from cognite.client import CogniteClient
    from cognite.client.credentials import OAuthInteractive

    tenant_id = _env("COGNITE_TENANT_ID", "TENANT_ID", "IDP_TENANT_ID")
    client_id = _env("COGNITE_CLIENT_ID", "CLIENT_ID", "IDP_CLIENT_ID")
    project = _resolve_project()
    base_url = _resolve_base_url()
    if not all([tenant_id, client_id, project, base_url]):
        raise RuntimeError(
            "Interactive login requires CDF_PROJECT, CDF_CLUSTER (or CDF_URL), "
            "IDP_TENANT_ID, and IDP_CLIENT_ID in the environment or repo-root .env"
        )

    cluster = _env("CDF_CLUSTER", "COGNITE_CLUSTER") or _cluster_from_base_url(base_url)
    redirect_port_raw = _env("IDP_REDIRECT_PORT", "COGNITE_REDIRECT_PORT")
    redirect_port = int(redirect_port_raw) if redirect_port_raw else 53000

    if cluster:
        credentials = OAuthInteractive.default_for_azure_ad(
            tenant_id=str(tenant_id),
            client_id=str(client_id),
            cdf_cluster=str(cluster),
            redirect_port=redirect_port,
        )
    else:
        credentials = OAuthInteractive(
            authority_url=f"https://login.microsoftonline.com/{tenant_id}",
            client_id=str(client_id),
            scopes=_resolve_scopes(base_url),
            redirect_port=redirect_port,
        )

    print(
        "CDF credentials not found for client-credentials auth; opening browser for interactive sign-in…",
        file=sys.stderr,
    )
    config = _client_config(
        client_name=client_name,
        project=str(project),
        base_url=str(base_url),
        credentials=credentials,
    )
    client = CogniteClient(config=config)
    client.iam.token.inspect()
    return client


def create_cognite_client(*, client_name: str = "cdf-discovery") -> Any:
    """Create a CogniteClient using env credentials, or interactive OAuth when configured."""
    from cognite.client import CogniteClient
    from cognite.client.credentials import OAuthClientCredentials, Token

    project = _resolve_project()
    base_url = _resolve_base_url()
    api_key = _env("COGNITE_API_KEY", "API_KEY", "CDF_API_KEY")

    if api_key:
        if not (project and base_url):
            raise RuntimeError(
                "API key auth requires COGNITE_PROJECT and CDF_CLUSTER (or COGNITE_BASE_URL)"
            )
        config = _client_config(
            client_name=client_name,
            project=project,
            base_url=base_url,
            credentials=Token(api_key),
        )
        return CogniteClient(config=config)

    bearer = _env("COGNITE_TOKEN", "CDF_TOKEN")
    if bearer:
        if not (project and base_url):
            raise RuntimeError(
                "Token auth requires COGNITE_PROJECT and CDF_CLUSTER (or COGNITE_BASE_URL)"
            )
        config = _client_config(
            client_name=client_name,
            project=project,
            base_url=base_url,
            credentials=Token(bearer),
        )
        return CogniteClient(config=config)

    if _interactive_oauth_available():
        return _create_interactive_client(client_name=client_name)

    tenant_id = _env("COGNITE_TENANT_ID", "TENANT_ID", "IDP_TENANT_ID")
    client_id = _env("COGNITE_CLIENT_ID", "CLIENT_ID", "IDP_CLIENT_ID")
    client_secret = _env("COGNITE_CLIENT_SECRET", "CLIENT_SECRET", "IDP_CLIENT_SECRET")
    token_url = _env("COGNITE_TOKEN_URL", "TOKEN_URL", "IDP_TOKEN_URL")
    scopes = _resolve_scopes(base_url) if base_url else []

    if not (tenant_id and client_id and client_secret and token_url and scopes and project and base_url):
        raise RuntimeError(
            "Missing CDF credentials: set COGNITE_API_KEY, CDF_TOKEN, OAuth client credentials, "
            "or toolkit-style interactive vars (CDF_PROJECT, CDF_CLUSTER, IDP_TENANT_ID, "
            "IDP_CLIENT_ID, LOGIN_FLOW=interactive) in repo-root .env"
        )

    credentials = OAuthClientCredentials(
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes,
    )
    config = _client_config(
        client_name=client_name,
        project=project,
        base_url=base_url,
        credentials=credentials,
    )
    return CogniteClient(config=config)
