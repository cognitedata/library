"""Create CogniteClient from .env credentials."""

from __future__ import annotations

import os


def create_cognite_client(client_name: str = "inverted-index-contextualization"):
    """Create a CogniteClient using API key or OAuth credentials from env."""
    from cognite.client import ClientConfig, CogniteClient
    from cognite.client.credentials import OAuthClientCredentials, Token

    project = (
        os.getenv("COGNITE_PROJECT") or os.getenv("PROJECT") or os.getenv("CDF_PROJECT")
    )
    base_url = (
        os.getenv("COGNITE_BASE_URL")
        or os.getenv("BASE_URL")
        or os.getenv("CDF_BASE_URL")
        or os.getenv("CDF_URL")
    )
    api_key = (
        os.getenv("COGNITE_API_KEY") or os.getenv("API_KEY") or os.getenv("CDF_API_KEY")
    )

    if api_key:
        if not (project and base_url):
            cluster = os.getenv("CDF_CLUSTER")
            if cluster:
                base_url = f"https://{cluster}.cognitedata.com"
        config = ClientConfig(
            client_name=client_name,
            project=project,
            base_url=base_url,
            credentials=Token(api_key),
        )
        return CogniteClient(config=config)

    tenant_id = (
        os.getenv("COGNITE_TENANT_ID")
        or os.getenv("TENANT_ID")
        or os.getenv("IDP_TENANT_ID")
    )
    client_id = (
        os.getenv("COGNITE_CLIENT_ID")
        or os.getenv("CLIENT_ID")
        or os.getenv("IDP_CLIENT_ID")
    )
    client_secret = (
        os.getenv("COGNITE_CLIENT_SECRET")
        or os.getenv("CLIENT_SECRET")
        or os.getenv("IDP_CLIENT_SECRET")
    )
    token_url = (
        os.getenv("COGNITE_TOKEN_URL")
        or os.getenv("TOKEN_URL")
        or os.getenv("IDP_TOKEN_URL")
    )
    scopes_str = (
        os.getenv("COGNITE_SCOPES")
        or os.getenv("SCOPES")
        or os.getenv("IDP_SCOPES")
        or ""
    )
    scopes = [s for s in scopes_str.split(" ") if s]

    if not base_url:
        cluster = os.getenv("CDF_CLUSTER")
        if cluster:
            base_url = f"https://{cluster}.cognitedata.com"

    if not scopes and base_url:
        scopes = [f"{base_url.rstrip('/')}/.default"]

    if not (project and base_url and tenant_id and client_id and client_secret and token_url and scopes):
        raise RuntimeError(
            "Missing CDF credentials in .env: set COGNITE_API_KEY or OAuth "
            "(CDF_PROJECT, CDF_CLUSTER or CDF_URL, IDP_CLIENT_ID, IDP_CLIENT_SECRET, "
            "IDP_TENANT_ID, IDP_TOKEN_URL, IDP_SCOPES)"
        )

    credentials = OAuthClientCredentials(
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes,
    )
    config = ClientConfig(
        client_name=client_name,
        project=project,
        base_url=base_url,
        credentials=credentials,
    )
    return CogniteClient(config=config)


def auth_mode_from_env() -> str:
    if os.getenv("COGNITE_API_KEY") or os.getenv("API_KEY") or os.getenv("CDF_API_KEY"):
        return "api_key"
    return "oauth"
