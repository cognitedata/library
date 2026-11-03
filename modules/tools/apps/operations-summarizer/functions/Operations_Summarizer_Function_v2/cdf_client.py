"""
Local development helper — creates a CogniteClient from environment variables.

Token auth:
    CDF_PROJECT  – CDF project name
    CDF_URL      – cluster URL (e.g. "https://<cluster>.cognitedata.com")
    CDF_TOKEN    – bearer token (e.g. after `cdf auth login` with LOGIN_FLOW=token)

OAuth client-credentials auth:
    CDF_PROJECT       – CDF project name
    CDF_URL           – cluster URL (or set CDF_CLUSTER instead)
    IDP_TOKEN_URL     – OAuth token endpoint (or IDP_TENANT_ID to derive Entra URL)
    IDP_CLIENT_ID     – OAuth application (client) ID
    IDP_CLIENT_SECRET – OAuth client secret

Usage:
    from cdf_client import get_local_client
    client = get_local_client()
"""

import os
import sys

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials, Token


def _resolve_base_url() -> str:
    cdf_url = os.environ.get("CDF_URL", "").strip()
    if cdf_url:
        return cdf_url.rstrip("/")

    cluster = os.environ.get("CDF_CLUSTER", "").strip()
    if cluster:
        return f"https://{cluster}.cognitedata.com"

    return ""


def _resolve_token_url() -> str:
    token_url = os.environ.get("IDP_TOKEN_URL", "").strip()
    if token_url:
        return token_url

    tenant_id = os.environ.get("IDP_TENANT_ID", "").strip()
    if tenant_id:
        return f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    return ""


def get_local_client() -> CogniteClient:
    project = os.environ.get("CDF_PROJECT", "").strip()
    base_url = _resolve_base_url()

    for name, val in [("CDF_PROJECT", project), ("CDF_URL or CDF_CLUSTER", base_url)]:
        if not val:
            print(f"Missing required env var: {name}")
            sys.exit(1)

    static_token = os.environ.get("CDF_TOKEN", "").strip()
    if static_token:
        creds = Token(static_token)
    else:
        token_url = _resolve_token_url()
        client_id = os.environ.get("IDP_CLIENT_ID", "").strip()
        client_secret = os.environ.get("IDP_CLIENT_SECRET", "").strip()

        missing = [
            name
            for name, val in [
                ("IDP_TOKEN_URL or IDP_TENANT_ID", token_url),
                ("IDP_CLIENT_ID", client_id),
                ("IDP_CLIENT_SECRET", client_secret),
            ]
            if not val
        ]
        if missing:
            print(f"Missing env vars: {', '.join(missing)}")
            print(
                "Set CDF_TOKEN for token auth, or set IDP_CLIENT_ID, "
                "IDP_CLIENT_SECRET, and IDP_TOKEN_URL (or IDP_TENANT_ID)."
            )
            sys.exit(1)

        scopes = [f"{base_url}/.default"]
        creds = OAuthClientCredentials(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
            scopes=scopes,
        )

    config = ClientConfig(
        client_name="OperationsSummarizerFunction-local",
        project=project,
        base_url=base_url,
        credentials=creds,
    )

    return CogniteClient(config)
