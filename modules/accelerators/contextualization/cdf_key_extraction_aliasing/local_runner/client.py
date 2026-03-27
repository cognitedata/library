"""Create CogniteClient from environment (API key or OAuth)."""

import os


def create_cognite_client():
    """Create a CogniteClient using either API key or OAuth credentials from env."""
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
        credentials = Token(api_key)
        config = ClientConfig(
            client_name="key-extraction-aliasing",
            project=project,
            base_url=base_url,
            credentials=credentials,
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

    if not (tenant_id and client_id and client_secret and token_url and scopes):
        raise RuntimeError(
            "Missing CDF credentials: provide COGNITE_API_KEY or OAuth client credentials in .env"
        )

    credentials = OAuthClientCredentials(
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes,
    )
    config = ClientConfig(
        client_name="key-extraction-aliasing",
        project=project,
        base_url=base_url,
        credentials=credentials,
    )
    return CogniteClient(config=config)
