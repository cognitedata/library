# -*- coding: utf-8 -*-
"""
CDF Client Authentication for Local Script Execution.

Supports multiple authentication methods:
1. Environment variables (recommended for automation)
2. Interactive OAuth login (recommended for development)
3. API Key (legacy)

Usage:
    from client import get_client
    client = get_client()
    
Environment variables (compatible with cognite-toolkit .env):
    CDF_PROJECT         CDF project name (required)
    CDF_CLUSTER         CDF cluster (e.g., 'aws-dub-dev', 'westeurope-1')
    CDF_URL             Full CDF URL (optional, derived from cluster)
    IDP_CLIENT_ID       OAuth client ID
    IDP_CLIENT_SECRET   OAuth client secret
    IDP_TENANT_ID       Azure tenant ID
    IDP_TOKEN_URL       OAuth token URL (optional, derived from tenant)
    IDP_SCOPES          OAuth scopes (optional, derived from CDF URL)
"""

import os
import logging
from typing import Optional

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials, Token

logger = logging.getLogger(__name__)


def get_client(
    project: Optional[str] = None,
    cluster: Optional[str] = None,
) -> CogniteClient:
    """
    Get an authenticated CogniteClient.
    
    Authentication priority:
    1. CDF_TOKEN environment variable (direct token)
    2. OAuth Client Credentials (IDP_CLIENT_ID + IDP_CLIENT_SECRET)
    3. Interactive OAuth (opens browser)
    
    Environment variables (cognite-toolkit compatible):
    - CDF_PROJECT: CDF project name (required)
    - CDF_CLUSTER: CDF cluster (e.g., 'aws-dub-dev', 'westeurope-1')
    - CDF_URL: Full CDF URL (optional, derived from cluster)
    - CDF_TOKEN: Direct bearer token (optional)
    - IDP_CLIENT_ID: OAuth client ID
    - IDP_CLIENT_SECRET: OAuth client secret
    - IDP_TENANT_ID: Azure tenant ID
    - IDP_TOKEN_URL: OAuth token URL (optional, auto-detected)
    - IDP_SCOPES: OAuth scopes (optional, auto-detected)
    
    Args:
        project: Override project from environment
        cluster: Override cluster from environment
        
    Returns:
        Authenticated CogniteClient
    """
    # Get configuration from environment
    project = project or os.environ.get("CDF_PROJECT")
    cluster = cluster or os.environ.get("CDF_CLUSTER", "westeurope-1")
    
    if not project:
        raise ValueError(
            "CDF_PROJECT environment variable is required. "
            "Set it to your CDF project name."
        )
    
    # Determine base URL - use CDF_URL if provided, otherwise derive from cluster
    base_url = os.environ.get("CDF_URL")
    if not base_url:
        if cluster == "api":
            base_url = "https://api.cognitedata.com"
        else:
            base_url = f"https://{cluster}.cognitedata.com"
    
    # Try different authentication methods
    
    # Method 1: Direct token
    token = os.environ.get("CDF_TOKEN")
    if token:
        logger.info(f"Using direct token authentication for project: {project}")
        credentials = Token(token)
        config = ClientConfig(
            client_name="context-quality-local",
            project=project,
            base_url=base_url,
            credentials=credentials,
        )
        return CogniteClient(config)
    
    # Method 2: OAuth Client Credentials (cognite-toolkit compatible)
    client_id = os.environ.get("IDP_CLIENT_ID")
    client_secret = os.environ.get("IDP_CLIENT_SECRET")
    
    if client_id and client_secret:
        # Get token URL - use IDP_TOKEN_URL if provided, otherwise derive from tenant
        token_url = os.environ.get("IDP_TOKEN_URL")
        if not token_url:
            tenant_id = os.environ.get("IDP_TENANT_ID", "common")
            token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        
        # Get scopes - use IDP_SCOPES if provided, otherwise derive from base URL
        scopes_str = os.environ.get("IDP_SCOPES")
        if scopes_str:
            scopes = [scopes_str]
        else:
            scopes = [f"{base_url}/.default"]
        
        logger.info(f"Using OAuth client credentials for project: {project}")
        credentials = OAuthClientCredentials(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
            scopes=scopes,
        )
        config = ClientConfig(
            client_name="context-quality-local",
            project=project,
            base_url=base_url,
            credentials=credentials,
        )
        return CogniteClient(config)
    
    # Method 3: Try to use cognite-toolkit or interactive login
    try:
        # Try cognite-toolkit's built-in auth
        from cognite.client import global_config
        
        logger.info(f"Attempting interactive/toolkit authentication for project: {project}")
        
        # This will use any configured authentication from cognite-toolkit
        client = CogniteClient.default_oauth_client_credentials(
            project=project,
            cdf_cluster=cluster,
            client_name="context-quality-local",
        )
        return client
    except Exception as e:
        logger.warning(f"Interactive auth failed: {e}")
    
    raise ValueError(
        "No valid authentication found. Please set one of:\n"
        "  - CDF_TOKEN (direct bearer token)\n"
        "  - IDP_CLIENT_ID + IDP_CLIENT_SECRET (OAuth credentials)\n"
        "Or configure authentication via cognite-toolkit."
    )


def get_client_from_toolkit() -> CogniteClient:
    """
    Get CogniteClient using cognite-toolkit's configured credentials.
    
    This uses the cdf.toml configuration and any stored credentials.
    Useful when running from within a cognite-toolkit project.
    
    Returns:
        Authenticated CogniteClient
    """
    try:
        # Import toolkit's client builder
        from cognite_toolkit._cdf_tk.client import ToolkitClient
        
        toolkit_client = ToolkitClient()
        return toolkit_client.client
    except ImportError:
        logger.warning("cognite-toolkit not installed, falling back to standard auth")
        return get_client()
    except Exception as e:
        logger.warning(f"Toolkit auth failed: {e}, falling back to standard auth")
        return get_client()


if __name__ == "__main__":
    # Test authentication
    logging.basicConfig(level=logging.INFO)
    
    try:
        client = get_client()
        print(f"Successfully authenticated!")
        print(f"  Project: {client.config.project}")
        print(f"  Base URL: {client.config.base_url}")
        
        # Quick test
        status = client.iam.token.inspect()
        print(f"  Capabilities: {len(status.capabilities)} groups")
    except Exception as e:
        print(f"Authentication failed: {e}")
