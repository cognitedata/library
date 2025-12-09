from __future__ import annotations

import atexit
import os
from enum import Enum
from pathlib import Path
from tempfile import gettempdir
from typing import Any

import dotenv
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials, Token
from msal import PublicClientApplication, SerializableTokenCache

dotenv.load_dotenv()


def get_local_client():

    CLIENT_ID = os.environ.get(f"IDP_CLIENT_ID")
    CDF_CLUSTER = os.environ.get(f"CDF_CLUSTER")
    COGNITE_PROJECT = os.environ.get(f"CDF_PROJECT")
    CLIENT_SECRET = os.environ.get(f"IDP_CLIENT_SECRET")
    TOKEN_URL = os.environ.get(f"IDP_TOKEN_URL")

    print(f"CLIENT_ID: {CLIENT_ID}")
    print(f"CDF_CLUSTER: {CDF_CLUSTER}")
    print(f"COGNITE_PROJECT: {COGNITE_PROJECT}")
    # print(f"CLIENT_SECRET: {CLIENT_SECRET}")
    print(f"TOKEN_URL: {TOKEN_URL}")

    base_url = f"https://{CDF_CLUSTER}.cognitedata.com"
    creds = OAuthClientCredentials(
        token_url=TOKEN_URL,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=[f"{base_url}/.default"],
    )

    cnf = ClientConfig(
        client_name=COGNITE_PROJECT,
        project=COGNITE_PROJECT,
        credentials=creds,
        base_url=base_url,
    )

    return CogniteClient(cnf)
