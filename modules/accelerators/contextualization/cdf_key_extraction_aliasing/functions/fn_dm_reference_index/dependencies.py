import os
from dataclasses import dataclass

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from dotenv import load_dotenv

from ..cdf_fn_common.function_logging import cognite_function_logger


@dataclass
class EnvConfig:
    cdf_project: str
    cdf_cluster: str
    tenant_id: str
    client_id: str
    client_secret: str


def get_env_variables() -> EnvConfig:
    load_dotenv()
    required = (
        "CDF_PROJECT",
        "CDF_CLUSTER",
        "IDP_TENANT_ID",
        "IDP_CLIENT_ID",
        "IDP_CLIENT_SECRET",
    )
    missing = [e for e in required if e not in os.environ]
    if missing:
        raise ValueError(f"Missing env vars: {missing}")
    return EnvConfig(
        cdf_project=os.getenv("CDF_PROJECT"),  # type: ignore[arg-type]
        cdf_cluster=os.getenv("CDF_CLUSTER"),  # type: ignore[arg-type]
        tenant_id=os.getenv("IDP_TENANT_ID"),  # type: ignore[arg-type]
        client_id=os.getenv("IDP_CLIENT_ID"),  # type: ignore[arg-type]
        client_secret=os.getenv("IDP_CLIENT_SECRET"),  # type: ignore[arg-type]
    )


def create_client(env_config: EnvConfig, debug: bool = False) -> CogniteClient:
    SCOPES = [f"https://{env_config.cdf_cluster}.cognitedata.com/.default"]
    TOKEN_URL = (
        f"https://login.microsoftonline.com/{env_config.tenant_id}/oauth2/v2.0/token"
    )
    creds = OAuthClientCredentials(
        token_url=TOKEN_URL,
        client_id=env_config.client_id,
        client_secret=env_config.client_secret,
        scopes=SCOPES,
    )
    cnf = ClientConfig(
        client_name="ReferenceIndex_Client",
        project=env_config.cdf_project,
        base_url=f"https://p001.plink.{env_config.cdf_cluster}.cognitedata.com",
        credentials=creds,
        debug=debug,
    )
    return CogniteClient(cnf)


def create_logger_service(log_level: str, verbose: bool):
    return cognite_function_logger(str(log_level), bool(verbose))

