import os
from dataclasses import dataclass

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from dotenv import load_dotenv

from ..cdf_fn_common.function_logging import cognite_function_logger


@dataclass
class EnvConfig:
    """Configs used to connect to a CDF project locally."""

    cdf_project: str
    cdf_cluster: str
    tenant_id: str
    client_id: str
    client_secret: str


def get_env_variables() -> EnvConfig:
    """Load required CDF connection environment variables (local runs)."""
    print("Loading environment variables from .env...")

    load_dotenv()

    required_envvars = (
        "CDF_PROJECT",
        "CDF_CLUSTER",
        "IDP_TENANT_ID",
        "IDP_CLIENT_ID",
        "IDP_CLIENT_SECRET",
    )

    missing = [envvar for envvar in required_envvars if envvar not in os.environ]
    if missing:
        raise ValueError(f"Missing one or more env.vars: {missing}")

    return EnvConfig(
        cdf_project=os.getenv("CDF_PROJECT"),  # type: ignore
        cdf_cluster=os.getenv("CDF_CLUSTER"),  # type: ignore
        tenant_id=os.getenv("IDP_TENANT_ID"),  # type: ignore
        client_id=os.getenv("IDP_CLIENT_ID"),  # type: ignore
        client_secret=os.getenv("IDP_CLIENT_SECRET"),  # type: ignore
    )


def create_client(env_config: EnvConfig, debug: bool = False):
    """Create a `CogniteClient` using OAuth client credentials (local runs)."""
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
        client_name="AliasPersistence_Client",
        project=env_config.cdf_project,
        base_url=f"https://p001.plink.{env_config.cdf_cluster}.cognitedata.com",  # NOTE: base_url might need to be adjusted if on PSAAS or Private Link
        credentials=creds,
        debug=debug,
    )
    client = CogniteClient(cnf)
    return client


def create_logger_service(log_level, verbose):
    """Create a `CogniteFunctionLogger` with the requested verbosity."""
    return cognite_function_logger(str(log_level), bool(verbose))


def create_write_logger_service(log_level, verbose, filepath):
    """Create a logger service that matches the handler interface (local runs)."""
    # Note: aliasing logger doesn't support write/filepath, so this is a placeholder
    # that matches the interface but uses standard logger
    return cognite_function_logger(str(log_level), bool(verbose))
