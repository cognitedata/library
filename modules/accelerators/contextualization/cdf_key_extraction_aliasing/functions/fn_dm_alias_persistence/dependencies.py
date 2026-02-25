import os
from pathlib import Path
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from dotenv import load_dotenv

from .common.logger import CogniteFunctionLogger

# Import EnvConfig from key extraction utils (shared data structure)
try:
    from ..fn_dm_key_extraction.utils.DataStructures import EnvConfig
except ImportError:
    # Fallback: define EnvConfig locally if key extraction not available
    from dataclasses import dataclass

    @dataclass
    class EnvConfig:
        """Data structure holding the configs to connect to CDF client locally"""

        cdf_project: str
        cdf_cluster: str
        tenant_id: str
        client_id: str
        client_secret: str


def get_env_variables() -> EnvConfig:
    print("Loading environment variables from .env...")

    project_path = (Path(__file__).parent / ".env").resolve()
    print(f"project_path is set to: {project_path}")

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
    if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
        return CogniteFunctionLogger("INFO", verbose)
    else:
        return CogniteFunctionLogger(log_level, verbose)


def create_write_logger_service(log_level, verbose, filepath):
    # Note: aliasing logger doesn't support write/filepath, so this is a placeholder
    # that matches the interface but uses standard logger
    if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
        return CogniteFunctionLogger("INFO", verbose)
    else:
        return CogniteFunctionLogger(log_level, verbose)


try:
    from ..fn_dm_key_extraction.services.PipelineService import GeneralPipelineService

    def create_general_pipeline_service(
        client: CogniteClient, pipeline_ext_id: str
    ) -> GeneralPipelineService:
        return GeneralPipelineService(pipeline_ext_id, client)

except ImportError:
    # Pipeline service not available, define a placeholder
    GeneralPipelineService = None

    def create_general_pipeline_service(client: CogniteClient, pipeline_ext_id: str):
        raise ImportError(
            "GeneralPipelineService not available. Install key extraction dependencies."
        )
