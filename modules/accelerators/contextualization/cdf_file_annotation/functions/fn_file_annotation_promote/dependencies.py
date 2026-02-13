import os
from pathlib import Path
from typing import Any, Literal, Tuple, cast

from cognite.client import ClientConfig, CogniteClient, global_config
from cognite.client.credentials import OAuthClientCredentials
from dotenv import load_dotenv
from services.CacheService import CacheService
from services.ConfigService import Config, load_config_parameters
from services.EntitySearchService import EntitySearchService
from services.LoggerService import CogniteFunctionLogger
from utils.DataStructures import EnvConfig


def get_env_variables() -> EnvConfig:
    """
    Loads environment variables required for CDF authentication from .env file.

    Required environment variables:
    - CDF_PROJECT: CDF project name
    - CDF_CLUSTER: CDF cluster (e.g., westeurope-1)
    - IDP_TENANT_ID: Azure AD tenant ID
    - IDP_CLIENT_ID: Azure AD application client ID
    - IDP_CLIENT_SECRET: Azure AD application client secret

    Returns:
        EnvConfig object containing all required environment variables

    Raises:
        ValueError: If any required environment variables are missing
    """
    print("Loading environment variables from .env...")

    project_path: Path = (Path(__file__).parent / ".env").resolve()
    print(f"project_path is set to: {project_path}")

    load_dotenv()

    required_envvars: tuple[str, ...] = (
        "CDF_PROJECT",
        "CDF_CLUSTER",
        "IDP_TENANT_ID",
        "IDP_CLIENT_ID",
        "IDP_CLIENT_SECRET",
    )

    missing: list[str] = [envvar for envvar in required_envvars if envvar not in os.environ]
    if missing:
        raise ValueError(f"Missing one or more env.vars: {missing}")

    return EnvConfig(
        cdf_project=os.getenv("CDF_PROJECT"),  # type: ignore
        cdf_cluster=os.getenv("CDF_CLUSTER"),  # type: ignore
        tenant_id=os.getenv("IDP_TENANT_ID"),  # type: ignore
        client_id=os.getenv("IDP_CLIENT_ID"),  # type: ignore
        client_secret=os.getenv("IDP_CLIENT_SECRET"),  # type: ignore
    )


def create_client(env_config: EnvConfig, debug: bool = False) -> CogniteClient:
    """
    Creates an authenticated CogniteClient using OAuth client credentials flow.

    Args:
        env_config: Environment configuration containing CDF connection details
        debug: Whether to enable debug mode on the client (default: False)

    Returns:
        Authenticated CogniteClient instance
    """
    SCOPES: list[str] = [f"https://{env_config.cdf_cluster}.cognitedata.com/.default"]
    TOKEN_URL: str = f"https://login.microsoftonline.com/{env_config.tenant_id}/oauth2/v2.0/token"
    creds: OAuthClientCredentials = OAuthClientCredentials(
        token_url=TOKEN_URL,
        client_id=env_config.client_id,
        client_secret=env_config.client_secret,
        scopes=SCOPES,
    )
    settings: dict[str, bool] = {
        "disable_ssl": True,
    }
    global_config.apply_settings(settings)
    cnf: ClientConfig = ClientConfig(
        client_name="DEV_Working",
        project=env_config.cdf_project,
        base_url=f"https://{env_config.cdf_cluster}.cognitedata.com",
        credentials=creds,
        debug=debug,
    )
    client: CogniteClient = CogniteClient(cnf)
    return client


def create_logger_service(log_level: str, filepath: str | None) -> CogniteFunctionLogger:
    """
    Creates a logger service for tracking function execution.

    Args:
        log_level: Logging level ("DEBUG", "INFO", "WARNING", "ERROR")
        filepath: Optional file path for writing logs to disk

    Returns:
        CogniteFunctionLogger instance configured with specified settings
    """
    valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR"}
    level = cast(Literal["DEBUG", "INFO", "WARNING", "ERROR"], log_level) if log_level in valid_levels else "INFO"
    write = filepath is not None
    return CogniteFunctionLogger(log_level=level, write=write, filepath=filepath)


def create_config_service(
    function_data: dict[str, Any], client: CogniteClient | None = None
) -> Tuple[Config, CogniteClient]:
    """
    Creates configuration service and CogniteClient for the function.

    Loads configuration from CDF based on the ExtractionPipelineExtId provided in function_data.
    If no client is provided, creates one using environment variables.

    Args:
        function_data: Dictionary containing function input data (must include ExtractionPipelineExtId)
        client: Optional pre-initialized CogniteClient (if None, creates new client)

    Returns:
        Tuple of (Config, CogniteClient)
    """
    if not client:
        env_config: EnvConfig = get_env_variables()
        client = create_client(env_config)
    config: Config = load_config_parameters(client=client, function_data=function_data)
    return config, client


def create_entity_search_service(
    config: Config, client: CogniteClient, logger: CogniteFunctionLogger
) -> EntitySearchService:
    """
    Creates an EntitySearchService instance for finding entities by text.

    Factory function that initializes EntitySearchService with configuration.

    Args:
        config: Configuration object containing data model views and entity search settings
        client: CogniteClient for API interactions
        logger: Logger instance for tracking execution

    Returns:
        Initialized EntitySearchService instance

    Raises:
        ValueError: If regular_annotation_space (file_view.instance_space) is None
    """
    return EntitySearchService(config=config, client=client, logger=logger)


def create_cache_service(
    config: Config, client: CogniteClient, logger: CogniteFunctionLogger, entity_search_service: EntitySearchService
) -> CacheService:
    """
    Creates a CacheService instance for caching text→entity mappings.

    Factory function that initializes CacheService with configuration.
    Importantly, reuses the normalize() function from EntitySearchService to ensure
    consistent text normalization between caching and searching.

    Args:
        config: Configuration object containing RAW database settings and data model views
        client: CogniteClient for API interactions
        logger: Logger instance for tracking execution
        entity_search_service: EntitySearchService instance (to reuse normalize function)

    Returns:
        Initialized CacheService instance
    """
    return CacheService(
        config=config,
        client=client,
        logger=logger,
        normalize_fn=entity_search_service.normalize,  # Reuse normalization from entity search
    )
