import os

from pathlib import Path
from dotenv import load_dotenv
from typing import Any, Tuple, Literal
from cognite.client import CogniteClient, ClientConfig, global_config
from cognite.client.credentials import OAuthClientCredentials
from utils.DataStructures import EnvConfig

from services.ConfigService import Config, load_config_parameters
from services.LoggerService import CogniteFunctionLogger
from services.EntitySearchService import EntitySearchService
from services.CacheService import CacheService


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
    TOKEN_URL = f"https://login.microsoftonline.com/{env_config.tenant_id}/oauth2/v2.0/token"
    creds = OAuthClientCredentials(
        token_url=TOKEN_URL,
        client_id=env_config.client_id,
        client_secret=env_config.client_secret,
        scopes=SCOPES,
    )
    settings = {
        "disable_ssl": True,
    }
    global_config.apply_settings(settings)
    cnf = ClientConfig(
        client_name="DEV_Working",
        project=env_config.cdf_project,
        base_url=f"https://{env_config.cdf_cluster}.cognitedata.com",
        credentials=creds,
        debug=debug,
    )
    client = CogniteClient(cnf)
    return client


def create_logger_service(log_level, filepath: str | None):
    if filepath:
        write = True
    else:
        write = False
    if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
        return CogniteFunctionLogger()
    else:
        return CogniteFunctionLogger(log_level=log_level, write=write, filepath=filepath)


def create_config_service(
    function_data: dict[str, Any], client: CogniteClient | None = None
) -> Tuple[Config, CogniteClient]:
    if not client:
        env_config = get_env_variables()
        client = create_client(env_config)
    config = load_config_parameters(client=client, function_data=function_data)
    return config, client


def create_entity_search_service(
    config: Config, client: CogniteClient, logger: CogniteFunctionLogger
) -> EntitySearchService:
    """Creates an EntitySearchService instance for finding entities by text."""
    # Get required configuration
    core_annotation_view = config.data_model_views.core_annotation_view
    file_view = config.data_model_views.file_view
    target_entities_view = config.data_model_views.target_entities_view
    regular_annotation_space = file_view.instance_space

    if not regular_annotation_space:
        raise ValueError("regular_annotation_space (file_view.instance_space) is required but was None")

    return EntitySearchService(
        client=client,
        logger=logger,
        core_annotation_view_id=core_annotation_view.as_view_id(),
        file_view_id=file_view.as_view_id(),
        target_entities_view_id=target_entities_view.as_view_id(),
        regular_annotation_space=regular_annotation_space,
    )


def create_cache_service(
    config: Config, client: CogniteClient, logger: CogniteFunctionLogger, entity_search_service: EntitySearchService
) -> CacheService:
    """Creates a CacheService instance for caching text→entity mappings."""
    raw_db = config.finalize_function.apply_service.raw_db
    file_view = config.data_model_views.file_view
    target_entities_view = config.data_model_views.target_entities_view

    return CacheService(
        client=client,
        logger=logger,
        raw_db=raw_db,
        normalize_fn=entity_search_service.normalize,  # Reuse normalization from entity search
        file_view_id=file_view.as_view_id(),
        target_entities_view_id=target_entities_view.as_view_id(),
        cache_table_name="promote_text_to_entity_cache",
    )
