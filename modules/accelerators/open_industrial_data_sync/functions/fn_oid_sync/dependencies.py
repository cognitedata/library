import os
from typing import Tuple

from cognite.client import ClientConfig, CogniteClient, global_config
from cognite.client.credentials import OAuthClientCredentials
from dotenv import load_dotenv
from services.ConfigService import load_config_parameters
from services.DataSyncService import DataSyncService, OIDPublicClient
from services.LoggerService import CompactLogger
from utils.DataStructures import EnvConfig, OIDConfig


def get_env_variables() -> EnvConfig:
    """Load environment variables"""
    print("Loading environment variables...")
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
        raise ValueError(f"Missing environment variables: {missing}")
    
    # Load OID secret - check both possible names
    oid_secret = os.getenv("OPEN_ID_CLIENT_SECRET") or os.getenv("openIdClientSecret")
    
    return EnvConfig(
        cdf_project=os.getenv("CDF_PROJECT"),  # type: ignore
        cdf_cluster=os.getenv("CDF_CLUSTER"),  # type: ignore
        tenant_id=os.getenv("IDP_TENANT_ID"),  # type: ignore
        client_id=os.getenv("IDP_CLIENT_ID"),  # type: ignore
        client_secret=os.getenv("IDP_CLIENT_SECRET"),  # type: ignore
        open_id_client_secret=oid_secret  # type: ignore
    )


def create_client(env_config: EnvConfig, debug: bool = False) -> CogniteClient:
    """Create authenticated CogniteClient for target project"""
    settings = {
        "max_retries": 5,
        "disable_ssl": True,
    }
    global_config.apply_settings(settings)
    
    scopes = [f"https://{env_config.cdf_cluster}.cognitedata.com/.default"]
    token_url = f"https://login.microsoftonline.com/{env_config.tenant_id}/oauth2/v2.0/token"
    
    creds = OAuthClientCredentials(
        token_url=token_url,
        client_id=env_config.client_id,
        client_secret=env_config.client_secret,
        scopes=scopes,
    )
    
    cnf = ClientConfig(
        client_name="OID-Sync",
        project=env_config.cdf_project,
        base_url=f"https://{env_config.cdf_cluster}.cognitedata.com",
        credentials=creds,
        debug=debug,
        timeout=60,
    )
    
    return CogniteClient(cnf)


def _get_oid_secret(secrets: dict | None, env_config: EnvConfig | None) -> str:
    """Resolve OID secret from available sources"""
    if secrets and (secret := secrets.get("oid-secret")):
        return secret
    if env_config and env_config.open_id_client_secret:
        return env_config.open_id_client_secret
    if secret := (os.getenv("OPEN_ID_CLIENT_SECRET") or os.getenv("openIdClientSecret")):
        return secret
    raise ValueError("Missing OPEN_ID_CLIENT_SECRET - required for OID access")


def create_config_and_client(
    client: CogniteClient | None = None, secrets: dict | None = None
) -> Tuple[OIDConfig, CogniteClient]:
    """Create configuration and client
    
    Args:
        client: Optional pre-created client for target CDF project
        secrets: Optional secrets dict containing openIdClientSecret
        
    Returns:
        Tuple of (OIDConfig, CogniteClient)
    """
    # Get environment config and create client if not provided
    env_config = None
    if client is None:
        env_config = get_env_variables()
        client = create_client(env_config)
    
    oid_secret = _get_oid_secret(client, secrets, env_config)
    config = load_config_parameters(client=client, oid_client_secret=oid_secret)
    
    return config, client


def create_logger(config: OIDConfig) -> CompactLogger:
    """Create logger service"""
    return CompactLogger(name="OID-Sync", log_level=config.log_level)


def create_oid_client(config: OIDConfig, logger: CompactLogger) -> OIDPublicClient:
    """Create Open Industrial Data client"""
    return OIDPublicClient(config=config, logger=logger)


def create_data_sync_service(
    target_client: CogniteClient,
    oid_client: OIDPublicClient,
    config: OIDConfig,
    logger: CompactLogger
) -> DataSyncService:
    """Create data synchronization service"""
    return DataSyncService(
        target_client=target_client,
        oid_client=oid_client,
        config=config,
        logger=logger
    )

