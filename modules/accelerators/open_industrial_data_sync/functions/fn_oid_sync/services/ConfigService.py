import yaml
from cognite.client import CogniteClient
from utils.DataStructures import OIDConfig

_DEFAULTS = {
    "instance_space": "springfield_instances",
    "oid_tenant_id": "48d5043c-cf70-4c49-881c-c638f5796997",
    "oid_client_id": "1b90ede3-271e-401b-81a0-a4d52bea3273",
    "oid_cluster": "api",
    "oid_project": "publicdata",
    "sync_realtime_start": "15m-ago",
    "sync_random_start": "12w-ago",
    "sync_end": "168h-ago",
    "time_offset_weeks": 1,
    "time_offset_minutes": 10,
    "log_level": "INFO",
    "reset_backfill": False,
    "timeseries_view_space": "sp_enterprise_process_industry",
    "timeseries_view_external_id": "Enterprise_TimeSeries",
    "timeseries_view_version": "v1",
}


def _fetch_pipeline_config(client: CogniteClient) -> dict:
    """Fetch and parse extraction pipeline config."""
    try:
        pipeline_config = client.extraction_pipelines.config.retrieve("ep_oid_sync")
        if pipeline_config and pipeline_config.config:
            data = yaml.safe_load(pipeline_config.config)
            if isinstance(data, dict):
                return data
        print("Warning: Extraction pipeline config not found or invalid, using defaults")
    except Exception as e:
        print(f"Error loading extraction pipeline config: {e}")
    return {}


def _build_config(config_data: dict, oid_client_secret: str) -> OIDConfig:
    """Build OIDConfig from parsed data with defaults."""
    oid = config_data.get("oid_connection", {})
    sync = config_data.get("sync_configuration", {})
    offset = config_data.get("time_offset", {})
    log = config_data.get("logging", {})

    return OIDConfig(
        instance_space=sync.get("instance_space", _DEFAULTS["instance_space"]),
        oid_tenant_id=oid.get("tenant_id", _DEFAULTS["oid_tenant_id"]),
        oid_client_id=oid.get("client_id", _DEFAULTS["oid_client_id"]),
        oid_client_secret=oid_client_secret,
        oid_cluster=oid.get("cluster", _DEFAULTS["oid_cluster"]),
        oid_project=oid.get("project", _DEFAULTS["oid_project"]),
        sync_realtime_start=sync.get("sync_realtime_start", _DEFAULTS["sync_realtime_start"]),
        sync_random_start=sync.get("sync_random_start", _DEFAULTS["sync_random_start"]),
        sync_end=sync.get("sync_all_end", _DEFAULTS["sync_end"]),
        time_offset_weeks=offset.get("offset_weeks", _DEFAULTS["time_offset_weeks"]),
        time_offset_minutes=offset.get("offset_minutes", _DEFAULTS["time_offset_minutes"]),
        log_level=log.get("log_level", _DEFAULTS["log_level"]),
        reset_backfill=sync.get("reset_backfill", _DEFAULTS["reset_backfill"]),
        timeseries_view_space=sync.get("timeseries_view_space", _DEFAULTS["timeseries_view_space"]),
        timeseries_view_external_id=sync.get("timeseries_view_external_id", _DEFAULTS["timeseries_view_external_id"]),
        timeseries_view_version=sync.get("timeseries_view_version", _DEFAULTS["timeseries_view_version"]),
    )


def load_config_parameters(client: CogniteClient, oid_client_secret: str) -> OIDConfig:
    """
    Load configuration parameters from extraction pipeline configuration
    
    Args:
        client: CogniteClient for accessing extraction pipeline config
        oid_client_secret: Secret for Open Industrial Data access (from CDF secrets)
        
    Returns:
        OIDConfig: Configuration object
    """
    config_data = _fetch_pipeline_config(client)
    return _build_config(config_data, oid_client_secret)

