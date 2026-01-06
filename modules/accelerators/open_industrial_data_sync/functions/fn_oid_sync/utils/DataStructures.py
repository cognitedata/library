from dataclasses import dataclass
from typing import Optional


@dataclass
class EnvConfig:
    """Configuration from environment variables for target CDF project"""
    cdf_project: str
    cdf_cluster: str
    tenant_id: str
    client_id: str
    client_secret: str
    open_id_client_secret: Optional[str] = None  # Secret for OID access


@dataclass
class OIDConfig:
    """Configuration for Open Industrial Data sync
    
    This configures access to the publicdata CDF project (different from target project)
    """
    instance_space: str
    oid_tenant_id: str
    oid_client_id: str
    oid_client_secret: str  # From env OPEN_ID_CLIENT_SECRET
    timeseries_view_space: str
    timeseries_view_external_id: str
    timeseries_view_version: str
    oid_cluster: str = "api"
    oid_project: str = "publicdata"
    sync_realtime_start: str = "15m-ago"  # Start time for real-time sync (should match schedule frequency + buffer)
    sync_random_start: str = "12w-ago"  # Start time for random backfill
    sync_end: str = "168h-ago"  # End time (1 week ago due to OID publication delay)
    time_offset_weeks: int = 1  # Offset forward to simulate real-time
    time_offset_minutes: int = 10  # Additional minutes offset
    log_level: str = "INFO"
    reset_backfill: bool = False  # If true, remove all oid_backfilled tags to restart backfill

