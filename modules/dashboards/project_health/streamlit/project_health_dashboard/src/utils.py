"""
Utility functions for the CDF Project Health Dashboard.
"""

from datetime import datetime, timezone
from urllib.parse import urlparse

from .config import (
    STATUS_COLORS,
    STATUS_EMOJIS,
    COLORS,
    FAILED_STATUSES,
)


def is_failed_status(status: str) -> bool:
    if not status:
        return False
    return status.lower() in FAILED_STATUSES


def get_status_color(status: str) -> str:
    if not status:
        return COLORS["neutral"]
    return STATUS_COLORS.get(status.lower(), COLORS["neutral"])


def get_status_emoji(status: str) -> str:
    if not status:
        return "❔"
    return STATUS_EMOJIS.get(status.lower(), "❔")


def format_timestamp(ts) -> str:
    if ts is None:
        return "N/A"
    if isinstance(ts, (int, float)):
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    elif isinstance(ts, datetime):
        dt = ts
    else:
        return str(ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def get_time_ago(ts) -> str:
    if ts is None:
        return "Never"
    if isinstance(ts, (int, float)):
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    elif isinstance(ts, datetime):
        dt = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    else:
        return "Unknown"
    now = datetime.now(timezone.utc)
    delta = now - dt
    if delta.days > 30:
        months = delta.days // 30
        return f"{months} month{'s' if months > 1 else ''} ago"
    elif delta.days > 0:
        return f"{delta.days} day{'s' if delta.days > 1 else ''} ago"
    elif delta.seconds > 3600:
        hours = delta.seconds // 3600
        return f"{hours} hour{'s' if hours > 1 else ''} ago"
    elif delta.seconds > 60:
        minutes = delta.seconds // 60
        return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
    return "Just now"


def _base_url_to_origin(base_url: str) -> str:
    """Use only scheme + netloc so Project URL is the starting part; path is built per resource."""
    if not base_url or not base_url.strip():
        return ""
    u = urlparse(base_url.strip().rstrip("/"))
    if not u.scheme or not u.netloc:
        return base_url.strip().rstrip("/")
    return f"{u.scheme}://{u.netloc}"


def build_cdf_link(
    base_url: str,
    project: str,
    resource_type: str,
    resource_id: str = None,
    cluster: str = None,
) -> str:
    """Build CDF Fusion link: base_origin/project/resource_path?cluster=...&workspace=...
    base_url is normalized to origin only (scheme + host); path is resource-specific.
    """
    from .config import CDF_CLUSTER_DOMAIN, CDF_WORKSPACE
    base_origin = _base_url_to_origin(base_url or "")
    if not base_origin:
        return ""
    project = (project or "").strip()
    full_cluster = f"{cluster}{CDF_CLUSTER_DOMAIN}" if (cluster and str(cluster).strip()) else None
    query_parts = []
    if full_cluster:
        query_parts.append(f"cluster={full_cluster}")
    query_parts.append(f"workspace={CDF_WORKSPACE}")
    query_string = "&".join(query_parts)
    LIST_URL_PATHS = {
        "extraction_pipelines": f"extpipes?{query_string}&tab=self-hosted",
        "transformations": f"transformations?{query_string}",
        "workflows": f"flows?{query_string}",
        "functions": f"functions?{query_string}",
    }
    # List links (Overview Quick Links): require project so Fusion does not redirect to base
    if resource_type in LIST_URL_PATHS:
        path = LIST_URL_PATHS[resource_type]
        if not project:
            return ""
        return f"{base_origin}/{project}/{path}"
    # Detail links (table row "View ↗"): require project; use resource id (or external_id for workflows)
    if resource_type in ("extraction_pipeline", "transformation", "workflow", "function"):
        if not project:
            return ""
        # Function detail is just the list URL (no id in path); others need resource_id
        if resource_type == "function":
            path = f"functions?{query_string}"
        else:
            id_val = None if resource_id is None else str(resource_id).strip()
            if not id_val:
                return ""
            path_templates = {
                "extraction_pipeline": f"extpipes/extpipe/{id_val}?{query_string}",
                "transformation": f"transformations/{id_val}/run-history?{query_string}",
                "workflow": f"flows/{id_val}/run-history?{query_string}",
            }
            path = path_templates.get(resource_type)
            if not path:
                return base_origin
        return f"{base_origin}/{project}/{path}"
    return base_origin
