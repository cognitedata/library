"""Best-effort deployment-pack usage reporting."""

import os
import re
from typing import Protocol

from cognite.client import CogniteClient

_SOURCE = "dp:contextualization:cdf_file_annotation"
_DP_VERSION = "1"
_TRACKER_VERSION = "1"
_USAGE_ENV = "CDF_USAGE_REPORTING"
_REQUEST_TIMEOUT_SECONDS = 2


class _UsageTracker(Protocol):
    def track(self, distinct_id: str, event_name: str, properties: dict[str, str]) -> None:
        """Send one usage event."""


def _tracker() -> _UsageTracker:
    """Import Mixpanel lazily so a missing client cannot break the handler."""
    from mixpanel import Consumer, Mixpanel

    return Mixpanel(
        "8f28374a6614237dd49877a0d27daa78",
        consumer=Consumer(
            api_host="api-eu.mixpanel.com",
            request_timeout=_REQUEST_TIMEOUT_SECONDS,
            retry_limit=0,
        ),
    )


def _cluster_name(client: CogniteClient) -> str:
    """Best-effort cluster id. Prefer config.cdf_cluster; else parse base_url."""
    cluster = getattr(client.config, "cdf_cluster", None)
    if cluster:
        return str(cluster)
    match = re.match(r"https://([^.]+)\.cognitedata\.com", getattr(client.config, "base_url", "") or "")
    return match.group(1) if match else "unknown"


def report_usage(client: CogniteClient) -> None:
    """Report one function invocation without affecting pipeline behavior.

    Set CDF_USAGE_REPORTING=false to skip reporting. Tracking runs synchronously with a short
    timeout so the request can finish before the serverless runtime freezes after return.
    """
    if os.environ.get(_USAGE_ENV, "").strip().lower() == "false":
        return
    try:
        project = client.config.project
        cluster = _cluster_name(client)
        mixpanel = _tracker()
        mixpanel.track(
            f"{project}:{cluster}",
            "fn-handle",
            {
                "source": _SOURCE,
                "tracker_version": _TRACKER_VERSION,
                "dp_version": _DP_VERSION,
                "type": "py-function",
                "cdf_cluster": cluster,
                "cdf_project": project,
            },
        )
    except Exception:
        # Usage tracking is best-effort; must not affect the handler.
        return
