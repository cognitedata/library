"""Best-effort deployment-pack usage reporting."""

import os
import threading
from typing import Protocol

from cognite.client import CogniteClient

_SOURCE = "dp:contextualization:cdf_file_annotation"
_DP_VERSION = "1"
_TRACKER_VERSION = "1"
_USAGE_ENV = "CDF_USAGE_REPORTING"


class _UsageTracker(Protocol):
    def track(self, distinct_id: str, event_name: str, properties: dict[str, str]) -> None:
        """Send one usage event."""


def _tracker() -> _UsageTracker:
    """Import Mixpanel lazily so a missing client cannot break the handler."""
    from mixpanel import Consumer, Mixpanel

    return Mixpanel("8f28374a6614237dd49877a0d27daa78", consumer=Consumer(api_host="api-eu.mixpanel.com"))


def report_usage(client: CogniteClient) -> None:
    """Report one function invocation without affecting pipeline behavior.

    Set CDF_USAGE_REPORTING=false to skip reporting. The send runs on a daemon thread so a slow
    tracker cannot keep the function call alive after the stage returns.
    """
    if os.environ.get(_USAGE_ENV, "").strip().lower() == "false":
        return
    try:
        mixpanel = _tracker()
    except ImportError:
        return
    distinct_id = f"{client.config.project}:{client.config.cdf_cluster}"

    def send() -> None:
        from mixpanel import MixpanelException

        try:
            mixpanel.track(
                distinct_id,
                "fn-handle",
                {
                    "source": _SOURCE,
                    "tracker_version": _TRACKER_VERSION,
                    "dp_version": _DP_VERSION,
                    "type": "py-function",
                    "cdf_cluster": client.config.cdf_cluster,
                    "cdf_project": client.config.project,
                },
            )
        except MixpanelException:
            # Usage tracking is best-effort; must not affect the handler.
            pass

    threading.Thread(target=send, daemon=True).start()
