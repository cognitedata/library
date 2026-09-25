"""Best-effort deployment-pack usage reporting."""

import threading

from cognite.client import CogniteClient

from mixpanel import Consumer, Mixpanel

_SOURCE = "dp:contextualization:cdf_file_annotation"
_DP_VERSION = "1"
_TRACKER_VERSION = "1"


def report_usage(client: CogniteClient) -> None:
    """Report one function invocation without affecting pipeline behavior."""
    try:
        mixpanel = Mixpanel("8f28374a6614237dd49877a0d27daa78", consumer=Consumer(api_host="api-eu.mixpanel.com"))
        distinct_id = f"{client.config.project}:{client.config.cdf_cluster}"

        def send() -> None:
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
            except Exception:
                # Usage tracking is best-effort; must not affect the handler.
                pass

        threading.Thread(target=send, daemon=False).start()
    except Exception:
        # Usage tracking is best-effort; must not affect the handler.
        pass
