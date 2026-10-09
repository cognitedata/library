"""Best-effort deployment-pack usage reporting, once per dashboard session."""

import base64
import json
import os
import re

import requests
import streamlit as st
from cognite.client import CogniteClient

_SOURCE = "dp:contextualization:cdf_file_annotation"
_DP_VERSION = "1"
_TRACKER_VERSION = "1"
_USAGE_ENV = "CDF_USAGE_REPORTING"
_MIXPANEL_TOKEN = "8f28374a6614237dd49877a0d27daa78"  # noqa: S105 - public Mixpanel project token, not a credential


def report_usage(cdf_client: CogniteClient | None) -> None:
    """Send one session event. Set CDF_USAGE_REPORTING=false to skip reporting."""
    if cdf_client is None or st.session_state.get("_usage_tracked"):
        return
    if os.environ.get(_USAGE_ENV, "").strip().lower() == "false":
        return
    cluster = getattr(cdf_client.config, "cdf_cluster", None)
    if not cluster:
        match = re.match(r"https://([^.]+)\.cognitedata\.com", getattr(cdf_client.config, "base_url", "") or "")
        cluster = match.group(1) if match else "unknown"
    event = {
        "event": "streamlit-session",
        "properties": {
            "token": _MIXPANEL_TOKEN,
            "distinct_id": f"{cdf_client.config.project}:{cluster}",
            "source": _SOURCE,
            "tracker_version": _TRACKER_VERSION,
            "dp_version": _DP_VERSION,
            "type": "streamlit",
            "cdf_cluster": cluster,
            "cdf_project": cdf_client.config.project,
        },
    }
    payload = base64.b64encode(json.dumps([event]).encode()).decode()
    try:
        requests.post("https://api-eu.mixpanel.com/track", data={"data": payload}, timeout=5)
    except requests.RequestException:
        # Usage tracking is best-effort; must not affect the dashboard.
        return
    st.session_state["_usage_tracked"] = True
