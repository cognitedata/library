import streamlit as st
from client_factory import CogniteClientFactory
from cognite.client import CogniteClient
from dotenv import load_dotenv
from ui import PipelineHealthUI

st.set_page_config(page_title="Pipeline Health", page_icon="🩺", layout="wide")

# ---------------------------------------------------------------------------
# Usage tracking
# ---------------------------------------------------------------------------
_SOURCE = "dp:contextualization:cdf_file_annotation"
_DP_VERSION = "1"
_TRACKER_VERSION = "1"


def _report_usage(cdf_client: CogniteClient | None) -> None:
    if cdf_client is None or st.session_state.get("_usage_tracked"):
        return
    try:
        import base64
        import json
        import re

        import requests
        cluster = getattr(cdf_client.config, "cdf_cluster", None)
        if not cluster:
            m = re.match(r"https://([^.]+)\.cognitedata\.com", getattr(cdf_client.config, "base_url", "") or "")
            cluster = m.group(1) if m else "unknown"
        distinct_id = f"{cdf_client.config.project}:{cluster}"
        payload = base64.b64encode(json.dumps([{
            "event": "streamlit-session",
            "properties": {
                "token": "8f28374a6614237dd49877a0d27daa78",
                "distinct_id": distinct_id,
                "source": _SOURCE,
                "tracker_version": _TRACKER_VERSION,
                "dp_version": _DP_VERSION,
                "type": "streamlit",
                "cdf_cluster": cluster,
                "cdf_project": cdf_client.config.project,
            },
        }]).encode()).decode()
        requests.post(
            "https://api-eu.mixpanel.com/track",
            data={"data": payload, "verbose": 1, "ip": 1},
            timeout=5,
        )
        st.session_state["_usage_tracked"] = True
    except Exception:
        # Usage tracking is best-effort; must not affect the dashboard.
        pass


def main() -> None:
    load_dotenv()
    client: CogniteClient | None = CogniteClientFactory.create_from_env()
    _report_usage(client)
    ui = PipelineHealthUI(client)
    ui.render()

if __name__ == "__main__":
	main()
