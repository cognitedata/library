from typing import Any, Optional

import streamlit as st
from client_factory import CogniteClientFactory
from dotenv import load_dotenv
from ui import AnnotationQualityUI

st.set_page_config(page_title="Annotation Quality", page_icon="🎯", layout="wide")

# ---------------------------------------------------------------------------
# Usage tracking
# ---------------------------------------------------------------------------
_SOURCE = "dp:cdf_file_annotation"
_DP_VERSION = "1"
_TRACKER_VERSION = "1"


def _report_usage(cdf_client) -> None:
    if cdf_client is None or st.session_state.get("_usage_tracked"):
        return
    try:
        import threading
        from mixpanel import Consumer, Mixpanel
        mp = Mixpanel("8f28374a6614237dd49877a0d27daa78", consumer=Consumer(api_host="api-eu.mixpanel.com"))
        distinct_id = f"{cdf_client.config.project}:{cdf_client.config.cdf_cluster}"
        def _send() -> None:
            mp.track(distinct_id, "streamlit-session", {
                "source": _SOURCE,
                "tracker_version": _TRACKER_VERSION,
                "dp_version": _DP_VERSION,
                "type": "streamlit",
                "cdf_cluster": cdf_client.config.cdf_cluster,
                "cdf_project": cdf_client.config.project,
            })
        threading.Thread(target=_send, daemon=True).start()
        st.session_state["_usage_tracked"] = True
    except Exception:
        pass


def main() -> None:
    load_dotenv()
    client: Optional[Any] = CogniteClientFactory.create_from_env()
    _report_usage(client)
    ui = AnnotationQualityUI(client=client)
    ui.render()


if __name__ == "__main__":
    main()
