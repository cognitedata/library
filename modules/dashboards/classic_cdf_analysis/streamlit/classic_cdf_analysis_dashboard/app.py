"""
Classic CDF Analysis — Complete.
Multi-resource-type analysis with a shared dataset section.
"""
from __future__ import annotations

import asyncio
import html
import os
import re
import time
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning, module="pandas")
warnings.filterwarnings("ignore", message=".*pyarrow.*", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*coroutine.*was never awaited", category=RuntimeWarning)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
from typing import Any, Optional

import streamlit as st
import pandas as pd

import analysis as _analysis_module
from analysis import (
    ClientAdapter,
    get_aggregate_count_no_filter,
    get_dataset_resource_counts,
    get_datasets_list,
    get_global_extended_counts,
    get_metadata_keys_list,
    get_total_count,
)
from analysis import (
    _aggregate_resource,
    _data_set_filter_aggregate,
    _documents_data_set_filter,
    _parse_count_response as _analysis_parse_count_response,
    _project_path,
    _unique_properties_keys_documents,
)
_analysis_unwrap = getattr(_analysis_module, "_unwrap_maybe_coro", None)

from key_selection import PRIMARY_FILTER_KEYS, select_filter_keys_for_analysis, slug_for_file_name

try:
    import logging
    logging.getLogger("cognite").setLevel(logging.WARNING)
    logging.getLogger("cognite.client").setLevel(logging.WARNING)
except Exception:
    pass

RESOURCE_OPTIONS = [
    {"value": "assets", "label": "Assets"},
    {"value": "timeseries", "label": "Time series"},
    {"value": "events", "label": "Events"},
    {"value": "sequences", "label": "Sequences"},
    {"value": "files", "label": "Files"},
]

COUNT_LOAD_CAP = 50

# ---------------------------------------------------------------------------
# Usage tracking
# ---------------------------------------------------------------------------
_SOURCE = "dp:classic_cdf_analysis"
_DP_VERSION = "1"
_TRACKER_VERSION = "1"


def _report_usage(cdf_client) -> None:
    if st.session_state.get("_usage_tracked"):
        return
    try:
        import re
        import threading
        from mixpanel import Consumer, Mixpanel
        mp = Mixpanel("8f28374a6614237dd49877a0d27daa78", consumer=Consumer(api_host="api-eu.mixpanel.com"))
        cluster = getattr(cdf_client.config, "cdf_cluster", None)
        if not cluster:
            m = re.match(r"https://([^.]+)\.cognitedata\.com", getattr(cdf_client.config, "base_url", "") or "")
            cluster = m.group(1) if m else "unknown"
        distinct_id = f"{cdf_client.config.project}:{cluster}"
        def _send() -> None:
            mp.track(distinct_id, "streamlit-session", {
                "source": _SOURCE,
                "tracker_version": _TRACKER_VERSION,
                "dp_version": _DP_VERSION,
                "type": "streamlit",
                "cdf_cluster": cluster,
                "cdf_project": cdf_client.config.project,
            })
        threading.Thread(target=_send, daemon=False).start()
        st.session_state["_usage_tracked"] = True
    except Exception:
        pass


def _is_pyodide() -> bool:
    try:
        import sys
        if getattr(sys, "platform", None) == "emscripten":
            return True
    except Exception:
        pass
    try:
        import pyodide  # noqa: F401
        return True
    except ImportError:
        pass
    return False


def _secret(*keys: str) -> str | None:
    """Return the first non-empty value found across the given env-var / Streamlit-secret names."""
    for key in keys:
        val = os.environ.get(key)
        if val:
            return val
        try:
            val = st.secrets.get(key) if hasattr(st, "secrets") else None
            if val:
                return val
        except Exception:
            pass
    return None


def _resolve_base_url() -> str:
    """Resolve CDF base URL from CDF_URL / COGNITE_BASE_URL, or derive from CDF_CLUSTER."""
    explicit = _secret("CDF_URL", "COGNITE_BASE_URL")
    if explicit:
        return explicit
    cluster = _secret("CDF_CLUSTER")
    if cluster:
        return f"https://{cluster}.cognitedata.com"
    return "https://api.cognitedata.com"


def get_client_and_project() -> tuple[Any, str] | None:
    """Build CogniteClient and project from env/secrets. Returns (client, project) or None."""
    try:
        from cognite.client import CogniteClient, ClientConfig
        from cognite.client.credentials import OAuthClientCredentials, Token
    except ImportError:
        st.error("Install cognite-sdk >= 7: `pip install cognite-sdk`")
        return None

    # 0) CDF-hosted Stlite environment: CogniteClient() auto-configures from the logged-in session
    try:
        _c = CogniteClient()
        if getattr(_c.config, "project", None):
            return (_c, _c.config.project)
    except Exception:
        pass

    # APIKey was removed in cognite-sdk v7; import optionally so it doesn't break other auth flows
    try:
        from cognite.client.credentials import APIKey as _APIKey
    except ImportError:
        _APIKey = None

    # CDF_PROJECT (Toolkit) or COGNITE_PROJECT (app)
    project = _secret("CDF_PROJECT", "COGNITE_PROJECT")
    base_url = _resolve_base_url()

    # 1) API key auth (only available in cognite-sdk <7)
    api_key = _secret("COGNITE_API_KEY")
    if api_key and project and _APIKey is not None:
        config = ClientConfig(
            client_name="classic-analysis-complete",
            project=project,
            credentials=_APIKey(api_key),
            base_url=base_url,
        )
        return (CogniteClient(config), project)

    # 2) Bearer-token auth — CDF_TOKEN (Toolkit) or COGNITE_TOKEN (app)
    token = _secret("CDF_TOKEN", "COGNITE_TOKEN")
    if token and project:
        config = ClientConfig(
            client_name="classic-analysis-complete",
            project=project,
            credentials=Token(token),
            base_url=base_url,
        )
        return (CogniteClient(config), project)

    # 3) OAuth client-credentials auth
    #    IDP_CLIENT_ID / IDP_CLIENT_SECRET / IDP_TENANT_ID (Toolkit)
    #    or COGNITE_CLIENT_ID / COGNITE_CLIENT_SECRET / COGNITE_TENANT_ID (app)
    client_id = _secret("IDP_CLIENT_ID", "COGNITE_CLIENT_ID")
    client_secret = _secret("IDP_CLIENT_SECRET", "COGNITE_CLIENT_SECRET")
    # IDP_TOKEN_URL takes precedence; otherwise construct from tenant ID
    token_url = _secret("IDP_TOKEN_URL")
    if not token_url:
        tenant_id = _secret("IDP_TENANT_ID", "COGNITE_TENANT_ID")
        token_url = f"https://login.microsoftonline.com/{tenant_id or 'organizations'}/oauth2/v2.0/token"
    if client_id and client_secret and project:
        creds = OAuthClientCredentials(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
            scopes=[f"{base_url.rstrip('/')}/.default"],
        )
        config = ClientConfig(
            client_name="classic-analysis-complete",
            project=project,
            credentials=creds,
            base_url=base_url,
        )
        return (CogniteClient(config), project)

    return None


def _ensure_sync(val: Any) -> Any:
    """If val is a coroutine/awaitable or JS Promise, resolve it synchronously."""
    is_thenable = hasattr(val, "then") and callable(getattr(val, "then", None))
    if is_thenable and _analysis_unwrap is not None:
        return _analysis_unwrap(val)
    if asyncio.iscoroutine(val) or (hasattr(val, "__await__") and callable(getattr(val, "__await__", None))):
        try:
            return asyncio.run(val)
        except RuntimeError:
            loop = asyncio.get_event_loop()
            if not getattr(loop, "is_running", lambda: False)():
                try:
                    return loop.run_until_complete(val)
                except Exception:
                    pass
            else:
                try:
                    import nest_asyncio
                    nest_asyncio.apply()
                    return loop.run_until_complete(val)
                except Exception:
                    pass
        if _analysis_unwrap is not None:
            return _analysis_unwrap(val)
        return val
    return val


def filter_key_for_api(raw: str) -> str:
    raw = raw.strip()
    m = re.match(r"^(.+)\s+\((\d+)\)$", raw)
    return m.group(1).strip() if m else raw


def _prop_path(fk: str, rt: str) -> list:
    """Property path for the CDF aggregate endpoint."""
    if rt == "timeseries":
        n = fk.strip().lower()
        if n in ("is step", "isstep"): return ["isStep"]
        if n in ("is string", "isstring"): return ["isString"]
        if n in ("unit", "units"): return ["unit"]
        return ["metadata", fk.strip()]
    if rt == "files":
        n = fk.strip().lower()
        if n == "type": return ["type"]
        if n == "labels": return ["labels"]
        if n == "author": return ["author"]
        if n == "source": return ["sourceFile", "source"]
        return ["sourceFile", "metadata", fk.strip()]
    if rt == "events":
        from analysis import _events_property_path
        return _events_property_path(fk.strip())
    return ["metadata", fk.strip()]


def _meta_path(rt: str) -> list:
    return ["sourceFile", "metadata"] if rt == "files" else ["metadata"]


def _run_analysis_for_key(adapter: ClientAdapter, project: str, rt: str, key: str, ds_ids: Optional[list[dict]]) -> list[str]:
    """Run uniqueValues + uniqueProperties for one metadata key. Returns report lines."""
    api_key = filter_key_for_api(key)
    agg_resource = "documents" if rt == "files" else rt
    agg_path = _project_path(project, agg_resource)
    filter_part = _documents_data_set_filter(ds_ids) if rt == "files" else _data_set_filter_aggregate(ds_ids)
    pp = _prop_path(api_key, rt)

    uv = adapter.post(agg_path, {"aggregate": "uniqueValues", "properties": [{"property": pp}], **filter_part})
    items = (uv.get("items") or []) if isinstance(uv, dict) else []
    results = []
    for item in items:
        if not isinstance(item, dict):
            continue
        raw_val = item.get("value")
        if raw_val is None and isinstance(item.get("values"), list) and item["values"]:
            raw_val = item["values"][0]
        if raw_val is None:
            continue
        val = str(raw_val) if not isinstance(raw_val, str) else raw_val
        cnt = int(item.get("count", 0) or 0)
        is_labels = rt == "files" and len(pp) == 1 and pp[0] == "labels"
        max_len = 512 if rt == "files" else 64
        if is_labels:
            mk = []
        elif len(val) > max_len:
            mk = []
        elif rt == "files":
            eq_filter = {"equals": {"property": pp, "value": raw_val}}
            ds_filter = filter_part.get("filter") if filter_part else None
            merged = {"and": [ds_filter, eq_filter]} if ds_filter else eq_filter
            up = adapter.post(agg_path, {
                "aggregate": "uniqueProperties",
                "properties": [{"property": ["sourceFile", "metadata"]}],
                "limit": 1000,
                "filter": merged,
            })
            up_items = (up.get("items") or []) if isinstance(up, dict) else []
            mk = _unique_properties_keys_documents(up_items)
        else:
            filter_val = raw_val
            if pp[0] in ("isStep", "isString") and str(raw_val).lower() in ("true", "false"):
                filter_val = str(raw_val).lower() == "true"
            value_clause = {"equals": {"property": pp, "value": filter_val}}
            ds_clause = filter_part.get("advancedFilter") if filter_part else None
            merged_af = {"and": [value_clause, ds_clause]} if ds_clause else value_clause
            up = adapter.post(agg_path, {
                "aggregate": "uniqueProperties",
                "path": _meta_path(rt),
                "advancedFilter": merged_af,
            })
            up_items = (up.get("items") or []) if isinstance(up, dict) else []
            mk = []
            for p in up_items:
                if not isinstance(p, dict):
                    continue
                prop = p.get("property")
                if not prop and isinstance(p.get("values"), list) and p["values"] and isinstance(p["values"][0], dict):
                    prop = p["values"][0].get("property")
                if isinstance(prop, list) and len(prop) > 0:
                    mk.append(prop[-1])
        from analysis import _parse_metadata_tag
        bare_key, _ = _parse_metadata_tag(api_key)
        label = bare_key.title()
        count_str = f"Count: {cnt}\n" if cnt else ""
        if is_labels:
            meta_part = "Metadata keys: (not available for labels)\n\n"
        else:
            meta_part = f"Metadata keys: [{', '.join(mk)}]\n\n" if mk else "\n"
        results.append({"count": cnt, "text": f"{label}: {val}\n{count_str}{meta_part}"})

    results.sort(key=lambda r: r["count"], reverse=True)
    out_lines = [f"=== Filter key: {key} ===", ""]
    if results:
        values_list = [r["text"].split(":")[1].split("\n")[0].strip() if ":" in r["text"] else "" for r in results]
        out_lines.append("Values: " + ", ".join(v for v in values_list if v))
        out_lines.append("")
        out_lines.append("".join(r["text"] for r in results))
    else:
        out_lines.append("(no values found)")
    out_lines.append("")
    return out_lines


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(page_title="Classic CDF Analysis — Complete", layout="wide")
    st.title("Classic CDF Analysis")
    st.caption("Metadata field distribution for Assets, Time series, Events, Sequences, and Files.")
    st.markdown(
        "<style>"
        "div[data-testid='stVerticalBlock'] > div { padding-top: 0; }"
        "div.block-container { padding-top: 2.5rem; padding-bottom: 6rem; }"
        "h2 { margin-top: 0.3rem !important; margin-bottom: 0.2rem !important; font-size: 1.3rem !important; }"
        "h3 { margin-top: 0.2rem !important; margin-bottom: 0.1rem !important; }"
        "</style>",
        unsafe_allow_html=True,
    )

    client_and_project = get_client_and_project()
    if not client_and_project:
        st.warning(
            "Configure CDF credentials. Set **CDF_PROJECT** (or `COGNITE_PROJECT`) and one of:\n\n"
            "- **COGNITE_API_KEY** — API key auth\n"
            "- **CDF_TOKEN** (or `COGNITE_TOKEN`) — bearer-token auth\n"
            "- **IDP_CLIENT_ID** + **IDP_CLIENT_SECRET** (or `COGNITE_CLIENT_ID` / `COGNITE_CLIENT_SECRET`) — OAuth client-credentials\n\n"
            "Set the CDF cluster via **CDF_CLUSTER** (e.g. `westeurope-1`), **CDF_URL**, or `COGNITE_BASE_URL`.\n\n"
            "Values can be set as environment variables, in a `.env` file, or in Streamlit secrets."
        )
        return

    client, project = client_and_project
    _report_usage(client)
    adapter = ClientAdapter(client, project)

    # ---- Analysis loading block (must be before UI so API calls run at top level) ----
    if st.session_state.get("_analysis_all_pending"):
        _rts = st.session_state.pop("_analysis_all_rts", ["assets"])
        _cov_frac = st.session_state.pop("_analysis_coverage_frac", 0.6)
        _run_mode = st.session_state.pop("_analysis_mode_for_run", "auto")
        st.session_state["analysis_mode_radio"] = "Custom" if _run_mode == "custom" else "Auto"
        _rts_set = set(_rts)
        for _opt in RESOURCE_OPTIONS:
            st.session_state[f"analysis_rt_{_opt['value']}"] = _opt["value"] in _rts_set
        _custom_selections = st.session_state.pop("_analysis_custom_selections", {})
        _sel_ids = st.session_state.get("_selected_dataset_ids", [])
        _ds_ids = [{"id": i} for i in _sel_ids] if _sel_ids else None
        _ds_display = []
        for sid in _sel_ids:
            d = next((x for x in (st.session_state.get("datasets") or []) if getattr(x, "id", None) is not None and int(getattr(x, "id", 0)) == int(sid)), None)
            name = (getattr(d, "name", None) or getattr(d, "external_id", None) or str(sid)).strip() if d else str(sid)
            _ds_display.append(f"{name} ({sid})")
        _datasets_line = ", ".join(_ds_display) if _ds_display else "All datasets"
        try:
            with st.spinner("Running analysis…"):
                _all_results = []
                for ri, _rt in enumerate(_rts):
                    _rt_label = next((o["label"] for o in RESOURCE_OPTIONS if o["value"] == _rt), _rt)
                    print(f"[ANALYSIS] [{ri+1}/{len(_rts)}] Starting {_rt_label}…")
                    try:
                        agg_resource = "documents" if _rt == "files" else _rt
                        count_path = _project_path(project, agg_resource)
                        filter_part = _documents_data_set_filter(_ds_ids) if _rt == "files" else _data_set_filter_aggregate(_ds_ids)
                        count_res = adapter.post(count_path, {"aggregate": "count", **filter_part})
                        total_count = _analysis_parse_count_response(count_res)
                        print(f"[ANALYSIS]   Count: {total_count:,}")

                        if _run_mode == "custom" and _rt in _custom_selections:
                            sel_keys = _custom_selections[_rt]
                        else:
                            raw_meta = get_metadata_keys_list(adapter, _rt, project, _ds_ids)
                            raw_meta = raw_meta if isinstance(raw_meta, list) else []
                            sel_keys = select_filter_keys_for_analysis(raw_meta, total_count, _rt, _cov_frac) if total_count > 0 else (PRIMARY_FILTER_KEYS.get(_rt, []) or ["type"])[:10]
                        print(f"[ANALYSIS]   Metadata keys: {len(sel_keys)}")

                        header_lines = [
                            f"CDF Project: {project}", "",
                            f"Resource type: {_rt_label}",
                            f"Aggregate count: {total_count:,}",
                            f"Instance count threshold: {int(_cov_frac * 100)}%", "",
                            "Datasets:", "  - " + _datasets_line, "",
                            f"Metadata keys analysed: {len(sel_keys)}",
                            *[f"  - {k}" for k in sel_keys], "",
                            "---", "",
                        ]
                        lines = list(header_lines)
                        for ki, key in enumerate(sel_keys):
                            if not key:
                                continue
                            print(f"[ANALYSIS]   Key [{ki+1}/{len(sel_keys)}]: {key}")
                            try:
                                lines.extend(_run_analysis_for_key(adapter, project, _rt, key, _ds_ids))
                            except Exception as e:
                                lines.append(f"=== Filter key: {key} ===")
                                lines.append("")
                                lines.append("Error: " + str(e))
                                lines.append("")

                        _pct = int(((ri + 1) / len(_rts)) * 100)
                        print(f"[ANALYSIS]   Done — {_pct}% overall")
                        _all_results.append({
                            "report": "\n".join(lines),
                            "rt": _rt,
                            "rt_label": _rt_label,
                            "count": total_count,
                            "keys": sel_keys,
                        })
                    except Exception as _rt_err:
                        _err_str = str(_rt_err)
                        if "403" in _err_str or "Forbidden" in _err_str:
                            _err_line = "403 Forbidden — insufficient access for this resource type"
                        else:
                            _err_line = _err_str
                        print(f"[ANALYSIS]   ERROR for {_rt_label}: {_err_line}")
                        _all_results.append({
                            "report": f"Resource type: {_rt_label}\n\nError: {_err_line}\n",
                            "rt": _rt,
                            "rt_label": _rt_label,
                            "count": 0,
                            "keys": [],
                        })

            st.session_state["_analysis_results"] = _all_results
            st.session_state.pop("_analysis_all_pending", None)
            st.rerun()
        except Exception as _e:
            print(f"[ANALYSIS] ERROR: {_e}")
            st.session_state.pop("_analysis_all_pending", None)
            st.error(f"Analysis failed: {_e}")

    # ========================================================================
    # All Datasets summary
    # ========================================================================
    st.subheader("All Datasets")
    if "all_counts" not in st.session_state:
        st.session_state.all_counts = None
    if st.session_state.all_counts is None:
        with st.spinner("Loading project counts…"):
            try:
                def _safe_count(rt: str):
                    try:
                        return get_aggregate_count_no_filter(adapter, project, rt)
                    except Exception:
                        return "N/A"
                counts = {
                    "assets": _safe_count("assets"),
                    "timeseries": _safe_count("timeseries"),
                    "events": _safe_count("events"),
                    "sequences": _safe_count("sequences"),
                    "files": _safe_count("files"),
                }
                try:
                    ext = get_global_extended_counts(adapter, project)
                    counts.update(ext)
                except Exception:
                    counts.setdefault("transformations", 0)
                    counts.setdefault("functions", 0)
                    counts.setdefault("workflows", 0)
                    counts.setdefault("rawTables", 0)
                # Fetch metadata key counts per resource type
                meta_key_counts = {}
                for _mk_rt in ["assets", "timeseries", "events", "sequences", "files"]:
                    try:
                        _mk_list = get_metadata_keys_list(adapter, _mk_rt, project)
                        meta_key_counts[_mk_rt] = len(_mk_list) if isinstance(_mk_list, list) else 0
                    except Exception:
                        meta_key_counts[_mk_rt] = "N/A"
                st.session_state.meta_key_counts = meta_key_counts
                st.session_state.all_counts = counts
            except Exception as e:
                st.error(f"Failed to load counts: {e}")
                st.session_state.all_counts = {}

    all_counts = st.session_state.all_counts or {}
    _mk_counts = st.session_state.get("meta_key_counts", {})

    _hdr = ["Assets", "Timeseries", "Events", "Sequences", "Files", "Transformations", "Functions", "Workflows", "Raw tables"]
    _keys = ["assets", "timeseries", "events", "sequences", "files", "transformations", "functions", "workflows", "rawTables"]
    _mk_rt_keys = ["assets", "timeseries", "events", "sequences", "files"]

    def _fmt_count(v):
        if v == "N/A":
            return "N/A"
        return f"{v:,}" if isinstance(v, (int, float)) else "—"

    _css = """<style>
    .ad-tbl table { width:100%; border-collapse:collapse; border:none !important; margin-bottom:0.5rem; }
    .ad-tbl th, .ad-tbl td { border:none !important; padding:0.5rem 0.75rem; font-size:0.875rem; text-align:right; }
    .ad-tbl th { background:rgba(151,166,195,0.15); font-weight:600; white-space:nowrap; }
    .ad-tbl .ad-lbl { text-align:left; }
    .ad-tbl .ad-bold { font-weight:700; }
    .ad-tbl .ad-muted { color:#64748b; }
    </style>"""

    _rows_html = "<tr>"
    _rows_html += '<th class="ad-lbl"></th>'
    for h in _hdr:
        _rows_html += f"<th>{h}</th>"
    _rows_html += "</tr>"

    _rows_html += "<tr>"
    _rows_html += '<td class="ad-lbl">Count</td>'
    for k in _keys:
        _rows_html += f'<td class="ad-bold">{_fmt_count(all_counts.get(k))}</td>'
    _rows_html += "</tr>"

    if _mk_counts:
        _rows_html += "<tr>"
        _rows_html += '<td class="ad-lbl ad-muted">Metadata keys</td>'
        for k in _keys:
            if k in _mk_rt_keys:
                v = _mk_counts.get(k)
                if v == "N/A":
                    _rows_html += '<td class="ad-muted">N/A</td>'
                elif isinstance(v, int):
                    _rows_html += f'<td class="ad-muted">{v}</td>'
                else:
                    _rows_html += '<td class="ad-muted">—</td>'
            else:
                _rows_html += '<td class="ad-muted"></td>'
        _rows_html += "</tr>"

    _table_html = f'{_css}<div class="ad-tbl"><table>{_rows_html}</table></div>'
    st.markdown(_table_html, unsafe_allow_html=True)

    # ========================================================================
    # Datasets (optional) — shared by both analysis sections
    # ========================================================================
    st.subheader("Datasets (optional)")
    datasets_loading = bool(st.session_state.get("datasets_loaded") and "datasets" not in st.session_state)
    load_btn_col, load_status_col = st.columns([1, 4])
    with load_btn_col:
        if st.button("Load datasets", type="primary", disabled=bool(datasets_loading), key="load_ds"):
            st.session_state.datasets_loaded = True
            st.rerun()
    if datasets_loading:
        with load_status_col:
            st.markdown(
                '<span style="padding: 0.35rem 0.75rem; border-radius: 0.35rem; background: #fff3cd; border: 1px solid #ffc107; '
                'color: #856404; font-size: 0.9rem;">Loading datasets…</span>',
                unsafe_allow_html=True,
            )
    if st.session_state.get("datasets_loaded"):
        if "datasets" not in st.session_state:
            try:
                with st.spinner("Loading datasets…"):
                    ds_list = get_datasets_list(adapter, project, limit=500)
                    ds_list = sorted(ds_list, key=lambda d: (getattr(d, "name", None) or getattr(d, "external_id", None) or f"ID {d.id}").lower())
                    st.session_state.datasets = ds_list
                    st.session_state.dataset_counts = {}
                    st.session_state.dataset_counts_next = 0
                st.rerun()
            except Exception as e:
                st.error(f"Failed to load datasets: {e}")
                st.session_state.datasets = []

        datasets = st.session_state.get("datasets", [])
        dataset_counts = st.session_state.get("dataset_counts", {})
        to_load = (datasets or [])[:COUNT_LOAD_CAP]
        next_i = st.session_state.get("dataset_counts_next", 0)
        if datasets and next_i < len(to_load) and not st.session_state.get("_analysis_all_pending"):
            st.info("Loading resource counts…")
            batch_size = 10
            end_i = min(next_i + batch_size, len(to_load))
            with st.spinner("Loading resource counts…"):
                for d in to_load[next_i:end_i]:
                    try:
                        st.session_state.setdefault("dataset_counts", {})[d.id] = get_dataset_resource_counts(adapter, project, {"id": d.id})
                    except Exception:
                        st.session_state.setdefault("dataset_counts", {})[d.id] = {"assets": "N/A", "timeseries": "N/A", "events": "N/A", "sequences": "N/A", "files": "N/A"}
            st.session_state.dataset_counts_next = end_i
            st.rerun()

        if "_selected_dataset_ids" not in st.session_state:
            st.session_state["_selected_dataset_ids"] = []

        if datasets:
            if st.session_state.pop("_clear_dataset_selection", False):
                st.session_state["_dataset_editor_version"] = st.session_state.get("_dataset_editor_version", 0) + 1
                for key in ("_selected_dataset_ids",):
                    if key in st.session_state:
                        del st.session_state[key]

            editor_version = st.session_state.get("_dataset_editor_version", 0)
            editor_key = f"dataset_editor_v{editor_version}"
            prev_editor = st.session_state.get(editor_key)
            show_zero_count = st.session_state.get("show_zero_count_datasets", False)
            selected_ids_set = {str(x) for x in st.session_state.get("_selected_dataset_ids", [])}

            def _fmt_cell(x):
                if x == "N/A":
                    return "N/A"
                return f"{x:,}" if isinstance(x, (int, float)) else str(x)

            rows = []
            for i, d in enumerate(datasets):
                label = getattr(d, "name", None) or getattr(d, "external_id", None) or f"ID {d.id}"
                cnts = dataset_counts.get(d.id, {})
                if not cnts and d.id in [x.id for x in to_load]:
                    a = b = ev = seq = f = "…"
                    na = nb = nev = nseq = nf = 0
                    is_loading = True
                else:
                    a = cnts.get("assets", 0)
                    b = cnts.get("timeseries", 0)
                    ev = cnts.get("events", 0)
                    seq = cnts.get("sequences", 0)
                    f = cnts.get("files", 0)
                    na = int(a) if isinstance(a, (int, float)) else 0
                    nb = int(b) if isinstance(b, (int, float)) else 0
                    nev = int(ev) if isinstance(ev, (int, float)) else 0
                    nseq = int(seq) if isinstance(seq, (int, float)) else 0
                    nf = int(f) if isinstance(f, (int, float)) else 0
                    is_loading = False
                total = na + nb + nev + nseq + nf
                show_row = show_zero_count or is_loading or total > 0
                sel = bool(prev_editor.iloc[i]["Select"]) if (
                    prev_editor is not None and isinstance(prev_editor, pd.DataFrame)
                    and "Select" in prev_editor.columns and i < len(prev_editor)
                ) else (str(d.id) in selected_ids_set)
                rows.append({
                    "Select": sel, "Dataset": label,
                    "Assets": _fmt_cell(a), "Timeseries": _fmt_cell(b), "Events": _fmt_cell(ev),
                    "Sequences": _fmt_cell(seq), "Files": _fmt_cell(f),
                    "_id": str(d.id), "_show": show_row,
                })
            displayed_rows = [r for r in rows if r.pop("_show", True)]
            use_prev = (
                prev_editor is not None
                and isinstance(prev_editor, pd.DataFrame)
                and len(prev_editor) == len(displayed_rows)
                and "Select" in prev_editor.columns
            )
            if use_prev:
                for j, row in enumerate(displayed_rows):
                    if j < len(prev_editor):
                        row["Select"] = bool(prev_editor.iloc[j]["Select"])
            else:
                for row in displayed_rows:
                    row["Select"] = row["_id"] in selected_ids_set
            df = pd.DataFrame(displayed_rows) if displayed_rows else pd.DataFrame(
                columns=["Select", "Dataset", "Assets", "Timeseries", "Events", "Sequences", "Files", "_id"]
            )

            st.checkbox(
                "Show datasets with no resources (all counts 0)",
                value=show_zero_count,
                key="show_zero_count_datasets",
                help="By default, only datasets with at least one resource are shown.",
            )
            column_config = {
                "Select": st.column_config.CheckboxColumn("Select", width="small"),
                "Dataset": st.column_config.TextColumn("Dataset", width="medium"),
                "Assets": st.column_config.TextColumn("Assets", width="small"),
                "Timeseries": st.column_config.TextColumn("Timeseries", width="small"),
                "Events": st.column_config.TextColumn("Events", width="small"),
                "Sequences": st.column_config.TextColumn("Sequences", width="small"),
                "Files": st.column_config.TextColumn("Files", width="small"),
                "_id": None,
            }
            edited_df = st.data_editor(
                df,
                column_config=column_config,
                column_order=["Select", "Dataset", "Assets", "Timeseries", "Events", "Sequences", "Files"],
                hide_index=True,
                key=editor_key,
                use_container_width=True,
            )
            try:
                sel = edited_df["Select"].fillna(False)
                if sel.dtype == bool:
                    mask = sel
                else:
                    mask = sel.astype(int, errors="ignore").astype(bool, errors="ignore").fillna(False)
                raw_ids = edited_df.loc[mask, "_id"].dropna()
                selected_ids_from_table = []
                for x in raw_ids:
                    s = str(x).strip()
                    if not s:
                        continue
                    try:
                        i_val = int(s)
                        if i_val >= 1:
                            selected_ids_from_table.append(i_val)
                    except (TypeError, ValueError):
                        continue
            except Exception:
                selected_ids_from_table = []
            st.session_state["_selected_dataset_ids"] = selected_ids_from_table
            if st.button("Clear selection", key="clear_dataset_selection_btn"):
                st.session_state["_clear_dataset_selection"] = True
                st.rerun()
        else:
            st.session_state["_selected_dataset_ids"] = []
    else:
        st.info("Click **Load datasets** to list datasets and their resource counts.")
        st.session_state["_selected_dataset_ids"] = []

    datasets_list = st.session_state.get("datasets", [])
    selected_ids = st.session_state.get("_selected_dataset_ids", [])
    _valid = []
    for x in selected_ids:
        if x is None or str(x).strip() == "":
            continue
        try:
            i_val = int(x)
            if i_val >= 1:
                _valid.append(i_val)
        except (TypeError, ValueError):
            continue
    selected_ids = _valid
    data_set_ids_for_api = [{"id": i} for i in selected_ids] if selected_ids else None

    if selected_ids:
        st.caption(f"Using **{len(selected_ids)}** selected dataset(s) for analysis.")
    else:
        st.caption("Using **all datasets** for analysis. Select rows above to limit.")

    # ========================================================================
    # Analysis (multi-resource-type, all keys)
    # ========================================================================
    st.write("---")
    st.subheader("Analysis")

    _mode_col, _cov_col, _cov_spacer = st.columns([1, 1, 2])
    with _mode_col:
        _mode_radio = st.radio("Mode", ["Auto", "Custom"], horizontal=True, key="analysis_mode_radio",
                               help="Auto: algorithm selects keys. Custom: you pick from the full list.")
    _mode_val = "custom" if _mode_radio == "Custom" else "auto"
    with _cov_col:
        if _mode_val == "auto":
            coverage_pct = st.number_input(
                "Threshold (%)", min_value=0, max_value=100, value=60,
                step=5, key="analysis_coverage_pct", help="Metadata keys with count >= this % of total are included",
            )
        else:
            coverage_pct = st.session_state.get("analysis_coverage_pct", 60)
    _coverage_frac = coverage_pct / 100.0

    for opt in RESOURCE_OPTIONS:
        st.session_state.setdefault(f"analysis_rt_{opt['value']}", True)
    rt_cols = st.columns(len(RESOURCE_OPTIONS) + 4)
    chosen_rts = []
    for ci, opt in enumerate(RESOURCE_OPTIONS):
        with rt_cols[ci]:
            checked = st.checkbox(opt["label"], key=f"analysis_rt_{opt['value']}")
            if checked:
                chosen_rts.append(opt["value"])

    with rt_cols[len(RESOURCE_OPTIONS)]:
        if _mode_val == "custom":
            btn_load_keys = st.button("Load keys", key="analysis_load_keys_btn", disabled=(not chosen_rts))
        else:
            btn_load_keys = False

    analysis_results = st.session_state.get("_analysis_results") or []
    combined_report = "\n\n".join(r["report"] for r in analysis_results) if analysis_results else ""

    first_slug = "all"
    if selected_ids and datasets_list:
        _fd2 = next((d for d in datasets_list if getattr(d, "id", None) is not None and int(getattr(d, "id", 0)) == int(selected_ids[0])), None)
        first_slug = slug_for_file_name(getattr(_fd2, "name", None) or getattr(_fd2, "external_id", None) or str(selected_ids[0]), 36) if _fd2 else "all"

    with rt_cols[len(RESOURCE_OPTIONS) + 1]:
        btn_run = st.button("Run analysis", type="primary", key="analysis_btn_run", disabled=(not chosen_rts))
    with rt_cols[len(RESOURCE_OPTIONS) + 2]:
        _dl_rts = "_".join(r["rt"] for r in analysis_results) if analysis_results else "all"
        _dl_mode = "custom" if _mode_val == "custom" else "auto"
        _dl_name = f"{slug_for_file_name(project, 24)}_{first_slug}_{_dl_rts}_{_dl_mode}_analysis.txt"
        st.download_button("Download report", data=combined_report, file_name=_dl_name, mime="text/plain", key="analysis_dl", disabled=(not analysis_results))
    with rt_cols[len(RESOURCE_OPTIONS) + 3]:
        btn_new = st.button("Clear", key="analysis_btn_new", disabled=(not analysis_results))

    if btn_load_keys and chosen_rts:
        _sel_ids_load = st.session_state.get("_selected_dataset_ids", [])
        _ds_ids_load = [{"id": i} for i in _sel_ids_load] if _sel_ids_load else None
        with st.spinner("Loading metadata keys…"):
            _custom = {}
            for _rt_load in chosen_rts:
                _rt_lbl = next((o["label"] for o in RESOURCE_OPTIONS if o["value"] == _rt_load), _rt_load)
                try:
                    _total_load = get_total_count(adapter, project, _rt_load, _ds_ids_load)
                    _meta_load = get_metadata_keys_list(adapter, _rt_load, project, _ds_ids_load)
                    _meta_load = _meta_load if isinstance(_meta_load, list) else []
                    _auto_sel = set(
                        select_filter_keys_for_analysis(_meta_load, _total_load, _rt_load, _coverage_frac)
                        if _total_load > 0 else (PRIMARY_FILTER_KEYS.get(_rt_load, []) or [])
                    )
                    _custom[_rt_load] = [{"key": m["key"], "count": m.get("count", 0), "auto": m["key"] in _auto_sel} for m in _meta_load]
                except Exception as _e:
                    _err_msg = str(_e)
                    if "403" in _err_msg or "Forbidden" in _err_msg:
                        st.warning(f"{_rt_lbl}: 403 Forbidden — insufficient access, skipped")
                    else:
                        st.warning(f"{_rt_lbl}: {_err_msg}, skipped")
            _prev_custom = st.session_state.get("_analysis_custom_keys", {})
            for _rt_clr in chosen_rts:
                st.session_state.pop(f"analysis_custom_sel_{_rt_clr}", None)
                for _old_item in _prev_custom.get(_rt_clr, []):
                    st.session_state.pop(f"analysis_ck_{_rt_clr}_{_old_item.get('key', '')}", None)
            st.session_state["_analysis_custom_keys"] = _custom

    _analysis_custom_keys = st.session_state.get("_analysis_custom_keys", {})
    _custom_selections = {}
    if _mode_val == "custom" and _analysis_custom_keys:
        _visible_rts = [rt for rt in chosen_rts if rt in _analysis_custom_keys]
        if _visible_rts:
            _ck_cols = st.columns(len(_visible_rts))
            for ci, _ck_rt in enumerate(_visible_rts):
                _ck_items = _analysis_custom_keys[_ck_rt]
                _rt_label = next((o["label"] for o in RESOURCE_OPTIONS if o["value"] == _ck_rt), _ck_rt)
                _sel_count = sum(1 for item in _ck_items if st.session_state.get(f"analysis_ck_{_ck_rt}_{item['key']}", item.get("auto", False)))
                with _ck_cols[ci]:
                    st.markdown(f"**{_rt_label}** &nbsp; <span style='color:#64748b;font-size:0.8rem;'>{_sel_count}/{len(_ck_items)} selected</span>", unsafe_allow_html=True)
                    _rt_selected = []
                    _sorted_items = sorted(_ck_items, key=lambda x: (not x.get("auto", False), -x.get("count", 0)))
                    with st.container(height=260):
                        for item in _sorted_items:
                            _cb_key = f"analysis_ck_{_ck_rt}_{item['key']}"
                            _default = item.get("auto", False)
                            _checked = st.checkbox(
                                f"{item['key']} ({item.get('count', 0):,})",
                                value=_default,
                                key=_cb_key,
                            )
                            if _checked:
                                _rt_selected.append(item["key"])
                    _custom_selections[_ck_rt] = _rt_selected

    if btn_run and chosen_rts:
        st.session_state["_analysis_all_pending"] = True
        st.session_state["_analysis_all_rts"] = list(chosen_rts)
        st.session_state["_analysis_coverage_frac"] = _coverage_frac
        st.session_state["_analysis_mode_for_run"] = _mode_val
        if _mode_val == "custom" and _custom_selections:
            st.session_state["_analysis_custom_selections"] = _custom_selections
        st.session_state.pop("_analysis_results", None)
        st.rerun()

    if btn_new:
        st.session_state.pop("_analysis_results", None)
        _prev_ck = st.session_state.pop("_analysis_custom_keys", {})
        for _clr_rt, _clr_items in (_prev_ck or {}).items():
            for _clr_item in _clr_items:
                st.session_state.pop(f"analysis_ck_{_clr_rt}_{_clr_item.get('key', '')}", None)
        st.rerun()

    if analysis_results:
        st.write("---")
        for res in analysis_results:
            rt_label = res.get("rt_label", "")
            count = res.get("count", 0)
            n_keys = len(res.get("keys", []))
            st.subheader(f"{rt_label} ({count:,} resources, {n_keys} metadata keys)")
            _report_html = html.escape(res["report"]).replace("\n", "<br>")
            st.markdown(
                '<div style="white-space:normal;word-wrap:break-word;overflow-wrap:break-word;'
                'font-size:0.85rem;line-height:1.5;">'
                f'{_report_html}</div>',
                unsafe_allow_html=True,
            )
            st.write("---")


if __name__ == "__main__":
    main()
