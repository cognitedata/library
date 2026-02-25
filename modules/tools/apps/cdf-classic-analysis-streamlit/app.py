"""
Classic CDF Analysis — Complete.
Combines single-key analysis and multi-resource-type deep analysis in one app
with a shared dataset section.
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
    run_analysis,
)
from analysis import (
    _aggregate_resource,
    _data_set_filter_aggregate,
    _documents_data_set_filter,
    _parse_count_response as _analysis_parse_count_response,
    _project_path,
)
_analysis_unwrap = getattr(_analysis_module, "_unwrap_maybe_coro", None)

from deep_analysis import PRIMARY_FILTER_KEYS, select_filter_keys_for_deep_analysis, slug_for_file_name

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


def get_client_and_project() -> tuple[Any, str] | None:
    """Build CogniteClient and project from env/secrets. Returns (client, project) or None."""
    try:
        from cognite.client import CogniteClient, ClientConfig
        from cognite.client.credentials import APIKey, OAuthClientCredentials
    except ImportError:
        st.error("Install cognite-sdk: pip install cognite-sdk")
        return None

    project = os.environ.get("COGNITE_PROJECT") or (st.secrets.get("COGNITE_PROJECT") if hasattr(st, "secrets") else None)
    base_url = os.environ.get("COGNITE_BASE_URL") or st.secrets.get("COGNITE_BASE_URL", "https://api.cognitedata.com")

    api_key = os.environ.get("COGNITE_API_KEY") or (st.secrets.get("COGNITE_API_KEY") if hasattr(st, "secrets") else None)
    if api_key and project:
        config = ClientConfig(
            client_name="classic-analysis-complete",
            project=project,
            credentials=APIKey(api_key),
            base_url=base_url,
        )
        client = CogniteClient(config)
        return (client, project)

    client_id = os.environ.get("COGNITE_CLIENT_ID") or (st.secrets.get("COGNITE_CLIENT_ID") if hasattr(st, "secrets") else None)
    client_secret = os.environ.get("COGNITE_CLIENT_SECRET") or (st.secrets.get("COGNITE_CLIENT_SECRET") if hasattr(st, "secrets") else None)
    tenant_id = os.environ.get("COGNITE_TENANT_ID") or (st.secrets.get("COGNITE_TENANT_ID") if hasattr(st, "secrets") else None)
    if client_id and client_secret and project:
        token_url = f"https://login.microsoftonline.com/{tenant_id or 'organizations'}/oauth2/v2.0/token"
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
        client = CogniteClient(config)
        return (client, project)

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
        n = fk.strip().lower()
        if n == "type": return ["type"]
        return ["metadata", fk.strip()]
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
        max_len = 512 if rt == "files" else 64
        if len(val) > max_len:
            mk = []
        else:
            up_body = {"aggregate": "uniqueProperties", "path": _meta_path(rt), **filter_part}
            if rt == "files":
                up_body["filter"] = {"equals": {"property": pp, "value": raw_val}}
            else:
                up_body["advancedFilter"] = {"equals": {"property": pp, "value": raw_val}}
            up = adapter.post(agg_path, up_body)
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
        label = api_key.title()
        count_str = f"Count: {cnt}\n" if cnt else ""
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
            "Configure CDF credentials. Set COGNITE_PROJECT and either COGNITE_API_KEY or "
            "COGNITE_CLIENT_ID + COGNITE_CLIENT_SECRET (and optionally COGNITE_TENANT_ID, COGNITE_BASE_URL) "
            "in environment or Streamlit secrets."
        )
        return

    client, project = client_and_project
    adapter = ClientAdapter(client, project)

    # ---- Deep analysis loading block (must be before UI so API calls run at top level) ----
    if st.session_state.get("_deep_all_pending"):
        _rts = st.session_state.pop("_deep_all_rts", ["assets"])
        _cov_frac = st.session_state.pop("_deep_coverage_frac", 0.6)
        _sel_ids = st.session_state.get("_selected_dataset_ids", [])
        _ds_ids = [{"id": i} for i in _sel_ids] if _sel_ids else None
        _ds_display = []
        for sid in _sel_ids:
            d = next((x for x in (st.session_state.get("datasets") or []) if getattr(x, "id", None) is not None and int(getattr(x, "id", 0)) == int(sid)), None)
            name = (getattr(d, "name", None) or getattr(d, "external_id", None) or str(sid)).strip() if d else str(sid)
            _ds_display.append(f"{name} ({sid})")
        _datasets_line = ", ".join(_ds_display) if _ds_display else "All datasets"
        try:
            with st.spinner("Running deep analysis…"):
                _all_results = []
                for ri, _rt in enumerate(_rts):
                    _rt_label = next((o["label"] for o in RESOURCE_OPTIONS if o["value"] == _rt), _rt)
                    print(f"[DEEP] [{ri+1}/{len(_rts)}] Starting {_rt_label}…")

                    agg_resource = "documents" if _rt == "files" else _rt
                    count_path = _project_path(project, agg_resource)
                    filter_part = _documents_data_set_filter(_ds_ids) if _rt == "files" else _data_set_filter_aggregate(_ds_ids)
                    count_res = adapter.post(count_path, {"aggregate": "count", **filter_part})
                    total_count = _analysis_parse_count_response(count_res)
                    print(f"[DEEP]   Count: {total_count:,}")

                    raw_meta = get_metadata_keys_list(adapter, _rt, project, _ds_ids)
                    raw_meta = raw_meta if isinstance(raw_meta, list) else []
                    sel_keys = select_filter_keys_for_deep_analysis(raw_meta, total_count, _rt, _cov_frac) if total_count > 0 else (PRIMARY_FILTER_KEYS.get(_rt, []) or ["type"])[:10]
                    print(f"[DEEP]   Metadata keys: {len(sel_keys)}")

                    header_lines = [
                        f"CDF Project: {project}", "",
                        f"Resource type: {_rt_label}",
                        f"Aggregate count: {total_count:,}",
                        f"Instance count threshold: {int(_cov_frac * 100)}%", "",
                        "Datasets:", "  - " + _datasets_line, "",
                        f"Metadata keys analysed: {len(sel_keys)}", "",
                        "---", "",
                    ]
                    lines = list(header_lines)
                    for ki, key in enumerate(sel_keys):
                        if not key:
                            continue
                        print(f"[DEEP]   Key [{ki+1}/{len(sel_keys)}]: {key}")
                        try:
                            lines.extend(_run_analysis_for_key(adapter, project, _rt, key, _ds_ids))
                        except Exception as e:
                            lines.append(f"=== Filter key: {key} ===")
                            lines.append("")
                            lines.append("Error: " + str(e))
                            lines.append("")

                    _pct = int(((ri + 1) / len(_rts)) * 100)
                    print(f"[DEEP]   Done — {_pct}% overall")
                    _all_results.append({
                        "report": "\n".join(lines),
                        "rt": _rt,
                        "rt_label": _rt_label,
                        "count": total_count,
                        "keys": sel_keys,
                    })

            st.session_state["_deep_results"] = _all_results
            st.session_state.pop("_deep_all_pending", None)
            st.rerun()
        except Exception as _e:
            print(f"[DEEP] ERROR: {_e}")
            st.session_state.pop("_deep_all_pending", None)
            st.error(f"Deep analysis failed: {_e}")

    # ========================================================================
    # All Datasets summary
    # ========================================================================
    st.subheader("All Datasets")
    if "all_counts" not in st.session_state:
        st.session_state.all_counts = None
    if st.session_state.all_counts is None:
        with st.spinner("Loading project counts…"):
            try:
                def _safe_count(rt: str) -> int:
                    try:
                        return get_aggregate_count_no_filter(adapter, project, rt)
                    except Exception:
                        return 0
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
                st.session_state.all_counts = counts
            except Exception as e:
                st.error(f"Failed to load counts: {e}")
                st.session_state.all_counts = {}

    all_counts = st.session_state.all_counts or {}
    cols = st.columns([2, 2, 2, 2, 2, 2, 2, 2, 2])
    headers = ["Assets", "Timeseries", "Events", "Sequences", "Files", "Transformations", "Functions", "Workflows", "Raw tables"]
    for i, h in enumerate(headers):
        cols[i].write("**" + h + "**")
    for i, k in enumerate(["assets", "timeseries", "events", "sequences", "files", "transformations", "functions", "workflows", "rawTables"]):
        v = all_counts.get(k)
        cols[i].write(f"{v:,}" if isinstance(v, (int, float)) else "—")

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
        if datasets and next_i < len(to_load) and not st.session_state.get("_deep_all_pending"):
            st.info("Loading resource counts…")
            batch_size = 10
            end_i = min(next_i + batch_size, len(to_load))
            with st.spinner("Loading resource counts…"):
                for d in to_load[next_i:end_i]:
                    try:
                        st.session_state.setdefault("dataset_counts", {})[d.id] = get_dataset_resource_counts(adapter, project, {"id": d.id})
                    except Exception:
                        st.session_state.setdefault("dataset_counts", {})[d.id] = {"assets": 0, "timeseries": 0, "events": 0, "sequences": 0, "files": 0}
            st.session_state.dataset_counts_next = end_i
            st.rerun()

        if "_selected_dataset_ids" not in st.session_state:
            st.session_state["_selected_dataset_ids"] = []

        if datasets:
            if st.session_state.pop("_clear_dataset_selection", False):
                st.session_state["_dataset_editor_version"] = st.session_state.get("_dataset_editor_version", 0) + 1
                for key in ("metadata_keys_list", "metadata_keys_list_rt", "metadata_keys_list_dataset_ids", "last_result", "_selected_dataset_ids"):
                    if key in st.session_state:
                        del st.session_state[key]

            editor_version = st.session_state.get("_dataset_editor_version", 0)
            editor_key = f"dataset_editor_v{editor_version}"
            prev_editor = st.session_state.get(editor_key)
            show_zero_count = st.session_state.get("show_zero_count_datasets", False)
            selected_ids_set = {str(x) for x in st.session_state.get("_selected_dataset_ids", [])}

            def _fmt_cell(x):
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

    _current_selection_key = tuple(sorted(selected_ids)) if selected_ids else ()
    if st.session_state.get("metadata_keys_list_dataset_ids") != _current_selection_key:
        for key in ("metadata_keys_list", "metadata_keys_list_rt", "metadata_keys_list_dataset_ids"):
            if key in st.session_state:
                del st.session_state[key]

    # ========================================================================
    # Run analysis (single resource type + filter key) — compact layout
    # ========================================================================
    st.write("---")
    st.subheader("Run analysis")

    metadata_keys_list = st.session_state.get("metadata_keys_list", [])
    metadata_keys_rt = st.session_state.get("metadata_keys_list_rt")

    col_rt, col_fk = st.columns([1, 3])
    with col_rt:
        resource_type = st.selectbox(
            "Resource type",
            options=[o["value"] for o in RESOURCE_OPTIONS],
            format_func=lambda x: next(o["label"] for o in RESOURCE_OPTIONS if o["value"] == x),
            key="resource_type",
        )
    with col_fk:
        filter_key_placeholder = "Select or type a key"
        if resource_type == "timeseries":
            filter_key_placeholder += ' (e.g. "is step", "is string", "unit")'
        if resource_type == "files":
            filter_key_placeholder += ' ("type", "labels", "author", "source")'

        _selection_matches = st.session_state.get("metadata_keys_list_dataset_ids") == _current_selection_key
        if metadata_keys_rt == resource_type and _selection_matches:
            if not metadata_keys_list:
                filter_key_raw = st.selectbox(
                    "Filter key (metadata or \"type\")",
                    options=["No metadata available"],
                    index=0,
                    key="filter_key_select",
                )
                filter_key_raw = "" if filter_key_raw == "No metadata available" else filter_key_raw
            else:
                options = [""] + [f"{x['key']} ({x['count']})" for x in metadata_keys_list]
                filter_key_raw = st.selectbox(
                    "Filter key (metadata or \"type\")",
                    options=options,
                    key="filter_key_select",
                ) or ""
        else:
            filter_key_raw = st.text_input(
                "Filter key (metadata or \"type\")",
                value="",
                placeholder=filter_key_placeholder,
                key="filter_key_input",
            ) or ""

    result = st.session_state.get("last_result")
    _s1, _s2, col_load, _s3, _s4, col_run_s, col_dl_s, col_clr_s = st.columns([1, 1, 1, 1, 1, 1, 1, 1])
    with col_load:
        btn_load_keys = st.button("Load metadata keys", key="load_keys")
    with col_run_s:
        run_single = st.button("Run analysis", type="primary", key="run_single")
    with col_dl_s:
        _has_result_rows = bool(result and result.get("rows"))
        txt = "".join(r.get("text", "") for r in (result or {}).get("rows", [])) if _has_result_rows else ""
        project_slug = slug_for_file_name(project, 24)
        _first_slug = "all"
        if selected_ids and datasets_list:
            _fd = next((d for d in datasets_list if getattr(d, "id", None) is not None and int(getattr(d, "id", 0)) == int(selected_ids[0])), None)
            _first_slug = slug_for_file_name(getattr(_fd, "name", None) or getattr(_fd, "external_id", None) or str(selected_ids[0]), 36) if _fd else "all"
        _dl_fname = f"{project_slug}_{_first_slug}_{(result or {}).get('resourceType', 'out')}_analysis_{(result or {}).get('filterKey', 'key')}.txt"
        st.download_button("Download .txt", data=txt, file_name=_dl_fname, mime="text/plain", key="download_txt", disabled=(not _has_result_rows))
    with col_clr_s:
        btn_clear_result = st.button("Clear", key="clear_result", disabled=(not result))

    if btn_load_keys:
        with st.spinner("Loading…"):
            try:
                lst = get_metadata_keys_list(adapter, resource_type, project, data_set_ids_for_api)
                lst = _ensure_sync(lst) if lst is not None else []
                lst = lst if isinstance(lst, list) else []
                if data_set_ids_for_api:
                    try:
                        total = get_total_count(adapter, project, resource_type, data_set_ids_for_api)
                        total = _ensure_sync(total)
                        total = int(total) if isinstance(total, (int, float)) else 0
                        if total <= 0:
                            lst = []
                    except Exception:
                        lst = []
                st.session_state.metadata_keys_list = lst
                st.session_state.metadata_keys_list_rt = resource_type
                st.session_state.metadata_keys_list_dataset_ids = _current_selection_key
                st.rerun()
            except Exception as e:
                st.error(str(e))

    if run_single:
        raw_key = (filter_key_raw or "").strip()
        if not raw_key or raw_key.lower() == "no metadata":
            st.warning("Enter a filter key (or load metadata keys and choose one).")
        else:
            fk = filter_key_for_api(raw_key)
            with st.spinner("Running…"):
                try:
                    result = run_analysis(adapter, resource_type, fk, project, data_set_ids_for_api)
                    st.session_state.last_result = result
                    st.rerun()
                except Exception as e:
                    st.session_state.last_result = {"resourceType": resource_type, "filterKey": fk, "rows": [], "error": str(e)}
                    st.rerun()

    if btn_clear_result:
        st.session_state.pop("last_result", None)
        st.rerun()

    if result:
        if result.get("error"):
            st.error(result["error"])
        st.caption(f"**{len(result.get('rows', []))}** entries (sorted by count descending)")
        if result.get("rows"):
            wrap_style = (
                "white-space: pre-wrap; word-wrap: break-word; overflow-wrap: break-word; "
                "word-break: break-word; margin-bottom: 0.3rem; font-family: inherit; font-size: 0.85rem;"
            )
            inner = "".join(
                f'<div style="{wrap_style}">{html.escape(r.get("text", ""))}</div>' for r in result["rows"]
            )
            st.markdown(
                f'<div style="min-width:0;overflow:visible;box-sizing:border-box;">{inner}</div>',
                unsafe_allow_html=True,
            )

    # ========================================================================
    # Deep analysis (multi-resource-type, all keys)
    # ========================================================================
    st.write("---")
    st.subheader("Deep analysis")

    _cov_col, _cov_hint = st.columns([1, 3])
    with _cov_col:
        coverage_pct = st.number_input(
            "Instance count threshold (%)", min_value=0, max_value=100, value=60,
            step=5, key="deep_coverage_pct", help="Metadata keys with count >= this % of total are included",
        )
    with _cov_hint:
        st.markdown(
            '<p style="font-size:0.875rem; color:rgb(49,51,63); margin-top:2.1rem;">'
            'The deep analysis may take some time. Results will show below. '
            'See progress with Inspect‑Console‑Filter("DEEP")</p>',
            unsafe_allow_html=True,
        )
    _coverage_frac = coverage_pct / 100.0

    rt_cols = st.columns(len(RESOURCE_OPTIONS) + 3)
    chosen_rts = []
    for ci, opt in enumerate(RESOURCE_OPTIONS):
        with rt_cols[ci]:
            checked = st.checkbox(opt["label"], value=True, key=f"deep_rt_{opt['value']}")
            if checked:
                chosen_rts.append(opt["value"])

    deep_results = st.session_state.get("_deep_results") or []
    combined_report = "\n\n".join(r["report"] for r in deep_results) if deep_results else ""

    first_slug = "all"
    if selected_ids and datasets_list:
        _fd2 = next((d for d in datasets_list if getattr(d, "id", None) is not None and int(getattr(d, "id", 0)) == int(selected_ids[0])), None)
        first_slug = slug_for_file_name(getattr(_fd2, "name", None) or getattr(_fd2, "external_id", None) or str(selected_ids[0]), 36) if _fd2 else "all"

    with rt_cols[len(RESOURCE_OPTIONS)]:
        btn_run = st.button("Run deep analysis", type="primary", key="deep_btn_run", disabled=(not chosen_rts))
    with rt_cols[len(RESOURCE_OPTIONS) + 1]:
        _dl_rts = "_".join(r["rt"] for r in deep_results) if deep_results else "all"
        _dl_name = f"{slug_for_file_name(project, 24)}_{first_slug}_{_dl_rts}_deep_analysis.txt"
        st.download_button("Download report", data=combined_report, file_name=_dl_name, mime="text/plain", key="deep_dl", disabled=(not deep_results))
    with rt_cols[len(RESOURCE_OPTIONS) + 2]:
        btn_new = st.button("Clear", key="deep_btn_new", disabled=(not deep_results))

    if btn_run and chosen_rts:
        st.session_state["_deep_all_pending"] = True
        st.session_state["_deep_all_rts"] = list(chosen_rts)
        st.session_state["_deep_coverage_frac"] = _coverage_frac
        st.session_state.pop("_deep_results", None)
        st.rerun()

    if btn_new:
        st.session_state.pop("_deep_results", None)
        st.rerun()

    if deep_results:
        st.write("---")
        for res in deep_results:
            rt_label = res.get("rt_label", "")
            count = res.get("count", 0)
            n_keys = len(res.get("keys", []))
            st.subheader(f"{rt_label} ({count:,} resources, {n_keys} metadata keys)")
            st.text(res["report"])
            st.write("---")


if __name__ == "__main__":
    main()
