# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "polars>=1.0.0",
#     "altair>=5.0.0,<6.0.0",
#     "python-dotenv==1.2.1",
#     "pyarrow>=18.0.0",
#     "cognite-toolkit==0.7.69",
#     "wigglystuff>=0.1.0",
# ]
# ///

import marimo

__generated_with = "0.19.7"
app = marimo.App(width="columns", app_title="TSJM bench")

with app.setup(hide_code=True):
    # Standard library imports
    import json
    import os
    import re
    import time
    import traceback
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from pathlib import Path
    from typing import Any
    import datetime

    # Third-party library imports
    import marimo as mo
    import polars as pl
    import altair as alt
    import pandas as pd
    from dotenv import load_dotenv
    from wigglystuff import CellTour
    from cognite.client.credentials import OAuthInteractive
    from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


    # Configure Altair to disable download menu
    alt.renderers.set_embed_options(actions=False)

    #
    # monkeypatch Cognite Python SDK, to enforce an account-select and not fail with an already cached and logged in account
    # this patch is depends on Cognite SDK not changing this internal implementation!
    # this patch was implemented using v7.91.1 of the SDK as reference:
    #    https://github.com/cognitedata/cognite-sdk-python/blob/cognite-sdk-python-v7.91.1/cognite/client/credentials.py#L683-L696
    #
    # Store original method before patching
    _original_refresh_access_token = OAuthInteractive._refresh_access_token


    def _refresh_access_token_patch(self) -> tuple[str, float]:
        """
        Patched version of _refresh_access_token that adds 'prompt="select_account"'
        to the interactive token acquisition call.

        This allows users to select which account to use when multiple accounts are cached.
        """
        # Access private attributes using name-mangled form (Python name mangling)
        # __app becomes _OAuthInteractive__app, __scopes becomes _OAuthInteractive__scopes, etc.
        app = getattr(self, "_OAuthInteractive__app")
        scopes = getattr(self, "_OAuthInteractive__scopes")
        redirect_port = getattr(self, "_OAuthInteractive__redirect_port")

        # First check if a token cache exists on disk. If yes, find and use:
        # - A valid access token.
        # - A valid refresh token, and if so, use it automatically to redeem a new access token.
        credentials = None
        if accounts := app.get_accounts():
            credentials = app.acquire_token_silent(scopes=scopes, account=accounts[0])

        # If we're unable to find (or acquire a new) access token, we initiate the interactive auth flow:
        if credentials is None:
            # https://msal-python.readthedocs.io/en/latest/#msal.PublicClientApplication.acquire_token_interactive
            # https://msal-python.readthedocs.io/en/latest/#msal.Prompt
            # Add prompt="select_account" to allow account selection when multiple accounts are cached
            credentials = app.acquire_token_interactive(scopes=scopes, port=redirect_port, prompt="select_account")

        self._verify_credentials(credentials)
        return credentials["access_token"], time.time() + float(credentials["expires_in"])


    # Apply monkeypatch
    OAuthInteractive._refresh_access_token = _refresh_access_token_patch


@app.cell(hide_code=True)
def import_marimo_and_title():
    mo.md("""
    # TSJM Analysis Notebook

    Welcome to the CDF Transformation-Job-Metrics (TSJM) Analysis Tool!
    """)
    return


@app.cell(hide_code=True)
def create_notebook_tour():
    """Interactive notebook tour using wigglystuff CellTour."""

    # TODO: references by `cell_name` are not working, so we use the `cell` index instead
    # issue opened here: https://github.com/koaning/wigglystuff/issues/115
    tour = CellTour(
        steps=[
            {
                # "cell_name": "chapter1_setup_header",
                "cell": 3,
                "title": "1. Setup",
                "description": "Configure your CDF connection with .env credentials",
            },
            {
                # "cell_name": "chapter2_export_header",
                "cell": 10,
                "title": "2. Export from CDF",
                "description": "Export transformation job metrics from CDF API",
            },
            {
                # "cell_name": "chapter3_loading_header",
                "cell": 14,
                "title": "3. Load Data",
                "description": "Load previously exported JSONL files for analysis",
            },
            {
                # "cell_name": "chapter4_concurrency_header",
                "cell": 18,
                "title": "4. Concurrency Dashboard",
                "description": "Visualize when transformation jobs overlap",
            },
            {
                # "cell_name": "chapter5_metrics_header",
                "cell": 26,
                "title": "5. Metrics Dashboard",
                "description": "Explore metrics trends per transformation",
            },
            {
                # "cell_name": "chapter6_export_header",
                "cell": 36,
                "title": "6. Save Results",
                "description": "Export analysis results to CSV, Parquet, or JSON",
            },
            {
                # "cell_name": "chapter6_export_header",
                "cell": 3,
                "title": "1. Setup",
                "description": "and back to the start",
            },
        ],
        auto_start=False,
        show_progress=True,
    )

    mo.callout(
        mo.vstack(
            [
                mo.md("""Functionality overview:
                - Chapter 1: Setup and Configuration
                - Chapter 2: Export from CDF to disk
                - Chapter 3: Load Data from disk (created by the export)
                - Chapter 4: TSJM Concurrency Dashboard
                - Chapter 5: TSJM Metrics Dashboard
                - Chapter 6: Save selected Results
                """),
                mo.md("""🎯 **New here?
                **Click the button below for a guided tour of this chapters.
                """),
                tour,
            ]
        ),
        kind="info",
    )
    return


@app.cell(hide_code=True)
def chapter1_setup_header():
    # mo is available globally from app.setup
    mo.md("""
    ## Chapter 1: Setup
    """)
    return


@app.cell(hide_code=True)
def import_core_libraries():
    # All imports are now in app.setup block and available globally
    # This cell exists for backward compatibility with cells that depend on these imports
    return


@app.cell(hide_code=True)
def define_date_range_helpers():
    """
    Reusable date range selector factory.
    Creates consistent UI for date range selection across Concurrency and Metrics chapters.
    """
    # datetime and re are available globally from app.setup


    def create_date_range_ui(mo, min_date, max_date, title="Date Range", data_info=None):
        """
        Create a unified date range selector with dropdown presets and date_range picker.

        Args:
            mo: marimo module
            min_date: Minimum date in the data
            max_date: Maximum date in the data
            title: Section title
            data_info: Optional info string about data availability

        Returns:
            Form with preset dropdown and date_range picker
        """
        date_form = mo.ui.batch(
            mo.md("""
    **Quick select:** {preset}

    **Date range** _(only used when "Custom range" selected)_: {date_range}
            """),
            {
                "preset": mo.ui.dropdown(
                    options=[
                        "Last 7 days",
                        "Last 14 days",
                        "Last 30 days",
                        "Last 60 days",
                        "Last 90 days",
                        "Custom range",
                        "All data",
                    ],
                    value="Last 7 days",
                ),
                "date_range": mo.ui.date_range(
                    start=min_date,
                    stop=max_date,
                    value=(max_date - datetime.timedelta(days=6), max_date),
                ),
            },
        ).form(submit_button_label="Apply")

        _info_text = f"_{data_info}_" if data_info else ""
        output = mo.vstack(
            [
                mo.md(f"### {title}"),
                date_form,
                mo.md(_info_text) if _info_text else mo.md(""),
            ]
        )
        return date_form, output


    def calculate_date_range_from_form(form_value, min_date, max_date):
        """
        Calculate effective start/end dates based on form submission.

        Args:
            form_value: The form.value dict (or None if not submitted)
            min_date: Minimum date in the data
            max_date: Maximum date in the data

        Returns:
            Tuple of (start_date, end_date)
        """
        # Default: last 7 days
        start_date = max_date - datetime.timedelta(days=6)
        end_date = max_date

        if form_value is not None:
            preset = form_value["preset"]
            date_range = form_value["date_range"]

            if preset == "All data":
                start_date = min_date
                end_date = max_date
            elif preset == "Custom range":
                # Use the date_range picker values
                if date_range:
                    start_date, end_date = date_range
            else:
                # Parse "Last N days" format
                match = re.search(r"Last (\d+) days", preset)
                days = int(match.group(1)) if match else 7
                start_date = max_date - datetime.timedelta(days=days - 1)
                end_date = max_date

        return start_date, end_date
    return calculate_date_range_from_form, create_date_range_ui


@app.cell(hide_code=True)
def create_config_form():
    # mo is available globally from app.setup
    """
    Configuration form for CDF connection using cognite-toolkit .env format.

    Expected .env format (compatible with `cdf auth init`):
        CDF_CLUSTER=westeurope-1
        CDF_PROJECT=my-project
        PROVIDER=entra_id
        LOGIN_FLOW=interactive
        IDP_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        IDP_CLIENT_ID=yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy
    """

    config_form = mo.ui.batch(
        mo.md("""
    ### Configuration

    Provide the path to your `.env` file (cognite-toolkit format) and output folder.

    **`.env` File Path:** {env_path}

    **Output Folder:** {output_path}
        """),
        {
            "env_path": mo.ui.text(
                value=".env",
                full_width=True,
            ),
            "output_path": mo.ui.text(
                value="./",
                full_width=True,
            ),
        },
    ).form(submit_button_label="🔗 Connect to CDF")

    config_form
    return (config_form,)


@app.cell(hide_code=True)
def init_cdf_client(config_form):
    """
    Initialize CogniteClient using cognite-toolkit's EnvironmentVariables.

    This uses the same authentication logic as `cdf-toolkit` CLI,
    supporting all login flows: interactive, client_credentials, device_code, token.
    """
    # Path, mo, os, load_dotenv, EnvironmentVariables are available globally from app.setup

    cdf_client = None
    env_vars = None
    available_projects: list[str] = []
    output_folder_value = None

    if config_form.value is None:
        _output = mo.callout(
            mo.md("👆 **Fill in the configuration above and click Submit** to connect to CDF."),
            kind="info",
        )
    else:
        # Expand ~ in paths and resolve to absolute
        env_path = Path(config_form.value["env_path"]).expanduser().resolve()
        output_folder = Path(config_form.value["output_path"]).expanduser().resolve()
        output_folder_value = str(output_folder)  # Store as resolved string

        # Validate output folder exists and is writable
        _output_folder_error = None
        if not output_folder.exists():
            _output_folder_error = f"Output folder does not exist: `{output_folder}`"
        elif not output_folder.is_dir():
            _output_folder_error = f"Output path is not a directory: `{output_folder}`"
        elif not os.access(output_folder, os.W_OK):
            _output_folder_error = f"Output folder is not writable: `{output_folder}`"

        if _output_folder_error:
            _output = mo.callout(
                mo.md(f"⚠️ **{_output_folder_error}**\n\nPlease create the folder or choose a different path."),
                kind="warn",
            )
        elif not env_path.exists():
            _output = mo.callout(
                mo.md(
                    f"⚠️ **Config file not found:** `{env_path}`\n\n"
                    "Create one using `cdf auth init` or manually with toolkit format."
                ),
                kind="warn",
            )
        else:
            try:
                # Load .env file into os.environ
                load_dotenv(env_path, override=True)

                # Use toolkit's EnvironmentVariables to parse and create client

                env_vars = EnvironmentVariables.create_from_environment()

                # Get client using toolkit's authentication
                cdf_client = env_vars.get_client()

                # Override client name for identification
                cdf_client.config.client_name = "inso-tsjm-exporter-marimo"

                # Get available projects from token inspection
                _token_info = cdf_client.iam.token.inspect()
                available_projects = sorted([p.project_url_name for p in _token_info.projects])

                _output = mo.md(f"""
    ### Environment Configuration

    ✅ Loaded config from `{env_path.name}`

    | Setting | Value |
    |---------|-------|
    | Cluster | `{env_vars.CDF_CLUSTER}` |
    | Default Project | `{env_vars.CDF_PROJECT}` |
    | Provider | `{env_vars.PROVIDER}` |
    | Login Flow | `{env_vars.LOGIN_FLOW}` |
    | Output Folder | `{output_folder}` |

    ### CDF Connection

    **Status:** 🟢 Connected ({len(available_projects)} projects available)
                """)

            except Exception as e:
                _output = mo.callout(
                    mo.md(f"🔴 **Connection failed:** `{type(e).__name__}: {str(e)}`"),
                    kind="danger",
                )

    _output
    return available_projects, cdf_client, output_folder_value


@app.cell(hide_code=True)
def create_project_selector(available_projects: list[str], cdf_client):
    # mo and pd are available globally from app.setup

    # Project selector
    project_selector = None

    if cdf_client is not None and available_projects:
        # Create simple project list for selection
        _project_data = [{"CDF Project": p} for p in available_projects]

        project_selector = mo.ui.table(
            pd.DataFrame(_project_data),
            selection="single",
            label="Select a CDF project:",
        )

        _output = mo.vstack(
            [
                mo.md("### Select CDF Project"),
                project_selector,
            ]
        )
    else:
        _output = mo.callout(
            mo.md("⚠️ **Connect to CDF first** to see available projects."),
            kind="warn",
        )

    _output
    return (project_selector,)


@app.cell(hide_code=True)
def get_selected_project(
    available_projects: list[str],
    cdf_client,
    project_selector,
):
    # Get selected project and update client
    # NOTE: Requires explicit user selection - no auto-selection from .env default
    selected_project = None
    active_client = None

    if project_selector is not None and cdf_client is not None:
        if project_selector.value is not None and not project_selector.value.empty:
            # User explicitly selected a project from the table
            selected_project = project_selector.value.iloc[0]["CDF Project"]

            # Update the client's project configuration
            cdf_client.config.project = selected_project
            active_client = cdf_client

            _output = mo.callout(
                mo.md(f"✅ **Active Project:** `{selected_project}`"),
                kind="success",
            )
        else:
            _output = mo.callout(
                mo.md("👆 **Select a Project**\n\nClick a row in the table above to activate it."),
                kind="info",
            )
    elif available_projects:
        _output = mo.callout(
            mo.md("👆 **Select a Project**\n\nClick a row in the table above to activate it."),
            kind="info",
        )
    else:
        _output = mo.callout(
            mo.md("⏳ **Waiting for Projects**\n\nConnect to CDF first to see available projects."),
            kind="info",
        )

    _output
    return active_client, selected_project


@app.cell(hide_code=True)
def chapter2_export_header():
    # mo is available globally from app.setup
    mo.md("""
    ## Chapter 2: Export from CDF

    Export transformation job metrics from CDF using parallel processing.
    """)
    return


@app.cell(hide_code=True)
def create_export_controls(
    active_client,
    output_folder_value,
    selected_project,
):
    # Check if client is ready
    client_ready = active_client is not None and selected_project is not None

    # Get total transformation count (fetches metadata only, relatively fast)
    total_transformations = 0
    if client_ready:
        total_transformations = len(list(active_client.transformations.list(limit=None)))


    # Export configuration controls
    max_workers_slider = mo.ui.slider(
        start=1,
        stop=16,
        value=4,
        step=1,
        label="Max Parallel Workers",
        show_value=True,
    )

    jobs_per_trafo_slider = mo.ui.slider(
        start=10,
        stop=1000,
        value=10,
        step=100,
        label="Jobs per Transformation",
        show_value=True,
    )


    status_indicator = "🟢" if client_ready else "🔴"
    status_text = f"Connected to `{selected_project}`" if client_ready else "Select a project in Chapter 1 first"

    # Generate export filename preview (use project name directly)
    datestamp_preview = datetime.datetime.now(datetime.UTC).strftime("%y%m%d")
    _output_folder = output_folder_value or "/tmp"
    export_filename = f"{datestamp_preview}-{selected_project or 'unknown'}-tsjm-export.jsonl"
    export_filepath = Path(_output_folder) / export_filename

    # Export button (only enabled if client is ready)
    export_button = mo.ui.run_button(
        label="🚀 Start Export",
        disabled=not client_ready,
    )

    if client_ready:
        # Transformation limit with "Load all" checkbox
        _trafo_limit_label = (
            f"Transformation Limit ({total_transformations} available)"
            if total_transformations > 0
            else "no transformations available in this project"
        )
        trafo_limit_input = mo.ui.number(
            start=0,
            stop=total_transformations,
            value=min(10, total_transformations) if total_transformations > 0 else 1,
            step=10,
            label=_trafo_limit_label,
        )
        trafo_load_all = mo.ui.checkbox(label="Load all", value=False)

        _output = mo.vstack(
            [
                mo.md("""
    |         Export settings | Defaults | for bulk download |
    | ----------------------: | -------: | ----------------: |
    |             Max Workers |        4 |              8-12 |
    | Jobs per Transformation |       10 |          100-1000 |
    |    Transformation Limit |       10 |       ☑ Load all  |
                """),
                mo.hstack(
                    [
                        mo.md(f"**CDF Connection:** {status_indicator} {status_text}"),
                    ],
                    justify="start",
                ),
                mo.md(f"**Output File:** `{export_filename}`"),
                mo.md("---"),
                max_workers_slider,
                jobs_per_trafo_slider,
                mo.hstack([trafo_limit_input, trafo_load_all], justify="start", align="end", gap=1),
                export_button,
            ]
        )
    else:
        trafo_load_all = None
        _output = mo.callout(
            mo.md(f"⚠️ **{status_text}** before you can export."),
            kind="warn",
        )

    _output
    return (
        export_button,
        export_filepath,
        jobs_per_trafo_slider,
        max_workers_slider,
        trafo_limit_input,
        trafo_load_all,
    )


@app.cell(hide_code=True)
def define_export_functions():
    # json, threading, traceback, ThreadPoolExecutor, as_completed, Any are available globally from app.setup


    def process_single_transformation(args: tuple[int, Any, str, int, Any]) -> list[dict[str, Any]]:
        """
        Process a single transformation and all its jobs.

        Args:
            args: tuple of (ts_no, ts, project, jobs_per_transformation, client)

        Returns:
            List of job metric dictionaries
        """
        ts_no, ts, project, jobs_per_transformation, thread_client = args

        results: list[dict[str, Any]] = []
        try:
            for j, tsj in enumerate(
                thread_client.transformations.jobs.list(transformation_id=ts.id, limit=jobs_per_transformation)
            ):
                results.append(
                    dict(
                        project=project,
                        ts_no=ts_no,
                        ts_id=ts.id,
                        ts_external_id=ts.external_id,
                        tsj_no=j,
                        tsj_job_id=tsj.id,
                        tsj_created_time=tsj.created_time,
                        tsj_started_time=tsj.started_time,
                        tsj_finished_time=tsj.finished_time,
                        tsj_last_seen_time=tsj.last_seen_time,
                        tsj_error=tsj.error,
                        tsj_status=tsj.status.value,
                        tsjm_last_counts=(
                            tsjm.to_pandas().groupby("name")["count"].last().to_json() if (tsjm := tsj.metrics()) else "{}"
                        ),
                    )
                )
        except Exception as e:
            error_msg = f"Error processing transformation {ts.external_id} (ts_no={ts_no}): {type(e).__name__}: {str(e)}"
            print(error_msg)
            print(f"Traceback:\n{traceback.format_exc()}")
            return []

        return results


    def process_parallel_and_dump(
        transformations: list[Any],
        project: str,
        output_file: str,
        jobs_per_transformation: int,
        client: Any,
        max_workers: int = 8,
        batch_size: int = 100,
        append: bool = False,
        progress_callback: Any = None,
    ) -> tuple[int, list[str]]:
        """
        Process transformations in parallel and write to a single JSONL file.

        Args:
            transformations: List of transformation objects
            project: Project name
            output_file: Path to output JSONL file
            jobs_per_transformation: Max jobs per transformation
            client: CogniteClient instance
            max_workers: Number of parallel workers
            batch_size: Batch size for writing
            append: Whether to append to existing file
            progress_callback: Optional callback for progress updates

        Returns:
            Tuple of (total rows processed, list of error messages)
        """
        transformation_args = [(i, ts, project, jobs_per_transformation, client) for i, ts in enumerate(transformations)]

        write_lock = threading.Lock()
        buffer: list[dict[str, Any]] = []
        first_write = not append
        errors: list[str] = []

        def write_batch_to_file(items: list[dict[str, Any]], force: bool = False) -> None:
            nonlocal buffer, first_write
            try:
                with write_lock:
                    buffer.extend(items)
                    if len(buffer) >= batch_size or force:
                        if buffer:
                            mode = "a" if append or not first_write else "w"
                            with open(output_file, mode) as f:
                                f.write("\n".join(json.dumps(item) for item in buffer) + "\n")
                            buffer.clear()
                            first_write = False
            except Exception as e:
                error_msg = f"Error writing to file: {type(e).__name__}: {str(e)}"
                print(error_msg)
                raise

        total_processed = 0
        completed = 0
        total = len(transformations)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_ts = {executor.submit(process_single_transformation, args): args[1] for args in transformation_args}

            for future in as_completed(future_to_ts):
                ts = future_to_ts[future]
                completed += 1
                try:
                    results = future.result()
                    if results:
                        write_batch_to_file(results)
                        total_processed += len(results)
                    if progress_callback:
                        progress_callback(completed, total, ts.external_id)
                except Exception as e:
                    error_msg = f"Error processing {ts.external_id}: {type(e).__name__}: {str(e)}"
                    print(error_msg)
                    errors.append(error_msg)

        write_batch_to_file([], force=True)
        return total_processed, errors
    return (process_single_transformation,)


@app.cell(hide_code=True)
def run_tsjm_export(
    active_client,
    export_button,
    export_filepath,
    jobs_per_trafo_slider,
    max_workers_slider,
    process_single_transformation,
    selected_project,
    trafo_limit_input,
    trafo_load_all,
):
    # ThreadPoolExecutor, as_completed, json, mo, threading, time are available globally from app.setup
    export_output = mo.callout(
        mo.md("👆 **Ready to Export**\n\nConfigure limits above and click **Start Export** to download metrics."),
        kind="info",
    )
    export_result = {}  # dict with export results

    if export_button.value and active_client is not None and selected_project is not None:
        try:
            # Start timing
            start_time = time.time()

            # Use the pre-initialized client from Chapter 1
            client = active_client

            # List transformations
            transformations = list(client.transformations.list(limit=None))

            # Apply transformation limit (checkbox "Load all" overrides slider)
            total_available = len(transformations)
            if trafo_load_all and trafo_load_all.value:
                trafo_limit = 0  # 0 means no limit (load all)
            else:
                trafo_limit = trafo_limit_input.value
                transformations = transformations[:trafo_limit]

            # Check if no transformations to process
            if len(transformations) == 0:
                export_result = {
                    "status": "empty",
                    "rows": 0,
                    "file": None,
                    "transformations": 0,
                    "transformations_available": total_available,
                    "elapsed_seconds": time.time() - start_time,
                    "errors": [],
                }
                export_output = mo.md(
                    f"""
                    ### ⚠️ No Transformations Found

                    No transformations found in project `{selected_project}`.

                    | Metric | Value |
                    |--------|-------|
                    | Transformations Available | {total_available} |

                    **Tip:** Check that transformations exist in project `{selected_project}`.
                    """
                )
            else:
                # Use pre-computed output filepath
                output_file = export_filepath
                batch_size = 100

                # Prepare arguments for each transformation
                transformation_args = [
                    (i, ts, selected_project, jobs_per_trafo_slider.value, client) for i, ts in enumerate(transformations)
                ]

                # Thread-safe file writing
                write_lock = threading.Lock()
                write_state = {"buffer": [], "first_write": True}
                total_rows = 0
                export_errors = []

                def write_batch_to_file(items, force=False):
                    with write_lock:
                        write_state["buffer"].extend(items)
                        if len(write_state["buffer"]) >= batch_size or force:
                            if write_state["buffer"]:
                                mode = "w" if write_state["first_write"] else "a"
                                with open(str(output_file), mode) as f:
                                    f.write("\n".join(json.dumps(item) for item in write_state["buffer"]) + "\n")
                                write_state["buffer"].clear()
                                write_state["first_write"] = False

                # Submit all tasks
                with ThreadPoolExecutor(max_workers=max_workers_slider.value) as executor:
                    future_to_ts = {
                        executor.submit(process_single_transformation, args): args[1] for args in transformation_args
                    }

                    # Process with marimo progress bar - pass iterator directly with total
                    for future in mo.status.progress_bar(
                        as_completed(future_to_ts),
                        total=len(future_to_ts),
                        title="Exporting TSJM",
                        subtitle=f"Processing {len(transformations)} transformations",
                        show_eta=True,
                        show_rate=True,
                    ):
                        ts = future_to_ts[future]
                        try:
                            results = future.result()
                            if results:
                                write_batch_to_file(results)
                                total_rows += len(results)
                        except Exception as e:
                            error_msg = f"Error processing {ts.external_id}: {type(e).__name__}: {str(e)}"
                            print(error_msg)
                            export_errors.append(error_msg)

                # Flush remaining buffer
                write_batch_to_file([], force=True)

                # Calculate elapsed time
                elapsed_time = time.time() - start_time
                elapsed_str = f"{elapsed_time / 60:.1f} minutes" if elapsed_time >= 60 else f"{elapsed_time:.1f} seconds"

                # Determine status based on errors
                if export_errors:
                    error_count = len(export_errors)
                    # Check for critical errors like missing pandas
                    has_critical_error = any("CogniteImportError" in e or "pandas" in e.lower() for e in export_errors)

                    if has_critical_error:
                        export_result = {
                            "status": "failed",
                            "rows": total_rows,
                            "file": str(output_file),
                            "transformations": len(transformations),
                            "transformations_available": total_available,
                            "elapsed_seconds": elapsed_time,
                            "errors": export_errors,
                        }
                        # Show first few errors
                        error_sample = "\n".join(f"- {e}" for e in export_errors[:5])
                        if error_count > 5:
                            error_sample += f"\n- ... and {error_count - 5} more errors"

                        export_output = mo.md(
                            f"""
                            ### ❌ Export Failed

                            **Critical Error**: Missing required dependency (pandas).

                            The `cognite-sdk` requires pandas for `.to_pandas()` functionality.
                            This should be fixed by reinstalling with `cognite-sdk[pandas]`.

                            **Errors ({error_count}):**
                            {error_sample}

                            | Metric | Value |
                            |--------|-------|
                            | Rows Written | {total_rows:,} |
                            | Transformations | {len(transformations)}{f" (limited from {total_available})" if trafo_limit > 0 else ""} |
                            | Errors | {error_count} |
                            | Processing Time | {elapsed_str} |
                            """
                        )
                    else:
                        export_result = {
                            "status": "partial",
                            "rows": total_rows,
                            "file": str(output_file),
                            "transformations": len(transformations),
                            "transformations_available": total_available,
                            "elapsed_seconds": elapsed_time,
                            "errors": export_errors,
                        }
                        error_sample = "\n".join(f"- {e}" for e in export_errors[:5])
                        if error_count > 5:
                            error_sample += f"\n- ... and {error_count - 5} more errors"

                        export_output = mo.md(
                            f"""
                            ### ⚠️ Export Completed with Errors

                            Some transformations failed to process.

                            **Errors ({error_count}):**
                            {error_sample}

                            | Metric | Value |
                            |--------|-------|
                            | Total Rows | {total_rows:,} |
                            | Transformations | {len(transformations)}{f" (limited from {total_available})" if trafo_limit > 0 else ""} |
                            | Errors | {error_count} |
                            | Processing Time | {elapsed_str} |
                            | Output File | `{output_file}` |
                            """
                        )
                else:
                    # Check if any rows were actually exported
                    if total_rows == 0:
                        export_result = {
                            "status": "empty",
                            "rows": 0,
                            "file": None,
                            "transformations": len(transformations),
                            "transformations_available": total_available,
                            "elapsed_seconds": elapsed_time,
                            "errors": [],
                        }

                        export_output = mo.md(
                            f"""
                            ### ⚠️ Export Completed - No Jobs Found

                            The export ran successfully but no job data was found.
                            No output file was created.

                            | Metric | Value |
                            |--------|-------|
                            | Transformations Checked | {len(transformations)}{f" (limited from {total_available})" if trafo_limit > 0 else ""} |
                            | Jobs Found | 0 |
                            | Processing Time | {elapsed_str} |

                            **Possible reasons:**
                            - Transformations have no recent job history
                            - Jobs per transformation limit (`{jobs_per_trafo_slider.value}`) returned no results
                            """
                        )
                    else:
                        export_result = {
                            "status": "success",
                            "rows": total_rows,
                            "file": str(output_file),
                            "transformations": len(transformations),
                            "transformations_available": total_available,
                            "elapsed_seconds": elapsed_time,
                            "errors": [],
                        }

                        export_output = mo.md(
                            f"""
                            ### ✅ Export Complete

                            | Metric | Value |
                            |--------|-------|
                            | Total Rows | {total_rows:,} |
                            | Transformations | {len(transformations)}{f" (limited from {total_available})" if trafo_limit > 0 else ""} |
                            | Processing Time | {elapsed_str} |
                            | Output File | `{output_file}` |
                            """
                        )

        except ImportError as e:
            export_output = mo.md(
                f"""
                ### ❌ Import Error

                Missing required package: `{str(e)}`

                Make sure `cognite-sdk[pandas]` is installed.
                """
            )
        except Exception as e:
            export_output = mo.md(f"❌ Export failed: {type(e).__name__}: {str(e)}")
    elif export_button.value:
        export_output = mo.callout(
            mo.md("⚠️ **Project Required**\n\nSelect a CDF project in Chapter 1 first."),
            kind="warn",
        )

    # Append to output stream (after progress bar)
    mo.output.append(export_output)

    # Return exported file path for use in Chapter 3 file selector
    exported_file_path = export_result.get("file") if export_result.get("status") == "success" else None
    return (exported_file_path,)


@app.cell(column=1, hide_code=True)
def chapter3_loading_header():
    # mo is available globally from app.setup
    mo.md("""
    ## Chapter 3: Load Data

    Load exported TSJM data from JSONL files for analysis.
    """)
    return


@app.cell(hide_code=True)
def create_file_selector(exported_file_path, output_folder_value):
    # Path and mo are available globally from app.setup
    # File selection - use exported file path if available, otherwise use default
    # Note: Path works on both Linux and Windows
    if exported_file_path:
        _default_path = exported_file_path
    else:
        _default_folder = output_folder_value or "."
        _default_path = str(Path(_default_folder) / "260130-cdfproject-tsjm-export.jsonl")

    jsonl_file_path = mo.ui.text(
        value=_default_path,
        label="JSONL File Path",
        full_width=True,
    )

    load_data_btn = mo.ui.run_button(label="Load Data")

    mo.vstack(
        [
            mo.md("### Select Data File"),
            jsonl_file_path,
            load_data_btn,
        ]
    )
    return jsonl_file_path, load_data_btn


@app.cell(hide_code=True)
def load_jsonl_data(jsonl_file_path, load_data_btn):
    # Path, mo, and pl are available globally from app.setup
    # Load data when button is clicked
    jobs_df = None  # Will be pl.DataFrame or None
    load_status = ""

    # Support ~ expansion and resolve to absolute path
    _file_path = Path(jsonl_file_path.value).expanduser().resolve()

    if load_data_btn.value and _file_path.exists():
        try:
            # Load JSONL with Polars (timestamps are stored as ms integers)
            # Schema overrides:
            # - Columns with nulls in early rows: prevent Null type inference
            # - ID columns: store as strings to avoid thousand separators in display
            _schema_overrides = {
                # Nullable columns
                "tsj_error": pl.Utf8,  # Can be null or error string
                "tsjm_last_counts": pl.Utf8,  # JSON string, can be "{}" or actual data
                "tsj_last_seen_time": pl.Int64,  # Can be null
                # ID columns - store as strings to avoid formatting (91977912 not 91,977,912)
                "ts_id": pl.Utf8,
                "tsj_job_id": pl.Utf8,
                "ts_no": pl.Utf8,
                "tsj_no": pl.Utf8,
            }
            jobs_df = pl.read_ndjson(_file_path, schema_overrides=_schema_overrides)

            # Convert millisecond timestamps to Datetime
            # tsj_created_time is required (no nulls), others can be null
            jobs_df = jobs_df.with_columns(
                [
                    pl.from_epoch(pl.col("tsj_created_time"), time_unit="ms").alias("tsj_created_time"),
                    pl.when(pl.col("tsj_started_time").is_not_null())
                    .then(pl.from_epoch(pl.col("tsj_started_time"), time_unit="ms"))
                    .otherwise(None)
                    .alias("tsj_started_time"),
                    pl.when(pl.col("tsj_finished_time").is_not_null())
                    .then(pl.from_epoch(pl.col("tsj_finished_time"), time_unit="ms"))
                    .otherwise(None)
                    .alias("tsj_finished_time"),
                    pl.when(pl.col("tsj_last_seen_time").is_not_null())
                    .then(pl.from_epoch(pl.col("tsj_last_seen_time"), time_unit="ms"))
                    .otherwise(None)
                    .alias("tsj_last_seen_time"),
                ]
            )

            # Filter out running jobs for concurrency analysis
            jobs_df = jobs_df.filter(pl.col("tsj_status") != "Running")

            load_status = f"✅ Loaded {len(jobs_df):,} rows from `{_file_path.name}`"
        except Exception as e:
            load_status = f"❌ Error loading file: {type(e).__name__}: {str(e)}"
    elif load_data_btn.value:
        load_status = f"⚠️ File not found: `{_file_path}`"
    else:
        load_status = "Click 'Load Data' to load the JSONL file."

    mo.md(load_status)
    return (jobs_df,)


@app.cell(hide_code=True)
def show_data_overview(jobs_df):
    # mo and pl are available globally from app.setup
    # Display data overview
    if jobs_df is not None and len(jobs_df) > 0:
        _stats = {
            "Total Jobs": f"{len(jobs_df):,}",
            "Transformations": f"{jobs_df['ts_id'].n_unique():,}",
            "Projects": ", ".join(jobs_df["project"].unique().to_list()),
            "Date Range": f"{jobs_df['tsj_started_time'].min()} to {jobs_df['tsj_finished_time'].max()}",
            "Status Distribution": str(jobs_df.group_by("tsj_status").len().to_dict()),
        }

        _output = mo.vstack(
            [
                mo.md("### Data Overview"),
                mo.ui.table(
                    pl.DataFrame({"Metric": list(_stats.keys()), "Value": list(_stats.values())}),
                    selection=None,
                ),
                mo.md("### Sample Data (peak view)"),
                mo.ui.table(jobs_df.head(10), selection=None),
                mo.callout(
                    mo.md(
                        "✅ **Data loaded successfully!** Scroll down to **Chapter 4: Concurrency Dashboard** to explore job concurrency over time."
                    ),
                    kind="success",
                ),
            ]
        )
    else:
        _output = mo.callout(
            mo.md("👆 **Load Data**\n\nEnter a file path above and click **Load Data** to begin analysis."),
            kind="info",
        )

    _output
    return


@app.cell(hide_code=True)
def chapter4_concurrency_header():
    # mo is available globally from app.setup
    mo.md("""
    ## Chapter 4: Concurrency Dashboard

    Visualize concurrent transformation jobs over time.
    """)
    return


@app.cell(hide_code=True)
def create_concurrency_date_picker(create_date_range_ui, jobs_df):
    # Date range selection based on loaded data
    if jobs_df is not None and len(jobs_df) > 0:
        _min_dt = jobs_df["tsj_started_time"].min()
        _max_dt = jobs_df["tsj_finished_time"].max()

        if _min_dt is not None and _max_dt is not None:
            _min_date = _min_dt.date()
            _max_date = _max_dt.date()
        else:
            _min_date = datetime.date(2025, 1, 1)
            _max_date = datetime.date(2025, 12, 31)
    else:
        _min_date = datetime.date(2025, 1, 1)
        _max_date = datetime.date(2025, 12, 31)

    _data_days = (_max_date - _min_date).days + 1
    _data_info = f"**Data available:** {_min_date} to {_max_date} ({_data_days} days)"

    concurrency_date_form, _output = create_date_range_ui(
        mo, _min_date, _max_date, title="Date Range for Concurrency", data_info=_data_info
    )
    _output
    return (concurrency_date_form,)


@app.cell(hide_code=True)
def extract_concurrency_date_range(
    calculate_date_range_from_form,
    concurrency_date_form,
    jobs_df,
):
    # Calculate effective date range based on form submission
    if jobs_df is not None and len(jobs_df) > 0:
        _min_dt = jobs_df["tsj_started_time"].min()
        _max_dt = jobs_df["tsj_finished_time"].max()
        _min_date = _min_dt.date() if _min_dt else datetime.date(2025, 1, 1)
        _max_date = _max_dt.date() if _max_dt else datetime.date(2025, 12, 31)
    else:
        _min_date = datetime.date(2025, 1, 1)
        _max_date = datetime.date(2025, 12, 31)

    selected_start, selected_end = calculate_date_range_from_form(concurrency_date_form.value, _min_date, _max_date)

    # No output - this cell just extracts the selected range
    return selected_end, selected_start


@app.cell(hide_code=True)
def calculate_concurrency_events(jobs_df):
    # pl is available globally from app.setup
    def calculate_events_df(df):
        """
        Calculate event points and concurrency from job data.

        Args:
            df: DataFrame with tsj_started_time, tsj_finished_time, tsj_job_id columns

        Returns:
            DataFrame with time, job_id, change, and concurrency columns
        """
        if df is None or len(df) == 0:
            return pl.DataFrame(
                schema={
                    "time": pl.Datetime("ms"),
                    "job_id": pl.Int64,
                    "change": pl.Int64,
                    "concurrency": pl.Int64,
                }
            )

        # Create start events (+1)
        start_events = df.select(
            [
                pl.col("tsj_started_time").alias("time"),
                pl.col("tsj_job_id").alias("job_id"),
                pl.lit(1).alias("change"),
            ]
        )

        # Create end events (-1)
        end_events = df.select(
            [
                pl.col("tsj_finished_time").alias("time"),
                pl.col("tsj_job_id").alias("job_id"),
                pl.lit(-1).alias("change"),
            ]
        )

        # Combine and sort
        events = pl.concat([start_events, end_events])
        events = events.filter(pl.col("time").is_not_null())
        events = events.sort(["time", "change"], descending=[False, True])

        # Calculate cumulative concurrency
        events = events.with_columns(pl.col("change").cum_sum().alias("concurrency"))

        return events


    # Calculate events if data is loaded
    events_df = None  # Will be pl.DataFrame or None
    if jobs_df is not None and len(jobs_df) > 0:
        events_df = calculate_events_df(jobs_df)
    return (events_df,)


@app.cell(hide_code=True)
def create_peak_concurrency_table(events_df):
    # mo and pl are available globally from app.setup
    # Calculate peak concurrency for ALL days (sorted by date)
    # Then show top 5 by peak concurrency in the table
    all_daily_peaks = None
    top_peaks_table = None

    if events_df is not None and len(events_df) > 0:
        # Full dataset: all days sorted by peak concurrency (highest first)
        all_daily_peaks = (
            events_df.with_columns(pl.col("time").dt.date().alias("date"))
            .group_by("date")
            .agg(pl.col("concurrency").max().alias("peak_concurrency"))
            .sort("peak_concurrency", descending=True)  # Top peaks first
        )

        # Table with ALL days - user can page/sort to find any day
        top_peaks_table = mo.ui.table(
            all_daily_peaks,
            selection="single",
            label="Click a row to jump to that day",
            page_size=10,  # Show 10 per page, top peaks first
        )

        _output = mo.vstack(
            [
                mo.md(f"### Peak Concurrency by Day ({len(all_daily_peaks)} days)"),
                top_peaks_table,
                mo.md("*Click a row to zoom to that day. Use table controls to sort/page.*"),
            ]
        )
    else:
        _output = mo.callout(
            mo.md("⏳ **Waiting for Data**\n\nLoad data in Chapter 3 to see peak concurrency days."),
            kind="info",
        )

    _output
    return (top_peaks_table,)


@app.cell(hide_code=True)
def create_concurrency_chart(
    events_df,
    selected_end,
    selected_start,
    top_peaks_table,
):
    # alt, datetime, mo, and pl are available globally from app.setup
    # Create concurrency chart with Ctrl+Shift+scroll zoom
    concurrency_chart = None

    # Check if a peak day is selected - this overrides date picker range
    _peak_selected = None
    if top_peaks_table is not None and top_peaks_table.value is not None:
        _selected_rows = top_peaks_table.value
        if hasattr(_selected_rows, "to_dicts"):
            _selected_rows = _selected_rows.to_dicts()
        elif hasattr(_selected_rows, "to_dict"):
            _selected_rows = _selected_rows.to_dict("records")
        if _selected_rows and len(_selected_rows) > 0:
            _peak_selected = _selected_rows[0].get("date")

    if events_df is not None and len(events_df) > 0:
        # Determine date range: peak selection overrides date picker
        if _peak_selected:
            # Show just the selected peak day
            _view_start = _peak_selected
            _view_end = _peak_selected
            _date_range_str = f"📍 {_peak_selected} (peak day)"
            _days = 1
        elif selected_start and selected_end:
            _view_start = selected_start
            _view_end = selected_end
            _days = (selected_end - selected_start).days + 1
            _date_range_str = f"{selected_start} → {selected_end}"
        else:
            _view_start = None
            _view_end = None
            _date_range_str = "All data"
            _days = 0

        # Filter by view range
        if _view_start and _view_end:
            _start_dt = datetime.datetime.combine(_view_start, datetime.time.min)
            _end_dt = datetime.datetime.combine(_view_end, datetime.time.max)
            filtered_events = events_df.filter((pl.col("time") >= _start_dt) & (pl.col("time") <= _end_dt))
        else:
            filtered_events = events_df

        if len(filtered_events) > 0:
            # Prepare data for Altair
            # Convert to pandas to avoid OutOfBoundsError when selection indices change
            # Cast datetime to microseconds for compatibility with marimo's altair_chart filtering
            chart_data = (
                filtered_events.group_by("time")
                .agg(pl.col("concurrency").last())
                .sort("time")
                .with_columns(pl.col("time").cast(pl.Datetime("us")).alias("time"))
                .to_pandas()  # Convert to pandas for stable indexing with mo.ui.altair_chart
            )

            # Single interval selection - stable with marimo
            # Range select works reliably, click doesn't work well with mo.ui.altair_chart
            brush = alt.selection_interval(encodings=["x"], name="brush", empty=False)
            # Zoom: Ctrl+Shift+scroll to zoom x-axis only
            zoom = alt.selection_interval(
                bind="scales",
                encodings=["x"],
                zoom="wheel![event.ctrlKey && event.shiftKey]",
                translate=False,
            )

            # Build chart with date range in title
            _chart_title = f"Concurrency: {_date_range_str} ({_days} days)"

            base = alt.Chart(chart_data).encode(
                x=alt.X("time:T", title="Time", axis=alt.Axis(format="%Y-%m-%d %H:%M")),
                y=alt.Y("concurrency:Q", title="Concurrent Jobs"),
                tooltip=[
                    alt.Tooltip("time:T", title="Time", format="%Y-%m-%d %H:%M:%S"),
                    alt.Tooltip("concurrency:Q", title="Concurrent Jobs"),
                ],
            )

            line = base.mark_line(
                interpolate="step-after",
                color="#1f77b4",
            )

            points = (
                base.mark_point(
                    filled=True,
                    size=5,
                )
                .encode(
                    opacity=alt.condition(brush, alt.value(1), alt.value(0.4)),
                    color=alt.condition(brush, alt.value("red"), alt.value("#1f77b4")),
                    size=alt.condition(brush, alt.value(30), alt.value(5)),
                )
                .add_params(brush)
                .add_params(zoom)
            )

            concurrency_chart = (line + points).properties(
                width="container",
                height=400,
                title=_chart_title,
            )

    if concurrency_chart:
        chart_element = mo.ui.altair_chart(concurrency_chart)
        _output = mo.vstack(
            [
                mo.md(f"### Concurrency Chart: {_date_range_str}"),
                chart_element,
                mo.md("*Drag=select range, Ctrl+Shift+scroll=zoom*"),
            ]
        )
    else:
        chart_element = None
        _output = mo.vstack(
            [
                mo.md("### Concurrency Chart"),
                mo.callout(
                    mo.md("⏳ **No Data to Display**\n\nLoad data in Chapter 3 and select a date range."),
                    kind="info",
                ),
            ]
        )

    _output
    return (chart_element,)


@app.cell(hide_code=True)
def debug_concurrency_selection_state(chart_element):
    # mo is available globally from app.setup
    """Debug cell to show current selection state from the concurrency chart.
    Only visible in edit mode, hidden in app/run mode.
    """

    # Only show debug info in edit mode
    _is_edit_mode = mo.app_meta().mode == "edit"

    if not _is_edit_mode:
        _output = None  # Hidden in app/run mode
    elif chart_element is None:
        _output = mo.md("⏳ Chart not loaded yet")
    else:
        _selection = chart_element.value

        # Analyze selection state
        _selection_info = []
        _selection_info.append("### 🔍 Chart Selection Debug")

        if _selection is None:
            _selection_info.append("**State:** `None` (no selection)")
            _selection_info.append("**Action:** Drag to select a range")
        elif hasattr(_selection, "__len__") and len(_selection) == 0:
            _selection_info.append("**State:** Empty (0 chart points)")
            _selection_info.append("**Action:** Drag to select a range")
        else:
            # Get selection details
            if hasattr(_selection, "to_dicts"):
                _rows = _selection.to_dicts()
            elif hasattr(_selection, "to_dict"):
                _rows = _selection.to_dict("records")
            elif isinstance(_selection, list):
                _rows = _selection
            elif isinstance(_selection, dict):
                _rows = [_selection]
            else:
                _rows = []

            _selection_info.append(f"**Mode:** Range ({len(_rows)} chart event points)")

            if _rows:
                # Show time range used for filtering
                _times = [r.get("time") for r in _rows if r.get("time")]
                if _times:
                    _min_time = min(_times)
                    _max_time = max(_times)
                    _selection_info.append(f"**Filter:** Jobs overlapping `{_min_time}` → `{_max_time}`")
                    _selection_info.append(f"  - `started <= {_max_time} AND finished >= {_min_time}`")

                # Show concurrency values if available
                _concurrencies = [r.get("concurrency") for r in _rows if r.get("concurrency") is not None]
                if _concurrencies:
                    _selection_info.append(f"**Concurrency range:** {min(_concurrencies)} → {max(_concurrencies)}")

        _output = mo.callout(
            mo.md("\n\n".join(_selection_info)),
            kind="info",
        )

    _output
    return


@app.cell(hide_code=True)
def show_active_jobs_details(chart_element, jobs_df):
    # datetime, mo, and pl are available globally from app.setup
    # Show active transformations and jobs when chart point is selected
    _default_msg = mo.md(
        "🖱️ **Click** on a point or **drag** to select a time range to see active transformations and jobs."
    )

    if chart_element is None:
        _output = mo.callout(
            mo.md("⏳ **Waiting for Chart**\n\nSelect a date range above to load the concurrency chart."),
            kind="info",
        )
    elif jobs_df is None:
        _output = mo.callout(
            mo.md("⏳ **Waiting for Data**\n\nLoad data in Chapter 3 first."),
            kind="info",
        )
    else:
        _concurrency_selection = chart_element.value

        # Check if we have a selection
        if _concurrency_selection is None or (
            hasattr(_concurrency_selection, "__len__") and len(_concurrency_selection) == 0
        ):
            _output = _default_msg
        else:
            # chart_element.value returns a DataFrame or list of selected points
            # Get the time from the first selected point
            if hasattr(_concurrency_selection, "to_dicts"):
                # It's a DataFrame-like object (polars/pandas)
                _selected_rows = _concurrency_selection.to_dicts()
            elif hasattr(_concurrency_selection, "to_dict"):
                # pandas DataFrame
                _selected_rows = _concurrency_selection.to_dict("records")
            elif isinstance(_concurrency_selection, list):
                _selected_rows = _concurrency_selection
            elif isinstance(_concurrency_selection, dict):
                # Single dict selection
                _selected_rows = [_concurrency_selection]
            else:
                _selected_rows = []

            if not _selected_rows:
                _output = mo.vstack(
                    [
                        _default_msg,
                        mo.callout(
                            mo.md(f"⚠️ Could not parse selection. Raw: `{repr(_concurrency_selection)[:200]}`"), kind="warn"
                        ),
                    ]
                )
            else:
                # Get time from first and last selected points
                _first_time = _selected_rows[0].get("time")
                _last_time = _selected_rows[-1].get("time") if len(_selected_rows) > 1 else _first_time

                if not _first_time:
                    _output = mo.vstack(
                        [
                            _default_msg,
                            mo.callout(
                                mo.md(f"⚠️ No 'time' field in selection. Keys: `{list(_selected_rows[0].keys())}`"),
                                kind="warn",
                            ),
                        ]
                    )
                else:
                    # Convert to datetime - handle various formats
                    if isinstance(_first_time, str):
                        _first_dt = datetime.datetime.fromisoformat(_first_time.replace("Z", "+00:00"))
                        _last_dt = (
                            datetime.datetime.fromisoformat(_last_time.replace("Z", "+00:00")) if _last_time else _first_dt
                        )
                    else:
                        _first_dt = _first_time
                        _last_dt = _last_time if _last_time else _first_dt

                    # For single click, show jobs at that moment
                    # For range selection, show jobs that overlap with the range
                    if len(_selected_rows) == 1:
                        # Single point clicked
                        _active_jobs = jobs_df.filter(
                            (pl.col("tsj_started_time") <= _first_dt) & (pl.col("tsj_finished_time") > _first_dt)
                        )
                        _time_desc = f"at **{_first_dt}**"
                    else:
                        # Range selected - show jobs active during any part of the range
                        _active_jobs = jobs_df.filter(
                            (pl.col("tsj_started_time") <= _last_dt) & (pl.col("tsj_finished_time") >= _first_dt)
                        )
                        _time_desc = f"between **{_first_dt}** and **{_last_dt}**"

                    # Select columns for job details
                    _active_jobs_detail = _active_jobs.select(
                        [
                            "project",
                            "ts_external_id",
                            "ts_id",
                            "tsj_job_id",
                            "tsj_status",
                            "tsj_started_time",
                            "tsj_finished_time",
                        ]
                    )

                    # === TAB 1: Transformations Summary ===
                    # Group by transformation and compute stats
                    _trafo_stats = (
                        _active_jobs.group_by("ts_external_id")
                        .agg(
                            [
                                pl.len().alias("job_count"),
                                pl.col("tsj_started_time").min().alias("first_start"),
                                pl.col("tsj_started_time").max().alias("last_start"),
                                # For completed jobs, compute runtime
                                pl.when(pl.col("tsj_status") == "Completed")
                                .then((pl.col("tsj_finished_time") - pl.col("tsj_started_time")).dt.total_seconds())
                                .otherwise(None)
                                .mean()
                                .alias("avg_runtime_sec"),
                                # Count of completed jobs
                                (pl.col("tsj_status") == "Completed").sum().alias("completed_count"),
                                (pl.col("tsj_status") == "Failed").sum().alias("failed_count"),
                            ]
                        )
                        .sort("job_count", descending=True)
                    )

                    # Compute cadence (average time between job starts)
                    def _compute_cadence_sec(row):
                        """Return cadence in seconds (numeric for sorting)."""
                        _job_count = row["job_count"]
                        if _job_count <= 1:
                            return None  # Single run - no cadence
                        _first = row["first_start"]
                        _last = row["last_start"]
                        if _first is None or _last is None or _first == _last:
                            return None
                        # Total duration / (job_count - 1) = avg interval
                        _duration_sec = (_last - _first).total_seconds()
                        return _duration_sec / (_job_count - 1)

                    def _format_duration(sec):
                        """Format seconds as human-readable duration."""
                        if sec is None:
                            return "N/A"
                        if sec < 60:
                            return f"~{int(sec)}s"
                        elif sec < 3600:
                            return f"~{int(sec / 60)}min"
                        elif sec < 86400:
                            return f"~{sec / 3600:.1f}h"
                        else:
                            return f"~{sec / 86400:.1f}d"

                    # Build display dataframe with both numeric (sortable) and formatted columns
                    _trafo_display = []
                    for _row in _trafo_stats.to_dicts():
                        _cadence_sec = _compute_cadence_sec(_row)
                        _runtime_sec = _row["avg_runtime_sec"]
                        _trafo_display.append(
                            {
                                "Transformation": _row["ts_external_id"],
                                "Jobs": _row["job_count"],
                                "✓ Completed": _row["completed_count"],
                                "✗ Failed": _row["failed_count"],
                                "Cadence": _format_duration(_cadence_sec),
                                "⇅ cadence_sec": int(_cadence_sec) if _cadence_sec is not None else None,
                                "Avg Runtime": _format_duration(_runtime_sec) if _runtime_sec else "N/A",
                                "⇅ runtime_sec": int(_runtime_sec) if _runtime_sec is not None else None,
                            }
                        )
                    _trafo_display_df = pl.DataFrame(_trafo_display)

                    # Tab 1 content
                    _tab1_content = mo.vstack(
                        [
                            mo.md(f"**{len(_trafo_display_df)}** transformations were active {_time_desc}"),
                            mo.ui.table(_trafo_display_df, selection=None, page_size=10)
                            if len(_trafo_display_df) > 0
                            else mo.md("No transformations found."),
                        ]
                    )

                    # === TAB 2: Job Details ===
                    _tab2_content = mo.vstack(
                        [
                            mo.md(
                                f"**{len(_active_jobs_detail)}** jobs were running during this time. ({len(_selected_rows)} chart points selected)"
                            ),
                            mo.ui.table(_active_jobs_detail, selection=None, page_size=10)
                            if len(_active_jobs_detail) > 0
                            else mo.md("No active jobs found."),
                        ]
                    )

                    # === Build Tabs ===
                    _tabs = mo.ui.tabs(
                        {
                            f"📊 Transformations ({len(_trafo_display_df)})": _tab1_content,
                            f"📋 Jobs ({len(_active_jobs_detail)})": _tab2_content,
                        }
                    )

                    _output = mo.vstack(
                        [
                            mo.md(f"### Active Transformations and Jobs {_time_desc}"),
                            _tabs,
                        ]
                    )

    _output
    return


@app.cell(hide_code=True)
def chapter5_metrics_header():
    # mo is available globally from app.setup
    mo.md("""
    ## Chapter 5: Metrics Dashboard

    Select and visualize transformation metrics over time.
    Drill down into individual transformations and jobs.
    """)
    return


@app.cell(hide_code=True)
def extract_available_metrics(jobs_df):
    # json, mo, and pl are available globally from app.setup
    # Unpack metrics and aggregate
    available_metrics: list[str] = []
    metrics_sample_day = None


    def extract_metric(json_str: str | None, metric_name: str) -> int | None:
        """Extract a specific metric from JSON string."""
        if not json_str or json_str == "{}":
            return None
        try:
            data = json.loads(json_str)
            return data.get(metric_name)
        except (json.JSONDecodeError, TypeError):
            return None


    if jobs_df is not None and len(jobs_df) > 0:
        # Find the 2nd most recent day (latest might be incomplete)
        # Group by day and find unique days
        days_df = (
            jobs_df.filter(pl.col("tsjm_last_counts").is_not_null() & (pl.col("tsjm_last_counts") != "{}"))
            .with_columns(pl.col("tsj_created_time").dt.date().alias("day"))
            .group_by("day")
            .agg(pl.len().alias("count"))
            .sort("day", descending=True)
        )

        if len(days_df) >= 2:
            # Use 2nd most recent day
            metrics_sample_day = days_df["day"][1]
        elif len(days_df) == 1:
            # Only one day available, use it
            metrics_sample_day = days_df["day"][0]

        if metrics_sample_day is not None:
            # Get all rows from the sample day
            sample_day_data = jobs_df.filter(
                (pl.col("tsj_created_time").dt.date() == metrics_sample_day)
                & pl.col("tsjm_last_counts").is_not_null()
                & (pl.col("tsjm_last_counts") != "{}")
            )

            # Collect ALL unique metrics from this day
            all_metrics_set: set[str] = set()
            _parse_errors: list[dict] = []  # Track problematic rows
            for idx, row in enumerate(sample_day_data.iter_rows(named=True)):
                json_str = row.get("tsjm_last_counts")
                if json_str:
                    try:
                        data = json.loads(json_str)
                        all_metrics_set.update(data.keys())
                    except (json.JSONDecodeError, TypeError) as e:
                        _parse_errors.append(
                            {
                                "ts_external_id": row.get("ts_external_id", "unknown"),
                                "tsj_job_id": row.get("tsj_job_id", "unknown"),
                                "error": str(e),
                                "json_preview": json_str[:100] + "..." if len(json_str) > 100 else json_str,
                            }
                        )

            # Sort metrics alphabetically (case-insensitive) for consistent display
            available_metrics = sorted(all_metrics_set, key=str.lower)

            _output_parts = [
                mo.md("### Available Metrics"),
                mo.md(
                    f"Found **{len(available_metrics)}** unique metrics from **{metrics_sample_day}** ({len(sample_day_data):,} jobs):"
                ),
                mo.md(", ".join(f"`{m}`" for m in available_metrics[:30]) + ("..." if len(available_metrics) > 30 else "")),
            ]

            # Show parse errors if any
            if _parse_errors:
                import pandas as _pd

                _error_df = _pd.DataFrame(_parse_errors[:10])  # Show first 10
                _output_parts.append(
                    mo.callout(
                        mo.vstack(
                            [
                                mo.md(
                                    f"⚠️ **{len(_parse_errors)} rows had invalid JSON** (showing first {min(10, len(_parse_errors))}):"
                                ),
                                mo.ui.table(_error_df, selection=None),
                            ]
                        ),
                        kind="warn",
                    )
                )

            _output = mo.vstack(_output_parts)
        else:
            _output = mo.callout(
                mo.md("⚠️ **No Metrics Found**\n\nThe loaded data doesn't contain any metrics fields."),
                kind="warn",
            )
    else:
        _output = mo.callout(
            mo.md("⏳ **Waiting for Data**\n\nLoad data in Chapter 3 to see available metrics."),
            kind="info",
        )

    _output
    return available_metrics, extract_metric


@app.cell(hide_code=True)
def create_metric_selector(available_metrics: list[str]):
    # mo is available globally from app.setup
    # Multi-metric selector for comparison
    if available_metrics:
        # Suggest some common metrics as defaults
        _default_metrics = [m for m in available_metrics if "upserted" in m.lower()][:3]
        if not _default_metrics:
            _default_metrics = available_metrics[:2]

        metric_selector = mo.ui.multiselect(
            options=available_metrics,
            value=_default_metrics,
            label="Select Metrics to Compare",
            max_selections=10,  # Limit to avoid chart clutter
        )
        _output = mo.vstack(
            [
                mo.md("### Select Metrics"),
                mo.md(f"Choose up to 10 metrics from {len(available_metrics)} available (type to filter):"),
                metric_selector,
            ]
        )
    else:
        metric_selector = None
        _output = mo.callout(
            mo.md("⏳ **No Metrics Available**\n\nLoad data with metrics to enable selection."),
            kind="info",
        )

    _output
    return (metric_selector,)


@app.cell(hide_code=True)
def display_selected_metrics(metric_selector):
    # mo is available globally from app.setup
    # Display selected metrics as a numbered list (alphabetically sorted)
    if metric_selector is not None and metric_selector.value:
        _sorted_metrics = sorted(metric_selector.value, key=str.lower)
        _metrics_list = "\n".join(f"{i + 1}. `{m}`" for i, m in enumerate(_sorted_metrics))
        _output = mo.md(f"**Selected ({len(_sorted_metrics)}):**\n\n{_metrics_list}")
    else:
        _output = mo.callout(
            mo.md("👆 **Select Metrics**\n\nChoose metrics from the dropdown above to compare."),
            kind="info",
        )
    _output
    return


@app.cell(hide_code=True)
def create_metrics_date_form(create_date_range_ui, jobs_df):
    # datetime, mo, and pl are available globally from app.setup
    # Get data range from jobs_df
    if jobs_df is not None and len(jobs_df) > 0:
        _data_min = jobs_df.select("tsj_finished_time").min().item().date()
        _data_max = jobs_df.select("tsj_finished_time").max().item().date()
        _data_days = (_data_max - _data_min).days + 1

        # Also check jobs with actual metrics (non-empty tsjm_last_counts)
        _jobs_with_metrics = jobs_df.filter(pl.col("tsjm_last_counts").is_not_null() & (pl.col("tsjm_last_counts") != "{}"))
        if len(_jobs_with_metrics) > 0:
            _metrics_min = _jobs_with_metrics.select("tsj_finished_time").min().item().date()
            _metrics_max = _jobs_with_metrics.select("tsj_finished_time").max().item().date()
            _metrics_days = (_metrics_max - _metrics_min).days + 1
            _data_range_info = (
                f"**Jobs data:** {_data_min} to {_data_max} ({_data_days} days) | "
                f"**With metrics:** {_metrics_min} to {_metrics_max} ({_metrics_days} days)"
            )
        else:
            _data_range_info = f"**Jobs data:** {_data_min} to {_data_max} ({_data_days} days) | **No metrics found**"
    else:
        _data_min = datetime.date.today() - datetime.timedelta(days=30)
        _data_max = datetime.date.today()
        _data_range_info = "No data loaded"

    metrics_date_form, _output = create_date_range_ui(
        mo, _data_min, _data_max, title="Date Range for Metrics", data_info=_data_range_info
    )
    _output
    return (metrics_date_form,)


@app.cell(hide_code=True)
def calculate_metrics_date_range(
    calculate_date_range_from_form,
    jobs_df,
    metrics_date_form,
):
    # Calculate effective date range based on form submission
    # Get data boundaries
    if jobs_df is not None and len(jobs_df) > 0:
        _data_min = jobs_df.select("tsj_finished_time").min().item().date()
        _data_max = jobs_df.select("tsj_finished_time").max().item().date()
    else:
        _data_min = datetime.date.today() - datetime.timedelta(days=365)
        _data_max = datetime.date.today()

    metrics_date_start, metrics_date_end = calculate_date_range_from_form(metrics_date_form.value, _data_min, _data_max)
    return metrics_date_end, metrics_date_start


@app.cell(hide_code=True)
def create_daily_aggregation_chart(
    extract_metric,
    jobs_df,
    metric_selector,
    metrics_date_end,
    metrics_date_start,
):
    # alt, mo, and pl are available globally from app.setup
    # Aggregate selected metrics by day (supports multiple metrics for comparison)
    daily_metric_df = None  # Will be pl.DataFrame or None
    metric_chart_element = None
    metric_values_df = None  # Keep for later filtering
    selected_metrics: list[str] = []

    if jobs_df is not None and metric_selector is not None and metric_selector.value:
        selected_metrics = list(metric_selector.value)  # List of selected metrics

        if selected_metrics:
            # Extract all selected metric values
            _metric_columns = []
            for _metric in selected_metrics:
                _metric_columns.append(
                    pl.col("tsjm_last_counts")
                    .map_elements(
                        lambda x, m=_metric: extract_metric(x, m),
                        return_dtype=pl.Int64,
                    )
                    .alias(_metric)
                )

            # Add date column and filter by selected date range
            _df_with_metrics = jobs_df.with_columns(_metric_columns + [pl.col("tsj_finished_time").dt.date().alias("date")])

            # Apply date range filter (convert dates to ensure proper comparison)
            if metrics_date_start and metrics_date_end:
                # Convert Python date to Polars literal for reliable comparison
                _start_lit = pl.lit(metrics_date_start).cast(pl.Date)
                _end_lit = pl.lit(metrics_date_end).cast(pl.Date)
                metric_values_df = _df_with_metrics.filter((pl.col("date") >= _start_lit) & (pl.col("date") <= _end_lit))
            else:
                metric_values_df = _df_with_metrics

            # Aggregate each metric by day
            _agg_exprs = []
            for _metric in selected_metrics:
                _agg_exprs.append(pl.col(_metric).sum().alias(_metric))

            daily_metric_df = metric_values_df.group_by("date").agg(_agg_exprs).sort("date")

            if len(daily_metric_df) > 0:
                # Melt to long format for multi-line chart
                # From: date | metric1 | metric2 | metric3
                # To:   date | metric_name | value
                _chart_data = daily_metric_df.unpivot(
                    index="date",
                    on=selected_metrics,
                    variable_name="metric",
                    value_name="total",
                ).to_pandas()

                # Create brush selection for drag-select on x-axis
                # empty=False: when nothing selected, all lines show normal style
                # NOTE: Click selection doesn't work well with mo.ui.altair_chart, so we only use brush
                # NOTE: Zoom (bind="scales") causes offset bugs with brush, so we don't use it
                _metric_brush = alt.selection_interval(encodings=["x"], name="brush", empty=False)

                # Tableau10 color scheme for consistent colors across charts
                _tableau10 = [
                    "#4c78a8",
                    "#f58518",
                    "#54a24b",
                    "#e45756",
                    "#72b7b2",
                    "#eeca3b",
                    "#b279a2",
                    "#ff9da6",
                    "#9d755d",
                    "#bab0ac",
                ]
                _agg_color_scale = alt.Scale(domain=selected_metrics, range=_tableau10[: len(selected_metrics)])

                # Multi-line chart with explicit color scale for different metrics
                # Set explicit x-axis domain to show full selected date range (even if data has gaps)
                _x_domain = (
                    [str(metrics_date_start), str(metrics_date_end)] if metrics_date_start and metrics_date_end else None
                )

                # Brush selection for range select (captured by marimo)
                _metric_brush = alt.selection_interval(encodings=["x"], name="brush", empty=False)

                # Zoom: Ctrl+Shift+scroll to zoom x-axis only
                _zoom = alt.selection_interval(
                    bind="scales",
                    encodings=["x"],
                    zoom="wheel![event.ctrlKey && event.shiftKey]",
                    translate=False,
                )

                # Base line chart with brush selection for highlighting
                _line_chart = (
                    alt.Chart(_chart_data)
                    .mark_line(strokeWidth=2)
                    .encode(
                        x=alt.X("date:T", title="Date", scale=alt.Scale(domain=_x_domain) if _x_domain else alt.Undefined),
                        y=alt.Y("total:Q", title="Total Count"),
                        color=alt.Color(
                            "metric:N",
                            title="Metric",
                            scale=_agg_color_scale,
                            legend=alt.Legend(orient="bottom", columns=3),
                        ),
                        opacity=alt.condition(_metric_brush, alt.value(1), alt.value(0.5)),
                        strokeWidth=alt.condition(_metric_brush, alt.value(3), alt.value(1)),
                    )
                )

                # Points layer with tooltip (hover directly over point for tooltip)
                _points = (
                    alt.Chart(_chart_data)
                    .mark_point(filled=True, size=50)
                    .encode(
                        x=alt.X("date:T"),
                        y=alt.Y("total:Q"),
                        color=alt.Color("metric:N", scale=_agg_color_scale),
                        opacity=alt.condition(_metric_brush, alt.value(1), alt.value(0.7)),
                        size=alt.condition(_metric_brush, alt.value(100), alt.value(50)),
                        tooltip=[
                            alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
                            alt.Tooltip("metric:N", title="Metric"),
                            alt.Tooltip("total:Q", title="Total", format=",d"),
                        ],
                    )
                    .add_params(_metric_brush)
                    .add_params(_zoom)
                )

                _metric_chart = (_line_chart + _points).properties(
                    width="container",
                    height=400,
                    title=f"Daily Metrics ({len(selected_metrics)}) - drag to select range",
                )

                metric_chart_element = mo.ui.altair_chart(_metric_chart)

                # Show date range in header
                _date_range_str = (
                    f"{metrics_date_start} → {metrics_date_end}" if metrics_date_start and metrics_date_end else "All time"
                )
                # Calculate requested range span (not just days with data)
                _days_count = (
                    (metrics_date_end - metrics_date_start).days + 1
                    if metrics_date_start and metrics_date_end
                    else len(daily_metric_df)
                )

                _output = mo.vstack(
                    [
                        mo.md(f"### Daily Metrics Aggregation: {_date_range_str} ({_days_count} days)"),
                        mo.md(
                            f"Comparing **{len(selected_metrics)}** metrics: {', '.join(f'`{m}`' for m in selected_metrics[:5])}{'...' if len(selected_metrics) > 5 else ''}"
                        ),
                        metric_chart_element,
                        mo.md("*Drag=select range, Hover=tooltip, Ctrl+Shift+scroll=zoom*"),
                    ]
                )
            else:
                _output = mo.callout(
                    mo.md("⚠️ **No Data Found**\n\nNo data for selected metrics in the current date range."),
                    kind="warn",
                )
        else:
            _output = mo.callout(
                mo.md("👆 **Select Metrics**\n\nChoose at least one metric above to see the aggregation chart."),
                kind="info",
            )
    else:
        _output = mo.callout(
            mo.md("⏳ **Waiting for Metrics**\n\nLoad data and select metrics to see aggregation."),
            kind="info",
        )

    _output
    return (
        daily_metric_df,
        metric_chart_element,
        metric_values_df,
        selected_metrics,
    )


@app.cell(hide_code=True)
def show_transformation_details(
    metric_chart_element,
    metric_values_df,
    selected_metrics: list[str],
):
    # Show transformation details when chart selection is made
    _default_msg = mo.md("🖱️ **Drag** to select date range in chart above")

    # Initialize return values (set in conditional branches)
    trafo_details_table = None
    trafo_trend_data = None
    trafo_trend_date_range = None

    if metric_chart_element is None or metric_values_df is None or not selected_metrics:
        _output = mo.callout(
            mo.md("⏳ **Waiting for Chart**\n\nSelect metrics above and wait for the chart to load."),
            kind="info",
        )
    else:
        _selected = metric_chart_element.value

        if _selected is None or (hasattr(_selected, "__len__") and len(_selected) == 0):
            _output = _default_msg
        else:
            # Parse selection (prefix all locals with _ to avoid marimo conflicts)
            if hasattr(_selected, "to_dicts"):
                _selected_rows = _selected.to_dicts()
            elif hasattr(_selected, "to_dict"):
                _selected_rows = _selected.to_dict("records")
            elif isinstance(_selected, list):
                _selected_rows = _selected
            elif isinstance(_selected, dict):
                _selected_rows = [_selected]
            else:
                _selected_rows = []

            if not _selected_rows:
                _output = _default_msg
            else:
                # Get dates from selection
                _selected_dates: list = []
                for _row in _selected_rows:
                    _date_val = _row.get("date")
                    if _date_val:
                        if isinstance(_date_val, str):
                            # Parse ISO date string
                            _selected_dates.append(datetime.datetime.fromisoformat(_date_val.replace("Z", "+00:00")).date())
                        elif hasattr(_date_val, "date"):
                            _selected_dates.append(_date_val.date())
                        else:
                            _selected_dates.append(_date_val)

                if not _selected_dates:
                    _output = mo.vstack(
                        [
                            _default_msg,
                            mo.callout(
                                mo.md(f"⚠️ Could not parse dates. Keys: `{list(_selected_rows[0].keys())}`"), kind="warn"
                            ),
                        ]
                    )
                else:
                    # Filter to selected dates (prefix with _ to avoid marimo graph conflicts)
                    _min_sel_date = min(_selected_dates)
                    _max_sel_date = max(_selected_dates)

                    _filtered_data = metric_values_df.filter(
                        (pl.col("date") >= _min_sel_date) & (pl.col("date") <= _max_sel_date)
                    )

                    # Build aggregation expressions for all selected metrics
                    _agg_exprs = [pl.len().alias("completed_runs")]
                    for _metric in selected_metrics:
                        _agg_exprs.append(pl.col(_metric).sum().alias(f"{_metric}_sum"))

                    # Aggregate by transformation
                    _trafo_summary = _filtered_data.group_by("ts_external_id").agg(_agg_exprs)

                    # Calculate total across all metrics, filter zeros, sort by name
                    _sum_cols = [f"{m}_sum" for m in selected_metrics]
                    _trafo_summary = (
                        _trafo_summary.with_columns(pl.sum_horizontal(_sum_cols).alias("total_all_metrics"))
                        .filter(pl.col("total_all_metrics") > 0)  # Hide rows with all zeros
                        .sort("ts_external_id")  # Sort alphabetically by transformation
                    )

                    # === TREND CALCULATION (per metric) ===
                    # Compare first half vs second half of the selected period
                    _unique_dates = sorted(_selected_dates)
                    _num_days = len(_unique_dates)
                    _show_trend = _num_days >= 2  # Need at least 2 days for trend

                    if _show_trend:
                        # Split dates into first half and second half
                        _mid_idx = _num_days // 2
                        _first_half_dates = set(_unique_dates[:_mid_idx])
                        _second_half_dates = set(_unique_dates[_mid_idx:])

                        # Aggregate first half by transformation (per metric)
                        _first_half_data = metric_values_df.filter(pl.col("date").is_in(list(_first_half_dates)))
                        _first_half_agg = _first_half_data.group_by("ts_external_id").agg(
                            [pl.col(m).sum().alias(f"{m}_first") for m in selected_metrics]
                        )

                        # Aggregate second half by transformation (per metric)
                        _second_half_data = metric_values_df.filter(pl.col("date").is_in(list(_second_half_dates)))
                        _second_half_agg = _second_half_data.group_by("ts_external_id").agg(
                            [pl.col(m).sum().alias(f"{m}_second") for m in selected_metrics]
                        )

                        # Join trend data to summary
                        _trafo_summary = _trafo_summary.join(_first_half_agg, on="ts_external_id", how="left").join(
                            _second_half_agg, on="ts_external_id", how="left"
                        )

                        # Calculate trend emoji + percentage
                        def _format_trend(first_val, second_val):
                            if first_val is None or second_val is None:
                                return "—"
                            if first_val == 0 and second_val == 0:
                                return "➡️ 0%"
                            if first_val == 0:
                                return "📈 +∞"  # From zero to something
                            _pct_change = ((second_val - first_val) / first_val) * 100
                            if _pct_change > 5:
                                return f"📈 +{_pct_change:.0f}%"
                            elif _pct_change < -5:
                                return f"📉 {_pct_change:.0f}%"
                            else:
                                return f"➡️ {_pct_change:+.0f}%"

                        # Add trend column for EACH metric
                        for _metric in selected_metrics:
                            _first_col = f"{_metric}_first"
                            _second_col = f"{_metric}_second"
                            _trafo_summary = _trafo_summary.with_columns(
                                pl.struct([_first_col, _second_col])
                                .map_elements(
                                    lambda row, fc=_first_col, sc=_second_col: _format_trend(row[fc], row[sc]),
                                    return_dtype=pl.Utf8,
                                )
                                .alias(f"Trend: {_metric}")
                            )

                        # Drop helper columns (first/second aggregates)
                        _cols_to_drop = [f"{m}_first" for m in selected_metrics] + [f"{m}_second" for m in selected_metrics]
                        _trafo_summary = _trafo_summary.drop(_cols_to_drop)

                    _date_range_str = (
                        f"**{_min_sel_date}**"
                        if _min_sel_date == _max_sel_date
                        else f"**{_min_sel_date}** to **{_max_sel_date}**"
                    )

                    # Rename columns for display (use full metric names)
                    _rename_map = {"ts_external_id": "Transformation", "completed_runs": "Runs"}
                    for _metric in selected_metrics:
                        _rename_map[f"{_metric}_sum"] = f"Σ {_metric}"

                    _trend_note = (
                        f" *(Trend per metric: 1st half {len(_first_half_dates)}d vs 2nd half {len(_second_half_dates)}d)*"
                        if _show_trend
                        else ""
                    )

                    # Create table with multi-selection enabled for trend visualization
                    trafo_details_table = mo.ui.table(
                        _trafo_summary.drop("total_all_metrics").rename(_rename_map),
                        selection="single",
                        page_size=10,
                    )

                    # Store data for the trend chart cell
                    trafo_trend_data = _filtered_data
                    trafo_trend_date_range = (_min_sel_date, _max_sel_date)

                    _output = mo.vstack(
                        [
                            mo.md(
                                f"### Transformations for {len(selected_metrics)} metrics on "
                                + (
                                    f"**{_min_sel_date.strftime('%a, %Y-%m-%d')}**"
                                    if _min_sel_date == _max_sel_date
                                    else f"**{_min_sel_date.strftime('%a, %Y-%m-%d')}** to **{_max_sel_date.strftime('%a, %Y-%m-%d')}**"
                                )
                            ),
                            mo.md(
                                f"**{len(_trafo_summary)}** transformations, **{_filtered_data.shape[0]:,}** completed runs{_trend_note}"
                            ),
                            mo.md("*Select a row to visualize its daily trend below* ⬇️"),
                            trafo_details_table,
                            mo.callout(
                                mo.md(
                                    "**Trend Calculation (experimental):** The selected date range is split into two halves. "
                                    "Each metric's values are **summed** for the first half and second half, "
                                    "then the percentage change is calculated.\n\n"
                                    "**Legend:** 📈 increase (>5%) · 📉 decrease (<-5%) · ➡️ stable (±5%) · — no data"
                                ),
                                kind="info",
                            ),
                        ]
                    )

    _output
    return trafo_details_table, trafo_trend_data


@app.cell(hide_code=True)
def create_daily_trend_chart(
    selected_metrics: list[str],
    trafo_details_table,
    trafo_trend_data,
):
    # alt, mo, and pl are available globally from app.setup
    # Show trend chart for selected transformations
    # Initialize return values
    trend_chart_element = None
    trend_raw_data = None
    trend_selected_trafos = None

    if trafo_details_table is None or trafo_trend_data is None:
        _output = mo.md("")  # Empty - waiting for table
    else:
        _table_selection = trafo_details_table.value

        if _table_selection is None or (hasattr(_table_selection, "__len__") and len(_table_selection) == 0):
            _output = mo.callout(
                mo.md("👆 **Select a transformation** from the table above to visualize its daily trend."),
                kind="info",
            )
        else:
            # Parse selected transformation (single selection)
            if hasattr(_table_selection, "to_dicts"):
                _rows = _table_selection.to_dicts()
            elif hasattr(_table_selection, "to_dict"):
                _rows = _table_selection.to_dict("records")
            elif isinstance(_table_selection, list):
                _rows = _table_selection
            elif isinstance(_table_selection, dict):
                _rows = [_table_selection]
            else:
                _rows = []

            _selected_trafo = _rows[0].get("Transformation") if _rows else None

            if not _selected_trafo or not selected_metrics:
                _output = mo.callout(
                    mo.md("⚠️ **Invalid Selection**\n\nNo valid transformation selected."),
                    kind="warn",
                )
            else:
                # Filter data to selected transformation
                _chart_data = trafo_trend_data.filter(pl.col("ts_external_id") == _selected_trafo)

                # Aggregate by date for each metric
                # Use fill_null(0) before sum to ensure nulls become 0, not propagate
                _agg_exprs = []
                for _metric in selected_metrics:
                    _agg_exprs.append(pl.col(_metric).fill_null(0).sum().alias(_metric))

                _daily_agg = _chart_data.group_by("date").agg(_agg_exprs).sort("date")

                # Altair's tableau10 color scheme (matches Daily Aggregation chart)
                _tableau10_colors = [
                    "#4c78a8",
                    "#f58518",
                    "#54a24b",
                    "#e45756",
                    "#72b7b2",
                    "#eeca3b",
                    "#b279a2",
                    "#ff9da6",
                    "#9d755d",
                    "#bab0ac",
                ]

                # Build chart with pre-melted data (same approach as Daily Aggregation)
                _trafo_short = _selected_trafo.split(":")[-1] if ":" in _selected_trafo else _selected_trafo

                # Melt to long format BEFORE passing to Altair (avoids vegafusion dependency)
                # From: date | metric1 | metric2 | metric3
                # To:   date | metric | value
                _trend_chart_data = _daily_agg.unpivot(
                    index="date",
                    on=selected_metrics,
                    variable_name="metric",
                    value_name="value",
                ).to_pandas()

                # Fill NaN values
                _trend_chart_data["value"] = _trend_chart_data["value"].fillna(0)

                # Define explicit color scale matching tableau10 for consistency
                _color_scale = alt.Scale(domain=selected_metrics, range=_tableau10_colors[: len(selected_metrics)])

                # Brush selection for date range drill-down (same as Daily Aggregation)
                _trend_brush = alt.selection_interval(encodings=["x"], name="trend_brush", empty=False)

                # Simple multi-line chart matching Daily Aggregation style
                _chart = (
                    alt.Chart(_trend_chart_data)
                    .mark_line(point=True, strokeWidth=2)
                    .encode(
                        x=alt.X("date:T", title="Date"),
                        y=alt.Y("value:Q", title="Metric Value"),
                        color=alt.Color(
                            "metric:N",
                            title="Metric",
                            scale=_color_scale,
                            legend=alt.Legend(orient="bottom", columns=3),
                        ),
                        tooltip=["date:T", "metric:N", "value:Q"],
                        opacity=alt.condition(_trend_brush, alt.value(1), alt.value(0.5)),
                        strokeWidth=alt.condition(_trend_brush, alt.value(3), alt.value(1)),
                    )
                    .add_params(_trend_brush)
                    .properties(
                        width=700,
                        height=300,
                        title=f"Daily Trend: {_trafo_short} - drag to select date range",
                    )
                )

                # Wrap chart in mo.ui.altair_chart for selection support
                trend_chart_element = mo.ui.altair_chart(_chart)
                trend_raw_data = _chart_data
                trend_selected_trafos = [_selected_trafo]

                _output = mo.vstack(
                    [
                        mo.md(f"### Daily Trend: {_trafo_short}"),
                        mo.md(f"Showing **{len(selected_metrics)}** metrics"),
                        trend_chart_element,
                        mo.md("*Drag to select date range for job details below*"),
                    ]
                )

    _output
    return trend_chart_element, trend_raw_data, trend_selected_trafos


@app.cell(hide_code=True)
def show_trend_job_details(
    selected_metrics: list[str],
    trend_chart_element,
    trend_raw_data,
    trend_selected_trafos,
):
    # Show job details when date range is selected in Daily Trend chart
    # Same logic as Daily Aggregation -> Transformations table
    _default_msg = mo.md("🖱️ **Drag** on the chart above to select a date range and see job details.")

    if trend_chart_element is None or trend_raw_data is None or trend_selected_trafos is None:
        _output = mo.md("")  # Empty - waiting for chart
    else:
        # Get the transformation name for display
        _trafo_name = trend_selected_trafos[0] if trend_selected_trafos else "Unknown"
        _trafo_short = _trafo_name.split(":")[-1] if ":" in _trafo_name else _trafo_name

        # Get selection from chart (same approach as Daily Aggregation)
        _selected = trend_chart_element.value

        if _selected is None or (hasattr(_selected, "__len__") and len(_selected) == 0):
            _output = _default_msg
        else:
            # Parse selection to get dates
            if hasattr(_selected, "to_dicts"):
                _selected_rows = _selected.to_dicts()
            elif hasattr(_selected, "to_dict"):
                _selected_rows = _selected.to_dict("records")
            elif isinstance(_selected, list):
                _selected_rows = _selected
            elif isinstance(_selected, dict):
                _selected_rows = [_selected]
            else:
                _selected_rows = []

            if not _selected_rows:
                _output = _default_msg
            else:
                # Get dates from selection
                _selected_dates = []
                for _row in _selected_rows:
                    _date_val = _row.get("date")
                    if _date_val:
                        if isinstance(_date_val, str):
                            _selected_dates.append(datetime.datetime.fromisoformat(_date_val.replace("Z", "+00:00")).date())
                        elif hasattr(_date_val, "date"):
                            _selected_dates.append(_date_val.date())
                        else:
                            _selected_dates.append(_date_val)

                if not _selected_dates:
                    _output = _default_msg
                else:
                    # Filter to selected date range
                    _min_sel_date = min(_selected_dates)
                    _max_sel_date = max(_selected_dates)
                    _date_range_str = (
                        f"{_min_sel_date}" if _min_sel_date == _max_sel_date else f"{_min_sel_date} to {_max_sel_date}"
                    )

                    _filtered = trend_raw_data.filter((pl.col("date") >= _min_sel_date) & (pl.col("date") <= _max_sel_date))

                    if _filtered is None or len(_filtered) == 0:
                        _output = mo.vstack(
                            [
                                mo.md(f"### Transformation Job Details: {_trafo_short}"),
                                mo.md(f"No jobs found for {_date_range_str}"),
                            ]
                        )
                    else:
                        # Convert to pandas for display
                        _filtered_pd = _filtered.to_pandas()

                        # Select relevant columns for display
                        _display_cols = [
                            "ts_external_id",
                            "tsj_job_id",
                            "tsj_status",
                            "date",
                            "tsj_started_time",
                            "tsj_finished_time",
                        ]
                        for _m in selected_metrics:
                            if _m in _filtered_pd.columns:
                                _display_cols.append(_m)

                        _available_cols = [c for c in _display_cols if c in _filtered_pd.columns]
                        _raw_display = _filtered_pd[_available_cols].copy()

                        # Sort by date and start time
                        _sort_cols = [c for c in ["date", "tsj_started_time"] if c in _raw_display.columns]
                        if _sort_cols:
                            _raw_display = _raw_display.sort_values(_sort_cols)

                        _output = mo.vstack(
                            [
                                mo.md(f"### Transformation Job Details: {_trafo_short}"),
                                mo.md(f"**{len(_raw_display):,}** jobs from {_date_range_str}"),
                                mo.ui.table(_raw_display, selection=None, page_size=20)
                                if len(_raw_display) > 0
                                else mo.md("No jobs found."),
                            ]
                        )

    _output
    return


@app.cell(hide_code=True)
def chapter6_export_header():
    # mo is available globally from app.setup
    mo.md("""
    ## Chapter 6: Save Results

    Export analysis results for further processing or sharing.
    """)
    return


@app.cell(hide_code=True)
def create_export_catalog(daily_metric_df, events_df, jobs_df):
    """Build a catalog of available datasets with metadata."""

    # Dataset definitions with friendly names and descriptions
    _dataset_catalog = {
        "jobs_data": {
            "name": "Jobs Data",
            "description": "Raw transformation job records with metrics",
            "source": "Chapter 3: Load Data",
            "group": "📊 Data Sources",
            "df": jobs_df,
        },
        "concurrency_events": {
            "name": "Concurrency Events",
            "description": "Job start/end events for timeline analysis",
            "source": "Chapter 4: Concurrency Dashboard",
            "group": "📈 Concurrency Dashboard",
            "df": events_df,
        },
        "daily_metrics": {
            "name": "Daily Metrics Aggregation",
            "description": "Metrics aggregated by day and transformation",
            "source": "Chapter 5: Metrics Dashboard",
            "group": "📉 Metrics Dashboard",
            "df": daily_metric_df,
        },
    }

    # Build available datasets with stats
    _available = []
    for _key, _info in _dataset_catalog.items():
        _df = _info["df"]
        if _df is not None and len(_df) > 0:
            _available.append(
                {
                    "key": _key,
                    "name": _info["name"],
                    "description": _info["description"],
                    "source": _info["source"],
                    "group": _info["group"],
                    "rows": len(_df),
                    "columns": len(_df.columns),
                    "df": _df,
                }
            )

    export_catalog = _available
    export_catalog
    return (export_catalog,)


@app.cell(hide_code=True)
def create_export_ui(export_catalog):
    """Create export UI with dataset selection, preview, and download options."""

    if not export_catalog:
        export_dataset_key = None
        export_format = None
        download_filename = None
        show_preview = None
        _output = mo.callout(
            mo.md("⏳ **Waiting for Data**\n\nLoad and analyze data in Chapters 3-5 first to enable export."),
            kind="info",
        )
    else:
        # Build dropdown options with friendly names
        _dropdown_options = {item["name"]: item["key"] for item in export_catalog}

        # Group datasets for display
        _groups = {}
        for item in export_catalog:
            _group = item["group"]
            if _group not in _groups:
                _groups[_group] = []
            _groups[_group].append(item)

        # Build catalog table
        _catalog_rows = []
        for _group, _items in _groups.items():
            for item in _items:
                _catalog_rows.append(f"| {item['name']} | {item['description']} | {item['rows']:,} | {item['columns']} |")

        _catalog_md = f"""### Available Datasets

    The initial and transformed data driving the visualizations.

    | Dataset | Description | Rows | Columns |
    |---------|-------------|-----:|--------:|
    {chr(10).join(_catalog_rows)}
    """

        # UI elements - value must be a label (key), not the internal value
        export_dataset_key = mo.ui.dropdown(
            options=_dropdown_options,
            value=list(_dropdown_options.keys())[0],
            label="Dataset",
        )

        export_format = mo.ui.dropdown(
            options=["CSV", "Parquet", "JSON"],
            value="CSV",
            label="Format",
        )

        # Get default filename with date prefix (YYMMDD-datasetkey)
        _date_prefix = datetime.datetime.now().strftime("%y%m%d")
        _default_key = export_catalog[0]["key"] if export_catalog else "export"
        _default_name = f"{_date_prefix}-{_default_key}"
        download_filename = mo.ui.text(
            value=_default_name,
            label="Filename (without extension)",
            full_width=True,
        )

        show_preview = mo.ui.checkbox(label="Show preview", value=False)

        _output = mo.vstack(
            [
                mo.md(_catalog_md),
                mo.md("### Export Settings"),
                export_dataset_key,
                export_format,
                download_filename,
                show_preview,
            ]
        )

    _output
    return download_filename, export_dataset_key, export_format, show_preview


@app.cell(hide_code=True)
def generate_export_preview(export_catalog, export_dataset_key, show_preview):
    """Show preview of selected dataset."""

    if not export_catalog or not export_dataset_key or not show_preview:
        _output = None
    elif not show_preview.value:
        _output = None
    else:
        # Find selected dataset
        _selected_key = export_dataset_key.value
        _selected = next((item for item in export_catalog if item["key"] == _selected_key), None)

        if _selected and _selected["df"] is not None:
            _df = _selected["df"]
            _output = mo.vstack(
                [
                    mo.md(f"### Preview: {_selected['name']} (first 10 rows)"),
                    mo.ui.table(_df.head(10), selection=None),
                ]
            )
        else:
            _output = mo.callout(mo.md("⚠️ **No data available for preview**"), kind="warn")

    _output
    return


@app.cell(hide_code=True)
def generate_download(
    download_filename,
    export_catalog,
    export_dataset_key,
    export_format,
):
    """Generate download button for selected dataset."""

    if not export_catalog or not export_dataset_key or not export_format or not download_filename:
        _output = mo.callout(
            mo.md("👆 **Configure Export**\n\nSelect a dataset above to enable download."),
            kind="info",
        )
    else:
        # Find selected dataset
        _selected_key = export_dataset_key.value
        _selected = next((item for item in export_catalog if item["key"] == _selected_key), None)

        if _selected and _selected["df"] is not None and len(_selected["df"]) > 0:
            _df = _selected["df"]
            _format_val = export_format.value
            _base_filename = download_filename.value or _selected_key

            if _format_val == "CSV":
                _export_data = _df.write_csv()
                _filename = f"{_base_filename}.csv"
                _mime = "text/csv"
            elif _format_val == "Parquet":
                import io

                _buffer = io.BytesIO()
                _df.write_parquet(_buffer)
                _export_data = _buffer.getvalue()
                _filename = f"{_base_filename}.parquet"
                _mime = "application/octet-stream"
            else:  # JSON
                _export_data = _df.write_json()
                _filename = f"{_base_filename}.json"
                _mime = "application/json"

            _output = mo.vstack(
                [
                    mo.md(f"### Download Ready"),
                    mo.callout(
                        mo.vstack(
                            [
                                mo.md(f"**{_selected['name']}** → `{_filename}`"),
                                mo.md(f"{_selected['rows']:,} rows × {_selected['columns']} columns"),
                                mo.download(
                                    data=_export_data,
                                    filename=_filename,
                                    mimetype=_mime,
                                    label=f"⬇️ Download {_filename}",
                                ),
                            ]
                        ),
                        kind="success",
                    ),
                ]
            )
        else:
            _output = mo.callout(
                mo.md("⚠️ **Empty Dataset**\n\nThe selected dataset has no data to export."),
                kind="warn",
            )

    _output
    return


@app.cell(column=2, hide_code=True)
def chapter7_tests_header():
    # Only show tests chapter in edit mode (hidden in run/app mode)
    _is_edit_mode = mo.app_meta().mode == "edit"

    if not _is_edit_mode:
        _output = None
    else:
        _output = mo.md("""
    ## Chapter 7: Tests

    This chapter contains pytest-compatible test functions for validating the notebook's functionality.
    **Only visible in edit mode.**

    **Run tests from the command line:**
    ```bash
    uv run pytest marimo-tsjm-analysis.py -v
    ```

    These tests verify:
    - Configuration loading
    - Data processing functions
    - Export utilities
    - Concurrency calculations
    - Metrics aggregation
        """)

    _output
    return


@app.cell
def test_cdf_client_initialization(available_projects: list[str], cdf_client):
    # Test that CDF client and projects are properly initialized
    # Note: These will be None if form not submitted
    if cdf_client is not None:
        # Client should have a config with client_name
        assert hasattr(cdf_client, "config")
        assert cdf_client.config.client_name == "inso-tsjm-exporter-marimo"
    if available_projects is not None:
        # available_projects should be a list of strings
        assert isinstance(available_projects, list)
        for proj in available_projects:
            assert isinstance(proj, str)
    return


@app.cell
def test_output_folder_is_valid_path(output_folder_value):
    # Path is available globally from app.setup
    # Test that output folder path is valid (after form submission)
    # Note: output_folder_value can be None if form not submitted
    if output_folder_value is not None:
        path = Path(output_folder_value)
        assert isinstance(path, Path)
    return


@app.cell
def test_config_form_has_required_fields(config_form):
    # Test that config form is properly set up with env_path and output_path fields
    assert config_form is not None
    # The form should have a value attribute (None if not submitted, dict if submitted)
    assert hasattr(config_form, "value")
    if config_form.value is not None:
        # New form structure: env_path and output_path fields
        assert "env_path" in config_form.value, "Form should have env_path field"
        assert "output_path" in config_form.value, "Form should have output_path field"
        # Both should be strings (paths)
        assert isinstance(config_form.value["env_path"], str)
        assert isinstance(config_form.value["output_path"], str)
    return


@app.cell
def test_process_single_transformation_exists(process_single_transformation):
    # Test that process_single_transformation function is defined
    assert callable(process_single_transformation)
    return


@app.cell
def test_jsonl_file_path_exists(jsonl_file_path):
    # Test that JSONL file path input is defined
    assert jsonl_file_path is not None
    assert hasattr(jsonl_file_path, "value")
    return


@app.cell
def test_concurrency_calculation_logic():
    # datetime and pl are available globally from app.setup
    # Test concurrency calculation with sample data
    _sample_events = pl.DataFrame(
        {
            "timestamp": [
                datetime.datetime(2024, 1, 1, 10, 0),
                datetime.datetime(2024, 1, 1, 10, 5),
                datetime.datetime(2024, 1, 1, 10, 10),
                datetime.datetime(2024, 1, 1, 10, 15),
            ],
            "change": [1, 1, -1, -1],
            "event_type": ["start", "start", "end", "end"],
            "ts_external_id": ["job1", "job2", "job1", "job2"],
        }
    )
    _result = _sample_events.sort("timestamp").with_columns(pl.col("change").cum_sum().alias("concurrency"))
    _concurrencies = _result["concurrency"].to_list()
    assert _concurrencies == [1, 2, 1, 0], f"Expected [1, 2, 1, 0], got {_concurrencies}"
    return


@app.cell
def test_peak_concurrency_detection():
    # datetime and pl are available globally from app.setup
    # Test peak concurrency detection
    _events = pl.DataFrame(
        {
            "timestamp": [
                datetime.datetime(2024, 1, 1, 10, 0),
                datetime.datetime(2024, 1, 1, 10, 5),
                datetime.datetime(2024, 1, 1, 11, 0),
            ],
            "concurrency": [1, 5, 2],
        }
    )
    _peak = _events["concurrency"].max()
    assert _peak == 5, f"Expected peak of 5, got {_peak}"
    return


@app.cell
def test_json_metric_extraction():
    # pl is available globally from app.setup
    # Test extracting metrics from JSON strings
    _sample_df = pl.DataFrame(
        {
            "tsjm_last_counts": [
                '{"instances.upsertedNoop": 100, "instances.created": 5}',
                '{"instances.upsertedNoop": 200, "instances.created": 10}',
                "{}",
                None,
            ]
        }
    )
    _result = _sample_df.with_columns(
        pl.col("tsjm_last_counts")
        .str.json_path_match("$.['instances.upsertedNoop']")
        .cast(pl.Int64, strict=False)
        .alias("upserted_noop")
    )
    _values = _result["upserted_noop"].to_list()
    assert _values[0] == 100, f"Expected 100, got {_values[0]}"
    assert _values[1] == 200, f"Expected 200, got {_values[1]}"
    assert _values[2] is None, f"Expected None for empty JSON, got {_values[2]}"
    return


@app.cell
def test_daily_aggregation():
    # datetime and pl are available globally from app.setup
    # Test daily aggregation of metrics
    _sample_df = pl.DataFrame(
        {
            "date": [
                datetime.datetime(2024, 1, 1),
                datetime.datetime(2024, 1, 1),
                datetime.datetime(2024, 1, 2),
            ],
            "metric": [10, 20, 30],
        }
    )
    _daily = _sample_df.group_by("date").agg(pl.col("metric").sum())
    assert len(_daily) == 2, f"Expected 2 days, got {len(_daily)}"
    return


@app.cell
def test_timestamp_conversion_with_nulls():
    # pl is available globally from app.setup
    # Test loading and converting epoch timestamps with null handling
    # Simulates the data loading logic from Chapter 3

    # Mock data with epoch milliseconds (like exported JSONL)
    _mock_data = pl.DataFrame(
        {
            "tsj_job_id": ["1", "2", "3", "4"],  # String IDs (no thousand separators)
            "tsj_status": ["Completed", "Completed", "Failed", "Running"],
            # tsj_created_time - required, no nulls (epoch ms)
            "tsj_created_time": [1700000000000, 1700000100000, 1700000200000, 1700000300000],
            # tsj_started_time - can have nulls
            "tsj_started_time": [1700000010000, 1700000110000, None, 1700000310000],
            # tsj_finished_time - can have nulls
            "tsj_finished_time": [1700000050000, 1700000150000, None, None],
            # tsj_last_seen_time - can have nulls
            "tsj_last_seen_time": [1700000050000, None, None, 1700000350000],
        }
    )

    # Apply the same conversion logic as in the data loading cell
    _converted = _mock_data.with_columns(
        [
            pl.from_epoch(pl.col("tsj_created_time"), time_unit="ms").alias("tsj_created_time"),
            pl.when(pl.col("tsj_started_time").is_not_null())
            .then(pl.from_epoch(pl.col("tsj_started_time"), time_unit="ms"))
            .otherwise(None)
            .alias("tsj_started_time"),
            pl.when(pl.col("tsj_finished_time").is_not_null())
            .then(pl.from_epoch(pl.col("tsj_finished_time"), time_unit="ms"))
            .otherwise(None)
            .alias("tsj_finished_time"),
            pl.when(pl.col("tsj_last_seen_time").is_not_null())
            .then(pl.from_epoch(pl.col("tsj_last_seen_time"), time_unit="ms"))
            .otherwise(None)
            .alias("tsj_last_seen_time"),
        ]
    )

    # Verify tsj_created_time has no nulls and is datetime
    assert _converted["tsj_created_time"].null_count() == 0, "tsj_created_time should have no nulls"
    assert _converted["tsj_created_time"].dtype == pl.Datetime("ms"), "tsj_created_time should be Datetime"

    # Verify nullable columns handle nulls correctly
    assert _converted["tsj_started_time"].null_count() == 1, "tsj_started_time should have 1 null"
    assert _converted["tsj_finished_time"].null_count() == 2, "tsj_finished_time should have 2 nulls"
    assert _converted["tsj_last_seen_time"].null_count() == 2, "tsj_last_seen_time should have 2 nulls"

    # Verify non-null values are properly converted to datetime
    _non_null_started = _converted.filter(pl.col("tsj_started_time").is_not_null())
    assert len(_non_null_started) == 3, "Should have 3 non-null started times"

    # Verify the actual datetime values (spot check first row)
    _first_created = _converted["tsj_created_time"][0]
    assert _first_created.year == 2023, f"Expected year 2023, got {_first_created.year}"
    return


if __name__ == "__main__":
    app.run()
