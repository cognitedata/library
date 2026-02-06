# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "polars>=1.0.0",
#     "altair>=5.0.0,<6.0.0",
#     "python-dotenv>=1.0.0",
#     "pyarrow>=18.0.0",
#     "cognite-toolkit==0.7.46",
# ]
# ///

import marimo

__generated_with = "0.19.4"
app = marimo.App(width="columns", app_title="TSJM bench")


@app.cell(column=0, hide_code=True)
def import_marimo_and_title():
    import marimo as mo

    mo.md(
        """
        # TSJM Analysis Notebook

        **Transformation-Job-Metrics (TSJM) Analysis Tool**

        This notebook provides tools for:
        1. Exporting transformation job metrics from Cognite Data Fusion (CDF)
        2. Analyzing job concurrency patterns
        3. Aggregating metrics across transformations

        ---
        """
    )
    return (mo,)


@app.cell(hide_code=True)
def chapter1_setup_header(mo):
    mo.md("""
    ## Chapter 1: Setup and Configuration

    Configure the environment and load credentials for CDF access.
    """)
    return


@app.cell(hide_code=True)
def import_core_libraries():
    import polars as pl
    import altair as alt
    from pathlib import Path
    import json
    import datetime

    # deactivate the '...' menu in altair charts to download the graph in different formats
    alt.renderers.set_embed_options(actions=False)
    return Path, alt, datetime, json, pl


@app.cell
def create_config_form(Path, mo):
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
                value=str(Path.home() / ".env"),
                full_width=True,
            ),
            "output_path": mo.ui.text(
                value="~/tmp/",
                full_width=True,
            ),
        },
    ).form(submit_button_label="🔗 Connect to CDF")

    config_form
    return (config_form,)


@app.cell(hide_code=True)
def init_cdf_client(Path, config_form, mo):
    """
    Initialize CogniteClient using cognite-toolkit's EnvironmentVariables.

    This uses the same authentication logic as `cdf-toolkit` CLI,
    supporting all login flows: interactive, client_credentials, device_code, token.
    """

    import os
    from dotenv import load_dotenv

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
                from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

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
def create_project_selector(available_projects: list[str], cdf_client, mo):
    import pandas as pd

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
    mo,
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
            _output = mo.md("⏳ Select a project from the table above.")
    elif available_projects:
        _output = mo.md("⏳ Select a project from the table above.")
    else:
        _output = mo.md("")

    _output
    return active_client, selected_project


@app.cell(hide_code=True)
def chapter2_export_header(mo):
    mo.md("""
    ## Chapter 2: Execution Export (Cognite API)

    Export execution data from CDF resources:
    - **TSJM**: Transformation Job Metrics
    - **WFE**: Workflow Executions

    Both formats use a common JSONL structure (see `COMMON_JSONL_FORMAT.md`).

    **Prerequisites:**
    - ✅ Connected to CDF (Chapter 1)
    - ✅ Selected a CDF project
    """)
    return


@app.cell
def create_export_controls(
    Path,
    active_client,
    datetime,
    mo,
    output_folder_value,
    selected_project,
):
    # Check if client is ready (local variable, prefixed with _)
    _client_ready = active_client is not None and selected_project is not None

    # Get total transformation count (fetches metadata only, relatively fast)
    total_transformations = 0
    if _client_ready:
        try:
            total_transformations = len(list(active_client.transformations.list(limit=None)))
        except Exception:
            # Fallback: count is not critical
            pass

    # Export configuration controls
    max_workers_slider = mo.ui.slider(
        start=1,
        stop=16,
        value=4,
        step=1,
        label="Max Workers",
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

    # Local variables for display (prefixed with _)
    _status_indicator = "🟢" if _client_ready else "🔴"
    _status_text = f"Connected to `{selected_project}`" if _client_ready else "Select a project in Chapter 1 first"

    # Generate export filename preview (use project name directly)
    _datestamp_preview = datetime.datetime.now(datetime.UTC).strftime("%y%m%d")
    _output_folder = output_folder_value or "/tmp"
    _export_filename = f"{_datestamp_preview}-{selected_project or 'unknown'}-tsjm-dump.jsonl"
    export_filepath = Path(_output_folder) / _export_filename

    # Transformation limit (0 = no limit, useful for testing)
    _trafo_limit_label = (
        f"Transformation Limit ({total_transformations} available, set 0 to load all)"
        if total_transformations > 0
        else "Transformation Limit (connect to see available count)"
    )
    trafo_limit_input = mo.ui.number(
        start=0,
        stop=max(total_transformations, 1000) if total_transformations > 0 else 1000,
        value=10,
        step=10,
        label=_trafo_limit_label,
    )

    # Export button (only enabled if client is ready)
    export_tsjm_button = mo.ui.run_button(
        label="🚀 Start TSJM Export",
        disabled=not _client_ready,
    )

    _output_tsjm_export_controls = None

    if _client_ready:
        _output_tsjm_export_controls = mo.vstack(
            [
                mo.md("""### Transformation Job Metrics (TSJM) Export

    The **defaults** are good for testing functionality with a small subset.  
    **For bulk download:** increase *Max Workers* (8-12) and set *Transformation Limit* to **0** (all).
                """),
                mo.hstack(
                    [
                        mo.md(f"**CDF Connection:** {_status_indicator} {_status_text}"),
                    ],
                    justify="start",
                ),
                mo.md(f"**Output File:** `{_export_filename}`"),
                max_workers_slider,
                jobs_per_trafo_slider,
                trafo_limit_input,
                export_tsjm_button,
            ]
        )
    else:
        _output_tsjm_export_controls = mo.vstack(
            [
                mo.md("### Transformation Job Metrics (TSJM) Export"),
                mo.callout(
                    mo.md(f"⚠️ **{_status_text}** before you can export."),
                    kind="warn",
                ),
                max_workers_slider,
                jobs_per_trafo_slider,
                trafo_limit_input,
                export_tsjm_button,
            ]
        )

    # Return client_ready (without underscore) for potential use by other cells
    client_ready = _client_ready

    _output_tsjm_export_controls
    return (
        export_filepath,
        export_tsjm_button,
        jobs_per_trafo_slider,
        max_workers_slider,
        trafo_limit_input,
    )


@app.cell(hide_code=True)
def define_export_functions(json):
    import threading
    import traceback as _traceback  # Local import, prefixed to avoid conflicts
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from typing import Any


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
                # Get metrics as JSON string
                metrics_json = "{}"
                if tsjm := tsj.metrics():
                    metrics_dict = tsjm.to_pandas().groupby("name")["count"].last().to_dict()
                    metrics_json = json.dumps(metrics_dict)

                # Convert timestamps to milliseconds
                # Helper function to convert timestamps (handles both datetime and int)
                def to_ms(ts):
                    if ts is None:
                        return None
                    if isinstance(ts, int):
                        return ts  # Already in milliseconds
                    if hasattr(ts, "timestamp"):
                        return int(ts.timestamp() * 1000)  # datetime object
                    return None

                # Build record with both common format and legacy fields
                record = {
                    # Common format fields (for unified analysis)
                    "resource_type": "tsjm",
                    "project": project,
                    "resource_id": str(ts.id),
                    "resource_external_id": ts.external_id,
                    "execution_id": str(tsj.id),
                    "created_time": to_ms(tsj.created_time),
                    "started_time": to_ms(tsj.started_time),
                    "finished_time": to_ms(tsj.finished_time),
                    "status": tsj.status.value,
                    "error": tsj.error,
                    "metrics": metrics_json,
                    # TSJM-specific fields
                    "ts_no": ts_no,
                    "tsj_no": j,
                    "last_seen_time": to_ms(tsj.last_seen_time),
                    # Legacy fields (for backward compatibility)
                    "ts_id": str(ts.id),
                    "ts_external_id": ts.external_id,
                    "tsj_job_id": str(tsj.id),
                    "tsj_created_time": to_ms(tsj.created_time),
                    "tsj_started_time": to_ms(tsj.started_time),
                    "tsj_finished_time": to_ms(tsj.finished_time),
                    "tsj_last_seen_time": to_ms(tsj.last_seen_time),
                    "tsj_error": tsj.error,
                    "tsj_status": tsj.status.value,
                    "tsjm_last_counts": metrics_json,
                }
                results.append(record)
        except Exception as e:
            error_msg = f"Error processing transformation {ts.external_id} (ts_no={ts_no}): {type(e).__name__}: {str(e)}"
            print(error_msg)
            print(f"Traceback:\n{_traceback.format_exc()}")
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
    return (
        ThreadPoolExecutor,
        as_completed,
        process_single_transformation,
        threading,
    )


@app.cell
def run_tsjm_export(
    ThreadPoolExecutor,
    active_client,
    as_completed,
    export_filepath,
    export_tsjm_button,
    jobs_per_trafo_slider,
    json,
    max_workers_slider,
    mo,
    process_single_transformation,
    selected_project,
    threading,
    trafo_limit_input,
):
    import time

    _output_tsjm_export_results = mo.md("Click **Start Export** to begin downloading transformation job metrics.")
    export_result = {}  # dict with export results
    exported_file_path = None  # path to exported file

    if export_tsjm_button.value and active_client is not None and selected_project is not None:
        # Initialize output stream
        mo.output.append(mo.md("📋 Starting transformation job metrics export..."))

        try:
            # Start timing
            start_time = time.time()

            # Use the pre-initialized client from Chapter 1
            client = active_client

            # Step 1: Fetch transformations
            mo.output.append(mo.md("📋 Fetching transformations..."))
            transformations = list(client.transformations.list(limit=None))
            total_available = len(transformations)
            mo.output.append(mo.md(f"✅ Found {total_available} transformations"))

            # Step 2: Apply transformation limit (0 = no limit)
            trafo_limit = trafo_limit_input.value
            if trafo_limit > 0:
                transformations = transformations[:trafo_limit]

            mo.output.append(
                mo.md(
                    f"📊 Processing {len(transformations)} transformations (limit: {trafo_limit if trafo_limit > 0 else 'all'})"
                )
            )
            mo.output.append(mo.md(f"📊 Jobs per transformation: {jobs_per_trafo_slider.value}"))

            # Use progress bar with actual count of transformations
            with mo.status.progress_bar(total=len(transformations)) as _progress:
                _progress.update(0)

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
                    mo.output.append(
                        mo.callout(
                            mo.md(f"""
                        ⚠️ **No Transformations Found**

                        No transformations found in project `{selected_project}`.

                        **Transformations Available:** {total_available}

                        **Tip:** Check that transformations exist in project `{selected_project}`.
                        """),
                            kind="warn",
                        )
                    )
                    _output_tsjm_export_results = mo.md("")  # Empty, content was appended above
                else:
                    # Use pre-computed output filepath (local variable, prefixed with _)
                    _output_file = export_filepath
                    batch_size = 100

                    # Prepare arguments for each transformation
                    transformation_args = [
                        (i, ts, selected_project, jobs_per_trafo_slider.value, client)
                        for i, ts in enumerate(transformations)
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
                                    with open(str(_output_file), mode) as f:
                                        f.write("\n".join(json.dumps(item) for item in write_state["buffer"]) + "\n")
                                    write_state["buffer"].clear()
                                    write_state["first_write"] = False

                    # Submit all tasks
                    with ThreadPoolExecutor(max_workers=max_workers_slider.value) as executor:
                        future_to_ts = {
                            executor.submit(process_single_transformation, args): args[1] for args in transformation_args
                        }

                        # Process transformations and update progress based on count
                        for future in as_completed(future_to_ts):
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

                            # Update progress: increment by 1 for each completed transformation
                            _progress.update()

                    # Flush remaining buffer
                    write_batch_to_file([], force=True)

                    # Step 5: Write to file
                    if total_rows > 0:
                        mo.output.append(mo.md(f"💾 Writing {total_rows:,} records to file..."))

                    # Calculate elapsed time
                    elapsed_time = time.time() - start_time
                    elapsed_str = (
                        f"{elapsed_time / 60:.1f} minutes" if elapsed_time >= 60 else f"{elapsed_time:.1f} seconds"
                    )

                    # Determine status based on errors
                    if export_errors:
                        error_count = len(export_errors)
                        # Check for critical errors like missing pandas
                        has_critical_error = any("CogniteImportError" in e or "pandas" in e.lower() for e in export_errors)

                        if has_critical_error:
                            export_result = {
                                "status": "failed",
                                "rows": total_rows,
                                "file": str(_output_file),
                                "transformations": len(transformations),
                                "transformations_available": total_available,
                                "elapsed_seconds": elapsed_time,
                                "errors": export_errors,
                            }
                            # Show first few errors
                            error_sample = "\n".join(f"- {e}" for e in export_errors[:5])
                            if error_count > 5:
                                error_sample += f"\n- ... and {error_count - 5} more errors"

                            mo.output.append(
                                mo.callout(
                                    mo.md(f"""
                                ❌ **Export Failed**

                                **Critical Error**: Missing required dependency (pandas).

                                The `cognite-sdk` requires pandas for `.to_pandas()` functionality.
                                This should be fixed by reinstalling with `cognite-sdk[pandas]`.

                                **Errors ({error_count}):**
                                {error_sample}

                                **Metrics:**
                                - Rows Written: {total_rows:,}
                                - Transformations: {len(transformations)}{f" (limited from {total_available})" if trafo_limit > 0 else ""}
                                - Errors: {error_count}
                                - Processing Time: {elapsed_str}
                                """),
                                    kind="danger",
                                )
                            )
                            _output_tsjm_export_results = mo.md("")  # Empty, content was appended above
                        else:
                            export_result = {
                                "status": "partial",
                                "rows": total_rows,
                                "file": str(_output_file),
                                "transformations": len(transformations),
                                "transformations_available": total_available,
                                "elapsed_seconds": elapsed_time,
                                "errors": export_errors,
                            }
                            error_sample = "\n".join(f"- {e}" for e in export_errors[:5])
                            if error_count > 5:
                                error_sample += f"\n- ... and {error_count - 5} more errors"

                            mo.output.append(
                                mo.callout(
                                    mo.md(f"""
                                ⚠️ **Export Completed with Errors**

                                Some transformations failed to process.

                                **Errors ({error_count}):**
                                {error_sample}

                                **Metrics:**
                                - Total Rows: {total_rows:,}
                                - Transformations: {len(transformations)}{f" (limited from {total_available})" if trafo_limit > 0 else ""}
                                - Errors: {error_count}
                                - Processing Time: {elapsed_str}
                                - Output File: `{_output_file}`
                                """),
                                    kind="warn",
                                )
                            )
                            _output_tsjm_export_results = mo.md("")  # Empty, content was appended above
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

                            mo.output.append(
                                mo.callout(
                                    mo.md(f"""
                                ⚠️ **Export Completed - No Jobs Found**

                                The export ran successfully but no job data was found.
                                No output file was created.

                                **Metrics:**
                                - Transformations Checked: {len(transformations)}{f" (limited from {total_available})" if trafo_limit > 0 else ""}
                                - Jobs Found: 0
                                - Processing Time: {elapsed_str}

                                **Possible reasons:**
                                - Transformations have no recent job history
                                - Jobs per transformation limit (`{jobs_per_trafo_slider.value}`) returned no results
                                """),
                                    kind="warn",
                                )
                            )
                            _output_tsjm_export_results = mo.md("")  # Empty, content was appended above
                        else:
                            export_result = {
                                "status": "success",
                                "rows": total_rows,
                                "file": str(_output_file),
                                "transformations": len(transformations),
                                "transformations_available": total_available,
                                "elapsed_seconds": elapsed_time,
                                "errors": [],
                            }

                            mo.output.append(
                                mo.callout(
                                    mo.md(f"""
                            ✅ **Export Completed Successfully!**

                            - **File:** `{_output_file.name}`
                            - **Location:** `{_output_file.parent}`
                            - **Records:** {total_rows:,}
                            - **Transformations:** {len(transformations)}{f" (limited from {total_available})" if trafo_limit > 0 else ""}
                            - **Processing Time:** {elapsed_str}
                            - **Format:** Common JSONL (see COMMON_JSONL_FORMAT.md)
                            """),
                                    kind="success",
                                )
                            )
                            _output_tsjm_export_results = mo.md("")  # Empty, content was appended above

        except ImportError as e:
            mo.output.append(
                mo.callout(
                    mo.md(f"""
                ❌ **Import Error**

                Missing required package: `{str(e)}`

                Make sure `cognite-sdk[pandas]` is installed.
                """),
                    kind="danger",
                )
            )
            _output_tsjm_export_results = mo.md("")  # Empty, content was appended above
        except Exception as e:
            mo.output.append(mo.callout(mo.md(f"❌ **Export failed:** {type(e).__name__}: {str(e)}"), kind="danger"))
            _output_tsjm_export_results = mo.md("")  # Empty, content was appended above
    elif export_tsjm_button.value:
        _output_tsjm_export_results = mo.md("⚠️ Please select a CDF project in Chapter 1 first.")

    # Append to output stream (after progress bar)
    mo.output.append(_output_tsjm_export_results)

    # Return exported file path for use in Chapter 3 file selector
    exported_file_path = export_result.get("file") if export_result.get("status") == "success" else None

    exported_file_path
    return (exported_file_path,)


@app.cell
def create_wfe_export_controls(
    Path,
    active_client,
    datetime,
    mo,
    output_folder_value,
    selected_project,
):
    """
    Export controls for Workflow Executions (WFE).
    Similar structure to TSJM export controls.
    """

    # Check if client is ready (local variable, prefixed with _)
    _client_ready = active_client is not None and selected_project is not None

    # Export configuration controls
    wf_limit_input = mo.ui.number(
        start=0,
        stop=1000,
        value=0,
        step=1,
        label="Workflow Limit (0 = all)",
        full_width=False,
    )

    wf_execution_limit_slider = mo.ui.slider(
        start=10,
        stop=500,
        value=200,
        step=10,
        label="Executions per Workflow",
        show_value=True,
    )

    # Local variables for display (prefixed with _)
    _status_indicator = "🟢" if _client_ready else "🔴"
    _status_text = f"Connected to `{selected_project}`" if _client_ready else "Select a project in Chapter 1 first"

    # Generate export filename preview
    _datestamp_preview = datetime.datetime.now(datetime.UTC).strftime("%y%m%d")
    _output_folder = output_folder_value or "/tmp"
    _export_filename = f"{_datestamp_preview}-{selected_project or 'unknown'}-wfe-dump.jsonl"
    export_wfe_filepath = Path(_output_folder) / _export_filename

    # Export button (only enabled if client is ready)
    export_wfe_button = mo.ui.run_button(
        label="🚀 Start WFE Export",
        disabled=not _client_ready,
    )

    _output_wfe_export_controls = None

    if _client_ready:
        _output_wfe_export_controls = mo.vstack(
            [
                mo.md("### Workflow Executions Export"),
                mo.md(f"""
                **Status:** {_status_indicator} {_status_text}

                **Output File:** `{_export_filename}`
                """),
                mo.md("**Configuration:**"),
                wf_limit_input,
                wf_execution_limit_slider,
                export_wfe_button,
            ]
        )
    else:
        _output_wfe_export_controls = mo.vstack(
            [
                mo.md("### Workflow Executions Export"),
                mo.callout(
                    mo.md(f"⚠️ **{_status_text}** before you can export."),
                    kind="warn",
                ),
                wf_limit_input,
                wf_execution_limit_slider,
                export_wfe_button,
            ]
        )

    _output_wfe_export_controls
    return (
        export_wfe_button,
        export_wfe_filepath,
        wf_execution_limit_slider,
        wf_limit_input,
    )


@app.cell
def run_wfj_export(
    Path,
    active_client,
    export_wfe_button,
    export_wfe_filepath,
    json,
    mo,
    selected_project,
    wf_execution_limit_slider,
    wf_limit_input,
):
    """
    Export workflow executions in common JSONL format.

    See COMMON_JSONL_FORMAT.md for format specification.
    """

    import threading as _threading  # Local import, prefixed to avoid conflicts
    import traceback as _traceback  # Local import, prefixed to avoid conflicts

    # Check if client is ready (local variable, prefixed with _)
    _client_ready = active_client is not None and selected_project is not None

    _status_indicator = "🟢" if _client_ready else "🔴"
    _status_text = f"Connected to `{selected_project}`" if _client_ready else "Select a project in Chapter 1 first"

    _output_wfe_export_result = mo.md("⏳ Ready to export workflow executions.")

    if _client_ready and export_wfe_button.value:
        # Initialize output stream
        mo.output.append(mo.md("📋 Starting workflow executions export..."))

        try:
            # Step 1: Fetch workflow versions
            mo.output.append(mo.md("📋 Fetching workflow versions..."))

            wfvs = active_client.workflows.versions.list(limit=None)

            mo.output.append(mo.md(f"✅ Found {len(wfvs)} workflow versions"))

            # Step 2: Keep only latest versions
            latest_wfvs_df = (
                wfvs.to_pandas()
                .sort_values("created_time", ascending=False)
                .drop_duplicates(subset="workflow_external_id", keep="first")
                .sort_values("workflow_external_id")
                .reset_index(drop=True)
            )

            mo.output.append(mo.md(f"✅ Filtered to {len(latest_wfvs_df)} latest workflow versions"))

            # Step 3: Apply limits
            WF_LIMIT = wf_limit_input.value if wf_limit_input.value > 0 else len(latest_wfvs_df)
            WF_EXECUTION_LIMIT = wf_execution_limit_slider.value

            workflows_to_process = latest_wfvs_df.head(WF_LIMIT)
            mo.output.append(
                mo.md(
                    f"📊 Processing {len(workflows_to_process)} workflows (limit: {WF_LIMIT if wf_limit_input.value > 0 else 'all'})"
                )
            )
            mo.output.append(mo.md(f"📊 Execution limit per workflow: {WF_EXECUTION_LIMIT}"))

            # Step 4: Fetch executions and convert to common format
            mo.output.append(mo.md("🔄 Fetching workflow executions..."))

            # Use progress bar with actual count of workflows
            with mo.status.progress_bar(total=len(workflows_to_process)) as _progress:
                _progress.update(0)

                all_records = []
                total_executions = 0

                # Workflows don't have numeric IDs - use None for resource_id
                for wf_no, (_, wf_row) in enumerate(workflows_to_process.iterrows()):
                    wf_ext_id = wf_row["workflow_external_id"]
                    wf_version = wf_row["version"]

                    try:
                        executions = list(
                            active_client.workflows.executions.list((wf_ext_id, wf_version), limit=WF_EXECUTION_LIMIT)
                        )

                        for wfe_no, wfe in enumerate(executions):
                            # Convert to common format
                            # Helper function to convert timestamps (handles both datetime and int)
                            def to_ms_ts(ts):
                                if ts is None:
                                    return None
                                if isinstance(ts, int):
                                    return ts  # Already in milliseconds
                                if hasattr(ts, "timestamp"):
                                    return int(ts.timestamp() * 1000)  # datetime object
                                return None

                            # Get error message from reason_for_incompletion if status is failed
                            _error_msg = None
                            if hasattr(wfe, "reason_for_incompletion") and wfe.reason_for_incompletion:
                                _error_msg = str(wfe.reason_for_incompletion)
                            elif hasattr(wfe, "error") and wfe.error:
                                _error_msg = str(wfe.error)

                            record = {
                                # Common fields
                                "resource_type": "wfe",
                                "project": selected_project,
                                "resource_id": None,  # Workflows don't have numeric IDs
                                "resource_external_id": wf_ext_id,
                                "execution_id": str(wfe.id),
                                "created_time": to_ms_ts(wfe.created_time) if hasattr(wfe, "created_time") else None,
                                "started_time": to_ms_ts(wfe.started_time) if hasattr(wfe, "started_time") else None,
                                "finished_time": to_ms_ts(wfe.finished_time) if hasattr(wfe, "finished_time") else None,
                                "status": wfe.status.value if hasattr(wfe.status, "value") else str(wfe.status),
                                "error": _error_msg,
                                "metrics": "{}",  # Workflows don't have metrics like transformations
                                # WFE-specific fields
                                "workflow_version": wf_version,
                                "wf_no": wf_no,
                                "wfe_no": wfe_no,
                            }
                            all_records.append(record)
                            total_executions += 1

                        # Update progress: increment by 1 for each completed workflow
                        _progress.update()

                    except Exception as e:
                        mo.output.append(
                            mo.callout(
                                mo.md(
                                    f"⚠️ Error fetching executions for workflow `{wf_ext_id}`: {type(e).__name__}: {str(e)}"
                                ),
                                kind="warn",
                            )
                        )
                        # Still update progress even if this workflow failed
                        _progress.update()

                # Step 5: Write to JSONL file
                if total_executions > 0:
                    mo.output.append(mo.md(f"💾 Writing {total_executions:,} records to file..."))

                    _output_file = Path(export_wfe_filepath)
                    _output_file.parent.mkdir(parents=True, exist_ok=True)

                    with open(_output_file, "w") as f:
                        for record in all_records:
                            f.write(json.dumps(record) + "\n")

                    mo.output.append(
                        mo.callout(
                            mo.md(f"""
                        ✅ **Export Completed Successfully!**

                        - **File:** `{_output_file.name}`
                        - **Location:** `{_output_file.parent}`
                        - **Workflows processed:** {len(workflows_to_process):,}
                        - **Total executions:** {total_executions:,}
                        - **Format:** Common JSONL (see COMMON_JSONL_FORMAT.md)
                        """),
                            kind="success",
                        )
                    )
                else:
                    mo.output.append(
                        mo.callout(
                            mo.md(
                                "⚠️ **Export Completed - No Executions Found**\n\nNo workflow executions were found for the selected workflows and limits."
                            ),
                            kind="warn",
                        )
                    )

        except Exception as e:
            mo.output.append(mo.callout(mo.md(f"❌ **Export failed:** {type(e).__name__}: {str(e)}"), kind="danger"))
            mo.output.append(mo.md(f"```\n{_traceback.format_exc()}\n```"))

    elif not _client_ready:
        _output_wfe_export_result = mo.callout(
            mo.md(f"⚠️ **{_status_text}** before you can export."),
            kind="warn",
        )
    else:
        # Show initial message when button not clicked
        _output_wfe_export_result = mo.md("⏳ Ready to export workflow executions.")

    # Display output (either initial message or appended content)
    if _client_ready and export_wfe_button.value:
        # Output was already appended via mo.output.append() calls above
        # Still need to display something to satisfy Marimo's output requirement
        mo.md("")  # Empty output, actual content was appended above
    else:
        _output_wfe_export_result
    return


@app.cell
def show_wfj_execution_dtypes(mo):
    mo.md(r"""
    wf_executions.dtypes
    """)
    return


@app.cell(column=1, hide_code=True)
def chapter3_loading_header(mo):
    mo.md("""
    ## Chapter 3: Data Loading

    Load exported TSJM data from JSONL files for analysis.
    """)
    return


@app.cell(hide_code=True)
def create_file_selector(exported_file_path, mo, output_folder_value):
    # File selection - use exported file path if available, otherwise use default
    _default_folder = output_folder_value or "/tmp"

    # File selection
    loader_browser = mo.ui.file_browser(
        initial_path=_default_folder,
        multiple=False,
        filetypes=[
            ".jsonl",
        ],
        selection_mode="file",
        label="JSONL File Path",
    )

    # shortcut to load exported file from Chapter 2
    loader_exported_file = mo.ui.run_button(
        label="Just load Exported JSONL File",
        disabled=exported_file_path is None,
        tooltip="Use the JSONL file exported in Chapter 2",
    )

    mo.vstack(
        [
            mo.md("### Select Data File"),
            loader_exported_file,
            loader_browser,
        ]
    )
    return loader_browser, loader_exported_file


@app.cell
def load_jsonl_data(
    Path,
    exported_file_path,
    loader_browser,
    loader_exported_file,
    mo,
    pl,
):
    """
    Load JSONL data in common format (supports TSJM, WFE, FNC).
    Handles both common format fields and legacy TSJM fields for backward compatibility.
    """

    # Load data when button is clicked
    jobs_df = None  # Will be pl.DataFrame or None
    load_status = ""
    _file_path = None

    # Support ~ expansion and resolve to absolute path
    if loader_exported_file.value:
        _file_path = Path(exported_file_path)
    elif loader_browser.value:
        _file_path = loader_browser.path()

    if _file_path and _file_path.exists():
        try:
            # Load JSONL with Polars (timestamps are stored as ms integers)
            # Load once and then cast columns to desired types
            # This allows us to handle files with different field sets (TSJM vs WFE vs FNC)
            jobs_df = pl.read_ndjson(_file_path, infer_schema_length=1000)
            _existing_columns = set(jobs_df.columns)

            # Define type mappings for columns that need casting
            # Format: {column_name: (target_type, cast_method)}
            # cast_method: 'cast' for direct cast, 'parse' for string-to-number parsing
            _type_mappings = {
                # Common format fields - Int64 timestamps
                "created_time": (pl.Int64, "cast"),
                "started_time": (pl.Int64, "cast"),
                "finished_time": (pl.Int64, "cast"),
                "last_seen_time": (pl.Int64, "cast"),
                # Legacy TSJM fields - Int64 timestamps
                "tsj_last_seen_time": (pl.Int64, "cast"),
                # WFE-specific fields - Int64 numbers
                "workflow_version": (pl.Int64, "cast"),
                "wf_no": (pl.Int64, "cast"),
                "wfe_no": (pl.Int64, "cast"),
            }

            # Cast columns that exist and need type conversion
            # Handle both numeric and string-encoded numeric values
            _cast_expressions = []
            for col_name, (target_type, cast_method) in _type_mappings.items():
                if col_name in _existing_columns:
                    _col = pl.col(col_name)
                    _current_dtype = jobs_df[col_name].dtype

                    # For string columns that need to be Int64, use str.to_integer()
                    # For numeric columns, use direct cast
                    if _current_dtype == pl.Utf8 and target_type == pl.Int64:
                        # String to Int64: use str.to_integer() with strict=False
                        # This handles numeric strings like "1" and converts invalid values to null
                        _cast_expressions.append(_col.str.to_integer(strict=False).alias(col_name))
                    else:
                        # Direct cast (handles numeric types and other conversions)
                        _cast_expressions.append(_col.cast(target_type, strict=False).alias(col_name))

            if _cast_expressions:
                jobs_df = jobs_df.with_columns(_cast_expressions)

            # Normalize to common format: use common fields if available, fall back to legacy
            # This ensures backward compatibility with old TSJM files
            if "started_time" not in jobs_df.columns and "tsj_started_time" in jobs_df.columns:
                # Legacy format: create common fields from legacy fields
                jobs_df = jobs_df.with_columns(
                    [
                        pl.lit("tsjm").alias("resource_type"),
                        pl.coalesce([pl.col("resource_id"), pl.col("ts_id")]).alias("resource_id"),
                        pl.coalesce([pl.col("resource_external_id"), pl.col("ts_external_id")]).alias(
                            "resource_external_id"
                        ),
                        pl.coalesce([pl.col("execution_id"), pl.col("tsj_job_id")]).alias("execution_id"),
                        pl.coalesce([pl.col("created_time"), pl.col("tsj_created_time")]).alias("created_time"),
                        pl.coalesce([pl.col("started_time"), pl.col("tsj_started_time")]).alias("started_time"),
                        pl.coalesce([pl.col("finished_time"), pl.col("tsj_finished_time")]).alias("finished_time"),
                        pl.coalesce([pl.col("status"), pl.col("tsj_status")]).alias("status"),
                        pl.coalesce([pl.col("error"), pl.col("tsj_error")]).alias("error"),
                        pl.coalesce([pl.col("metrics"), pl.col("tsjm_last_counts")]).alias("metrics"),
                        pl.coalesce([pl.col("last_seen_time"), pl.col("tsj_last_seen_time")]).alias("last_seen_time"),
                    ]
                )
            elif "resource_type" not in jobs_df.columns:
                # Assume TSJM if resource_type missing
                jobs_df = jobs_df.with_columns(
                    [
                        pl.lit("tsjm").alias("resource_type"),
                    ]
                )

            # Convert millisecond timestamps to Datetime
            # Use common format fields (with fallback to legacy)
            timestamp_cols = {
                "created_time": "created_time",
                "started_time": "started_time",
                "finished_time": "finished_time",
                "last_seen_time": "last_seen_time",
            }

            for common_col, legacy_col in [
                ("created_time", "tsj_created_time"),
                ("started_time", "tsj_started_time"),
                ("finished_time", "tsj_finished_time"),
                ("last_seen_time", "tsj_last_seen_time"),
            ]:
                if common_col in jobs_df.columns:
                    jobs_df = jobs_df.with_columns(
                        [
                            pl.when(pl.col(common_col).is_not_null())
                            .then(pl.from_epoch(pl.col(common_col), time_unit="ms"))
                            .otherwise(None)
                            .alias(f"{common_col}_dt"),
                        ]
                    )

            # Filter out running jobs for concurrency analysis
            # Use common status field
            if "status" in jobs_df.columns:
                jobs_df = jobs_df.filter(pl.col("status") != "Running")
            elif "tsj_status" in jobs_df.columns:
                jobs_df = jobs_df.filter(pl.col("tsj_status") != "Running")

            # Get resource type distribution
            resource_types = ""
            if "resource_type" in jobs_df.columns:
                _type_counts = jobs_df.group_by("resource_type").len().sort("resource_type")
                resource_types = f" ({', '.join([f'{row[0]}: {row[1]:,}' for row in _type_counts.iter_rows()])})"

            load_status = f"✅ Loaded {len(jobs_df):,} rows from `{_file_path.name}`{resource_types}"
        except Exception as e:
            import traceback as _traceback  # Local import, prefixed to avoid conflicts

            load_status = f"❌ Error loading file: {type(e).__name__}: {str(e)}\n```\n{_traceback.format_exc()}\n```"
    elif _file_path:
        load_status = f"⚠️ File not found: `{_file_path}`"
    else:
        load_status = "Click 'Load Data' or choose as file in browser to load the JSONL file."

    mo.md(load_status)
    return (jobs_df,)


@app.cell
def show_data_overview(jobs_df, mo, pl):
    # Display data overview (supports common format and legacy format)
    if jobs_df is not None and len(jobs_df) > 0:
        # Use common format fields with fallback to legacy
        started_col = (
            "started_time_dt"
            if "started_time_dt" in jobs_df.columns
            else ("tsj_started_time" if "tsj_started_time" in jobs_df.columns else None)
        )
        finished_col = (
            "finished_time_dt"
            if "finished_time_dt" in jobs_df.columns
            else ("tsj_finished_time" if "tsj_finished_time" in jobs_df.columns else None)
        )
        status_col = (
            "status" if "status" in jobs_df.columns else ("tsj_status" if "tsj_status" in jobs_df.columns else None)
        )
        resource_id_col = (
            "resource_external_id"
            if "resource_external_id" in jobs_df.columns
            else ("ts_external_id" if "ts_external_id" in jobs_df.columns else None)
        )

        # Build stats
        _stats = {
            "Total Executions": f"{len(jobs_df):,}",
        }

        # Resource type distribution
        if "resource_type" in jobs_df.columns:
            _type_counts = jobs_df.group_by("resource_type").len().sort("resource_type")
            _stats["Resource Types"] = ", ".join([f"{row[0]}: {row[1]:,}" for row in _type_counts.iter_rows()])

        # Resources count
        if resource_id_col:
            _stats["Resources"] = f"{jobs_df[resource_id_col].n_unique():,}"
        elif "ts_id" in jobs_df.columns:
            _stats["Transformations"] = f"{jobs_df['ts_id'].n_unique():,}"

        # Projects
        if "project" in jobs_df.columns:
            _stats["Projects"] = ", ".join(jobs_df["project"].unique().to_list())

        # Date range
        if started_col and finished_col:
            _stats["Date Range"] = f"{jobs_df[started_col].min()} to {jobs_df[finished_col].max()}"

        # Status distribution
        if status_col:
            _stats["Status Distribution"] = str(jobs_df.group_by(status_col).len().to_dict())

        _output = mo.vstack(
            [
                mo.md("### Data Overview"),
                mo.ui.table(
                    pl.DataFrame({"Metric": list(_stats.keys()), "Value": list(_stats.values())}),
                    selection=None,
                ),
                mo.md("### Sample Data"),
                mo.ui.table(jobs_df.head(10), selection=None),
                mo.callout(
                    mo.md(
                        "✅ **Data loaded successfully!** Scroll down to **Chapter 4: Concurrency Analysis** to explore job concurrency over time."
                    ),
                    kind="success",
                ),
            ]
        )
    else:
        _output = mo.md("⏳ No data loaded yet. Use the file path input above and click **Load Data** to begin.")

    _output
    return


@app.cell(hide_code=True)
def chapter4_concurrency_header(mo):
    mo.md("""
    ## Chapter 4: Concurrency Analysis

    Visualize concurrent transformation jobs over time.
    """)
    return


@app.cell
def create_concurrency_date_picker(datetime, jobs_df, mo):
    # Date range selection based on loaded data (supports common format and legacy)
    if jobs_df is not None and len(jobs_df) > 0:
        # Use common format fields with fallback to legacy
        _started_col = (
            "started_time_dt"
            if "started_time_dt" in jobs_df.columns
            else ("tsj_started_time" if "tsj_started_time" in jobs_df.columns else None)
        )
        _finished_col = (
            "finished_time_dt"
            if "finished_time_dt" in jobs_df.columns
            else ("tsj_finished_time" if "tsj_finished_time" in jobs_df.columns else None)
        )

        _min_date = jobs_df[_started_col].min() if _started_col else None
        _max_date = jobs_df[_finished_col].max() if _finished_col else None

        if _min_date is not None and _max_date is not None:
            min_date = _min_date.date()
            max_date = _max_date.date()
        else:
            min_date = datetime.date(2025, 1, 1)
            max_date = datetime.date(2025, 12, 31)
    else:
        min_date = datetime.date(2025, 1, 1)
        max_date = datetime.date(2025, 12, 31)

    date_range_picker = mo.ui.date_range(
        start=min_date,
        stop=max_date,
        value=(max_date - datetime.timedelta(days=7), max_date),
        label="Select Date Range",
    )

    mo.vstack(
        [
            mo.md("### Date Range Selection"),
            mo.md(f"Data available from `{min_date}` to `{max_date}`"),
            date_range_picker,
        ]
    )
    return (date_range_picker,)


@app.cell(hide_code=True)
def extract_concurrency_date_range(date_range_picker):
    # Get selected date range (use Ctrl+Shift+scroll on chart to zoom)
    if date_range_picker.value:
        selected_start, selected_end = date_range_picker.value
    else:
        selected_start = None
        selected_end = None

    # No output - this cell just extracts the selected range
    return selected_end, selected_start


@app.cell(hide_code=True)
def calculate_concurrency_events(jobs_df, pl):
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
                    "job_id": pl.Utf8,
                    "change": pl.Int64,
                    "concurrency": pl.Int64,
                }
            )

        # Determine which columns to use (common format with fallback to legacy)
        started_col = (
            "started_time_dt"
            if "started_time_dt" in df.columns
            else ("tsj_started_time" if "tsj_started_time" in df.columns else None)
        )
        finished_col = (
            "finished_time_dt"
            if "finished_time_dt" in df.columns
            else ("tsj_finished_time" if "tsj_finished_time" in df.columns else None)
        )
        job_id_col = (
            "execution_id" if "execution_id" in df.columns else ("tsj_job_id" if "tsj_job_id" in df.columns else None)
        )

        if not started_col or not finished_col or not job_id_col:
            # Return empty if required columns missing
            return pl.DataFrame(
                schema={
                    "time": pl.Datetime("ms"),
                    "job_id": pl.Utf8,
                    "change": pl.Int64,
                    "concurrency": pl.Int64,
                }
            )

        # Create start events (+1)
        start_events = df.filter(pl.col(started_col).is_not_null()).select(
            [
                pl.col(started_col).alias("time"),
                pl.col(job_id_col).cast(pl.Utf8).alias("job_id"),
                pl.lit(1).alias("change"),
            ]
        )

        # Create end events (-1)
        end_events = df.filter(pl.col(finished_col).is_not_null()).select(
            [
                pl.col(finished_col).alias("time"),
                pl.col(job_id_col).cast(pl.Utf8).alias("job_id"),
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


@app.cell
def create_peak_concurrency_table(events_df, mo, pl):
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
            page_size=5,  # Show 5 per page, top peaks first
        )

        _output = mo.vstack(
            [
                mo.md(f"### Peak Concurrency by Day ({len(all_daily_peaks)} days)"),
                top_peaks_table,
                mo.md("*Click a row to zoom to that day. Use table controls to sort/page.*"),
            ]
        )
    else:
        _output = mo.md("Load data to see peak concurrency days.")

    _output
    return (top_peaks_table,)


@app.cell(hide_code=True)
def create_concurrency_chart(
    alt,
    datetime,
    events_df,
    mo,
    pl,
    selected_end,
    selected_start,
    top_peaks_table,
):
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
            # Prepare data for Altair (native Polars support since Altair 5.0)
            # Cast datetime to microseconds for compatibility with marimo's altair_chart filtering
            chart_data = (
                filtered_events.group_by("time")
                .agg(pl.col("concurrency").last())
                .sort("time")
                .with_columns(pl.col("time").cast(pl.Datetime("us")).alias("time"))
            )

            # Create selections:
            # 1. Brush selection (drag to select time range)
            # empty=False: when nothing selected, condition returns False (unselected style)
            brush = alt.selection_interval(encodings=["x"], name="brush", empty=False)
            # 2. Click selection (click on a point)
            click_select = alt.selection_point(on="click", nearest=True, name="click", empty=False)
            # 3. Zoom selection: Ctrl+Shift+scroll to zoom x-axis only
            zoom = alt.selection_interval(
                bind="scales",
                encodings=["x"],
                zoom="wheel![event.ctrlKey && event.shiftKey]",
                translate=False,  # Disable drag-to-pan (conflicts with brush)
            )

            # Build chart with date range in title
            _chart_title = f"Concurrency: {_date_range_str} ({_days} days)"

            base = alt.Chart(chart_data).encode(
                x=alt.X("time:T", title="Time", axis=alt.Axis(format="%Y-%m-%d %H:%M")),
                y=alt.Y("concurrency:Q", title="Concurrent Jobs"),
            )

            line = base.mark_line(
                interpolate="step-after",
                color="#1f77b4",
            )

            # Combined selection: either brush OR click highlights the point
            combined_selection = brush | click_select

            points = (
                base.mark_point(
                    filled=True,
                    size=5,
                )
                .encode(
                    opacity=alt.condition(combined_selection, alt.value(1), alt.value(0.4)),
                    color=alt.condition(combined_selection, alt.value("red"), alt.value("#1f77b4")),
                    size=alt.condition(click_select, alt.value(30), alt.value(5)),
                )
                .add_params(brush)
                .add_params(click_select)
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
                mo.md("*Drag=select range, Click=select point, Ctrl+scroll=zoom*"),
            ]
        )
    else:
        chart_element = None
        _output = mo.vstack(
            [
                mo.md("### Concurrency Chart"),
                mo.callout(mo.md("No data to display. Load data and select a date range."), kind="warn"),
            ]
        )

    _output
    return (chart_element,)


@app.cell(hide_code=True)
def show_active_jobs_details(chart_element, datetime, jobs_df, mo, pl):
    # Show active transformations and jobs when chart point is selected
    _default_msg = mo.md(
        "🖱️ **Click** on a point or **drag** to select a time range to see active transformations and jobs."
    )

    if chart_element is None:
        _output = mo.vstack([_default_msg, mo.md("⏳ Chart not loaded yet.")])
    elif jobs_df is None:
        _output = mo.vstack([_default_msg, mo.md("⏳ Data not loaded yet.")])
    else:
        _concurrency_selection = chart_element.value

        # Debug: Uncomment to see selection details
        # _debug = mo.callout(
        #     mo.md(f"""
        # **Debug Selection Info:**
        # - `type(_concurrency_selection)`: `{type(_concurrency_selection).__name__}`
        # - `_concurrency_selection`: `{repr(_concurrency_selection)[:500]}`
        # - `len(_concurrency_selection)`: `{len(_concurrency_selection) if hasattr(_concurrency_selection, '__len__') else 'N/A'}`
        #     """),
        #     kind="info",
        # )
        _debug = None  # Set to debug callout above to enable

        # Check if we have a selection
        if _concurrency_selection is None or (
            hasattr(_concurrency_selection, "__len__") and len(_concurrency_selection) == 0
        ):
            _output = mo.vstack([_default_msg] + ([_debug] if _debug else []))
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
                    [_default_msg]
                    + ([_debug] if _debug else [])
                    + [
                        mo.callout(
                            mo.md(f"⚠️ Could not parse selection. Raw: `{repr(_concurrency_selection)[:200]}`"), kind="warn"
                        )
                    ]
                )
            else:
                # Get time from first and last selected points
                _first_time = _selected_rows[0].get("time")
                _last_time = _selected_rows[-1].get("time") if len(_selected_rows) > 1 else _first_time

                if not _first_time:
                    _output = mo.vstack(
                        [_default_msg]
                        + ([_debug] if _debug else [])
                        + [
                            mo.callout(
                                mo.md(f"⚠️ No 'time' field in selection. Keys: `{list(_selected_rows[0].keys())}`"),
                                kind="warn",
                            )
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
                        + ([_debug] if _debug else [])
                    )

    _output
    return


@app.cell(hide_code=True)
def chapter5_metrics_header(mo):
    mo.md("""
    ## Chapter 5: TSJM Metrics Aggregation

    Analyze Transformation Job Metrics (TSJM) by unpacking nested JSON data.
    """)
    return


@app.cell(hide_code=True)
def extract_available_tsjm_metrics(jobs_df, json, mo, pl):
    # Extract available TSJM metrics (Transformation Job Metrics)
    # Filters for TSJM resource type only and uses common format fields
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
        # Filter for TSJM resource type only
        # Use common format fields with fallback to legacy fields
        _metrics_col = "metrics" if "metrics" in jobs_df.columns else "tsjm_last_counts"
        _created_time_col = (
            "created_time_dt"
            if "created_time_dt" in jobs_df.columns
            else ("tsj_created_time" if "tsj_created_time" in jobs_df.columns else None)
        )

        # Filter for TSJM only
        _tsjm_df = jobs_df
        if "resource_type" in jobs_df.columns:
            _tsjm_df = jobs_df.filter(pl.col("resource_type") == "tsjm")

        # Check for TSJM data and required columns
        if len(_tsjm_df) == 0:
            _output = mo.md("⚠️ No TSJM data found. This section only supports Transformation Job Metrics (TSJM).")
        elif _metrics_col not in _tsjm_df.columns:
            _output = mo.md("⚠️ No metrics column found in TSJM data.")
        elif _created_time_col is None:
            _output = mo.md("⚠️ No created_time column found in TSJM data.")
        else:
            # Find the 2nd most recent day (latest might be incomplete)
            # Group by day and find unique days
            days_df = (
                _tsjm_df.filter(pl.col(_metrics_col).is_not_null() & (pl.col(_metrics_col) != "{}"))
                .with_columns(pl.col(_created_time_col).dt.date().alias("day"))
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
                sample_day_data = _tsjm_df.filter(
                    (pl.col(_created_time_col).dt.date() == metrics_sample_day)
                    & pl.col(_metrics_col).is_not_null()
                    & (pl.col(_metrics_col) != "{}")
                )

                # Collect ALL unique metrics from this day
                all_metrics_set: set[str] = set()
                _parse_errors: list[dict] = []  # Track problematic rows
                for _idx, row in enumerate(sample_day_data.iter_rows(named=True)):
                    json_str = row.get(_metrics_col)
                    if json_str:
                        try:
                            data = json.loads(json_str)
                            all_metrics_set.update(data.keys())
                        except (json.JSONDecodeError, TypeError) as e:
                            # Use common format fields with fallback to legacy
                            _resource_ext_id = row.get("resource_external_id") or row.get("ts_external_id", "unknown")
                            _execution_id = row.get("execution_id") or row.get("tsj_job_id", "unknown")
                            _parse_errors.append(
                                {
                                    "resource_external_id": _resource_ext_id,
                                    "execution_id": _execution_id,
                                    "error": str(e),
                                    "json_preview": json_str[:100] + "..." if len(json_str) > 100 else json_str,
                                }
                            )

                # Sort metrics alphabetically (case-insensitive) for consistent display
                available_metrics = sorted(all_metrics_set, key=str.lower)

                _output_parts = [
                    mo.md("### Available TSJM Metrics"),
                    mo.md(
                        f"Found **{len(available_metrics)}** unique metrics from **{metrics_sample_day}** ({len(sample_day_data):,} TSJM jobs):"
                    ),
                    mo.md(
                        ", ".join(f"`{m}`" for m in available_metrics[:30]) + ("..." if len(available_metrics) > 30 else "")
                    ),
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
                _output = mo.md("⚠️ No TSJM data with metrics found.")
    else:
        _output = mo.md("Load data to see available TSJM metrics.")

    _output
    return available_metrics, extract_metric


@app.cell(hide_code=True)
def create_metric_selector(available_metrics: list[str], mo):
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
        _output = mo.md("No metrics available.")

    _output
    return (metric_selector,)


@app.cell(hide_code=True)
def display_selected_metrics(metric_selector, mo):
    # Display selected metrics as a numbered list (alphabetically sorted)
    if metric_selector is not None and metric_selector.value:
        _sorted_metrics = sorted(metric_selector.value, key=str.lower)
        _metrics_list = "\n".join(f"{i + 1}. `{m}`" for i, m in enumerate(_sorted_metrics))
        _output = mo.md(f"**Selected ({len(_sorted_metrics)}):**\n\n{_metrics_list}")
    else:
        _output = mo.md("*No metrics selected*")
    _output
    return


@app.cell(hide_code=True)
def create_metrics_date_form(datetime, jobs_df, mo):
    # Grafana-style date range selector for metrics (with form to prevent immediate updates)
    # Get data range from jobs_df
    if jobs_df is not None and len(jobs_df) > 0:
        _data_min = jobs_df.select("tsj_finished_time").min().item().date()
        _data_max = jobs_df.select("tsj_finished_time").max().item().date()
        _data_range_info = f"Data: `{_data_min}` → `{_data_max}`"
    else:
        _data_min = datetime.date.today() - datetime.timedelta(days=30)
        _data_max = datetime.date.today()
        _data_range_info = "No data loaded"

    # Wrap all date controls in a form to require explicit submit
    metrics_date_form = mo.ui.batch(
        mo.md("""
    {mode}
    ---
    - **Relative:** Last {number} {unit}
    - **Custom:** {start} → {end}
        """),
        {
            "mode": mo.ui.radio(
                options={"relative": "📅 Relative", "custom": "🎯 Custom", "all": "🌐 All Data"},
                value="relative",
            ),
            "number": mo.ui.slider(start=1, stop=30, value=7, step=1, show_value=True),
            "unit": mo.ui.dropdown(options=["days", "weeks", "months"], value="days"),
            "start": mo.ui.date(value=_data_max - datetime.timedelta(days=30)),
            "end": mo.ui.date(value=_data_max),
        },
    ).form(submit_button_label="📊 Apply Date Range")

    _output = mo.vstack(
        [
            mo.md("### Date Range for Metrics"),
            metrics_date_form,
            mo.md(f"_{_data_range_info}_"),
        ]
    )
    _output
    return (metrics_date_form,)


@app.cell(hide_code=True)
def calculate_metrics_date_range(datetime, jobs_df, metrics_date_form):
    # Calculate effective date range based on form submission
    # Get data boundaries
    if jobs_df is not None and len(jobs_df) > 0:
        _data_min = jobs_df.select("tsj_finished_time").min().item().date()
        _data_max = jobs_df.select("tsj_finished_time").max().item().date()
    else:
        _data_min = datetime.date.today() - datetime.timedelta(days=365)
        _data_max = datetime.date.today()

    # Default values before form submission
    metrics_date_start = _data_max - datetime.timedelta(days=6)  # Last 7 days
    metrics_date_end = _data_max

    if metrics_date_form.value is not None:
        _mode = metrics_date_form.value["mode"]
        _unit = metrics_date_form.value["unit"]
        _number = metrics_date_form.value["number"]

        if _mode == "all":
            # Use full data range
            metrics_date_start = _data_min
            metrics_date_end = _data_max
        elif _mode == "custom":
            # Use custom dates
            metrics_date_start = metrics_date_form.value["start"]
            metrics_date_end = metrics_date_form.value["end"]
        else:
            # Relative mode - calculate from data max
            # "Last N days" means N days total including the end date
            if _unit == "days":
                _days = _number
            elif _unit == "weeks":
                _days = _number * 7
            elif _unit == "months":
                _days = _number * 30
            else:
                _days = 30

            # Subtract (days - 1) so "last 6 days" gives exactly 6 days
            metrics_date_start = _data_max - datetime.timedelta(days=_days - 1)
            metrics_date_end = _data_max
    return metrics_date_end, metrics_date_start


@app.cell(hide_code=True)
def create_daily_aggregation_chart(
    alt,
    extract_metric,
    jobs_df,
    metric_selector,
    metrics_date_end,
    metrics_date_start,
    mo,
    pl,
):
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

            # Apply date range filter
            if metrics_date_start and metrics_date_end:
                metric_values_df = _df_with_metrics.filter(
                    (pl.col("date") >= metrics_date_start) & (pl.col("date") <= metrics_date_end)
                )
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
                _metric_chart = (
                    alt.Chart(_chart_data)
                    .mark_line(point=True, strokeWidth=2)
                    .encode(
                        x=alt.X("date:T", title="Date"),
                        y=alt.Y("total:Q", title="Total Count"),
                        color=alt.Color(
                            "metric:N",
                            title="Metric",
                            scale=_agg_color_scale,
                            legend=alt.Legend(orient="bottom", columns=3),
                        ),
                        tooltip=["date:T", "metric:N", "total:Q"],
                        opacity=alt.condition(_metric_brush, alt.value(1), alt.value(0.5)),
                        strokeWidth=alt.condition(_metric_brush, alt.value(3), alt.value(1)),
                    )
                    .add_params(_metric_brush)
                    .properties(
                        width="container",
                        height=400,
                        title=f"Daily Metrics ({len(selected_metrics)}) - drag to select date range",
                    )
                )

                metric_chart_element = mo.ui.altair_chart(_metric_chart)

                # Show date range in header
                _date_range_str = (
                    f"{metrics_date_start} → {metrics_date_end}" if metrics_date_start and metrics_date_end else "All time"
                )
                _days_count = len(daily_metric_df)

                _output = mo.vstack(
                    [
                        mo.md(f"### Daily Aggregation: {_date_range_str} ({_days_count} days)"),
                        mo.md(
                            f"Comparing **{len(selected_metrics)}** metrics: {', '.join(f'`{m}`' for m in selected_metrics[:5])}{'...' if len(selected_metrics) > 5 else ''}"
                        ),
                        metric_chart_element,
                        mo.md("*Drag to select date range, scroll wheel to adjust*"),
                    ]
                )
            else:
                _output = mo.md(f"No data found for selected metrics in date range")
        else:
            _output = mo.md("Select at least one metric to see aggregation.")
    else:
        _output = mo.md("Select metrics to see aggregation.")

    _output
    return (
        daily_metric_df,
        metric_chart_element,
        metric_values_df,
        selected_metrics,
    )


@app.cell(hide_code=True)
def show_transformation_details(
    datetime,
    metric_chart_element,
    metric_values_df,
    mo,
    pl,
    selected_metrics: list[str],
):
    # Show transformation details when chart selection is made
    _default_msg = mo.md("🖱️ **Drag** to select date range in chart above")

    # Initialize return values (set in conditional branches)
    trafo_details_table = None
    trafo_trend_data = None
    trafo_trend_date_range = None

    if metric_chart_element is None or metric_values_df is None or not selected_metrics:
        _output = mo.md("⏳ Select metrics and wait for chart to load.")
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
                                    "**Trend Calculation:** The selected date range is split into two halves. "
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
    alt,
    mo,
    pl,
    selected_metrics: list[str],
    trafo_details_table,
    trafo_trend_data,
):
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
                _output = mo.md("⚠️ No valid transformation selected.")
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
    datetime,
    mo,
    pl,
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
def chapter6_export_header(mo):
    mo.md("""
    ## Chapter 6: Data Export

    Export analysis results to various formats.
    """)
    return


@app.cell(hide_code=True)
def create_export_options(daily_metric_df, events_df, jobs_df, mo):
    # Export options
    export_options = []

    if jobs_df is not None and len(jobs_df) > 0:
        export_options.append("jobs_data")
    if events_df is not None and len(events_df) > 0:
        export_options.append("events_data")
    if daily_metric_df is not None and len(daily_metric_df) > 0:
        export_options.append("daily_metrics")

    if export_options:
        export_format = mo.ui.dropdown(
            options=["CSV", "Parquet", "JSON"],
            value="CSV",
            label="Export Format",
        )

        export_dataset = mo.ui.dropdown(
            options=export_options,
            value=export_options[0],
            label="Dataset to Export",
        )

        _output = mo.vstack(
            [
                mo.md("### Export Options"),
                mo.hstack([export_dataset, export_format]),
            ]
        )
    else:
        export_format = None
        export_dataset = None
        _output = mo.md("Load and analyze data first to enable export.")

    _output
    return export_dataset, export_format


@app.cell(hide_code=True)
def generate_download(
    daily_metric_df,
    events_df,
    export_dataset,
    export_format,
    jobs_df,
    mo,
):
    # Generate download
    if export_dataset and export_format:
        dataset_map = {
            "jobs_data": jobs_df,
            "events_data": events_df,
            "daily_metrics": daily_metric_df,
        }

        df_to_export = dataset_map.get(export_dataset.value)

        if df_to_export is not None and len(df_to_export) > 0:
            format_val = export_format.value

            if format_val == "CSV":
                export_data = df_to_export.write_csv()
                filename = f"{export_dataset.value}.csv"
                mime = "text/csv"
            elif format_val == "Parquet":
                import io

                buffer = io.BytesIO()
                df_to_export.write_parquet(buffer)
                export_data = buffer.getvalue()
                filename = f"{export_dataset.value}.parquet"
                mime = "application/octet-stream"
            else:  # JSON
                export_data = df_to_export.write_json()
                filename = f"{export_dataset.value}.json"
                mime = "application/json"

            _output = mo.vstack(
                [
                    mo.md(f"### Download `{export_dataset.value}` as {format_val}"),
                    mo.download(
                        data=export_data,
                        filename=filename,
                        mimetype=mime,
                        label=f"Download {filename}",
                    ),
                ]
            )
        else:
            _output = mo.md("Selected dataset is empty.")
    else:
        _output = mo.md("Configure export options above.")

    _output
    return


@app.cell
def _():
    return


@app.cell(column=2, hide_code=True)
def chapter7_tests_header(mo):
    mo.md("""
    ## Chapter 7: Tests

    This chapter contains pytest-compatible test functions for validating the notebook's functionality.

    **Run tests from the command line:**
    ```bash
    pytest streamlit-trafo-concurrency/src/jupyter/marimo-tsjm-analysis.py -v
    ```

    These tests verify:
    - Configuration loading
    - Data processing functions
    - Export utilities
    - Concurrency calculations
    - Metrics aggregation
    """)
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
def test_output_folder_is_valid_path(Path, output_folder_value):
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
def test_jsonl_file_path_exists(loader_browser):
    # Test that JSONL file browser UI component is defined
    assert loader_browser is not None
    assert hasattr(loader_browser, "value")
    assert hasattr(loader_browser, "path")
    return


@app.cell
def test_concurrency_calculation_logic(datetime, pl):
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
def test_peak_concurrency_detection(datetime, pl):
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
def test_json_metric_extraction(pl):
    # Test extracting metrics from JSON strings (common format field)
    # Tests both common format 'metrics' field and legacy 'tsjm_last_counts' fallback
    _sample_df = pl.DataFrame(
        {
            "resource_type": ["tsjm", "tsjm", "tsjm", "tsjm"],
            "metrics": [
                '{"instances.upsertedNoop": 100, "instances.created": 5}',
                '{"instances.upsertedNoop": 200, "instances.created": 10}',
                "{}",
                None,
            ],
            # Legacy field for backward compatibility test
            "tsjm_last_counts": [
                '{"instances.upsertedNoop": 100, "instances.created": 5}',
                '{"instances.upsertedNoop": 200, "instances.created": 10}',
                "{}",
                None,
            ],
        }
    )
    # Test common format 'metrics' field (preferred)
    _result = _sample_df.with_columns(
        pl.col("metrics")
        .str.json_path_match("$.['instances.upsertedNoop']")
        .cast(pl.Int64, strict=False)
        .alias("upserted_noop")
    )
    _values = _result["upserted_noop"].to_list()
    assert _values[0] == 100, f"Expected 100, got {_values[0]}"
    assert _values[1] == 200, f"Expected 200, got {_values[1]}"
    assert _values[2] is None, f"Expected None for empty JSON, got {_values[2]}"
    
    # Test legacy field fallback (backward compatibility)
    _legacy_result = _sample_df.with_columns(
        pl.col("tsjm_last_counts")
        .str.json_path_match("$.['instances.upsertedNoop']")
        .cast(pl.Int64, strict=False)
        .alias("upserted_noop_legacy")
    )
    _legacy_values = _legacy_result["upserted_noop_legacy"].to_list()
    assert _legacy_values[0] == 100, f"Expected 100 from legacy field, got {_legacy_values[0]}"
    return


@app.cell
def test_daily_aggregation(datetime, pl):
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
def test_timestamp_conversion_with_nulls(pl):
    # Test loading and converting epoch timestamps with null handling
    # Tests both common format fields and legacy TSJM fields (backward compatibility)
    # Simulates the data loading logic from Chapter 3

    # Test 1: Common format fields (preferred)
    _common_format_data = pl.DataFrame(
        {
            "resource_type": ["tsjm", "tsjm", "tsjm", "tsjm"],
            "execution_id": ["1", "2", "3", "4"],
            "status": ["Completed", "Completed", "Failed", "Running"],
            # Common format timestamps (epoch ms)
            "created_time": [1700000000000, 1700000100000, 1700000200000, 1700000300000],
            "started_time": [1700000010000, 1700000110000, None, 1700000310000],
            "finished_time": [1700000050000, 1700000150000, None, None],
            "last_seen_time": [1700000050000, None, None, 1700000350000],
        }
    )

    # Apply conversion logic (creates *_dt columns)
    _common_converted = _common_format_data.with_columns(
        [
            pl.when(pl.col("created_time").is_not_null())
            .then(pl.from_epoch(pl.col("created_time"), time_unit="ms"))
            .otherwise(None)
            .alias("created_time_dt"),
            pl.when(pl.col("started_time").is_not_null())
            .then(pl.from_epoch(pl.col("started_time"), time_unit="ms"))
            .otherwise(None)
            .alias("started_time_dt"),
            pl.when(pl.col("finished_time").is_not_null())
            .then(pl.from_epoch(pl.col("finished_time"), time_unit="ms"))
            .otherwise(None)
            .alias("finished_time_dt"),
            pl.when(pl.col("last_seen_time").is_not_null())
            .then(pl.from_epoch(pl.col("last_seen_time"), time_unit="ms"))
            .otherwise(None)
            .alias("last_seen_time_dt"),
        ]
    )

    # Verify common format conversion
    assert _common_converted["created_time_dt"].null_count() == 0, "created_time_dt should have no nulls"
    assert _common_converted["created_time_dt"].dtype == pl.Datetime("ms"), "created_time_dt should be Datetime"
    assert _common_converted["started_time_dt"].null_count() == 1, "started_time_dt should have 1 null"
    assert _common_converted["finished_time_dt"].null_count() == 2, "finished_time_dt should have 2 nulls"
    assert _common_converted["last_seen_time_dt"].null_count() == 2, "last_seen_time_dt should have 2 nulls"

    # Test 2: Legacy TSJM fields (backward compatibility)
    _legacy_data = pl.DataFrame(
        {
            "tsj_job_id": ["1", "2", "3", "4"],
            "tsj_status": ["Completed", "Completed", "Failed", "Running"],
            "tsj_created_time": [1700000000000, 1700000100000, 1700000200000, 1700000300000],
            "tsj_started_time": [1700000010000, 1700000110000, None, 1700000310000],
            "tsj_finished_time": [1700000050000, 1700000150000, None, None],
            "tsj_last_seen_time": [1700000050000, None, None, 1700000350000],
        }
    )

    _legacy_converted = _legacy_data.with_columns(
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

    # Verify legacy format conversion
    assert _legacy_converted["tsj_created_time"].null_count() == 0, "tsj_created_time should have no nulls"
    assert _legacy_converted["tsj_created_time"].dtype == pl.Datetime("ms"), "tsj_created_time should be Datetime"
    assert _legacy_converted["tsj_started_time"].null_count() == 1, "tsj_started_time should have 1 null"
    assert _legacy_converted["tsj_finished_time"].null_count() == 2, "tsj_finished_time should have 2 nulls"
    assert _legacy_converted["tsj_last_seen_time"].null_count() == 2, "tsj_last_seen_time should have 2 nulls"

    # Verify datetime values (spot check)
    _first_created_common = _common_converted["created_time_dt"][0]
    assert _first_created_common.year == 2023, f"Expected year 2023, got {_first_created_common.year}"
    
    _first_created_legacy = _legacy_converted["tsj_created_time"][0]
    assert _first_created_legacy.year == 2023, f"Expected year 2023, got {_first_created_legacy.year}"
    return


if __name__ == "__main__":
    app.run()
