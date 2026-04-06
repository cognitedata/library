# Pipeline Health Dashboard

A Streamlit app to monitor file-annotation pipeline health, runs and function logs for a selected extraction pipeline.

## Overview

- Shows live KPIs for the pipeline (files awaiting processing, total files processed, failure rate).
- Visualizes pipeline throughput and aggregated Launch/Finalize activity.
- Provides file-centric debugging with per-file metadata and the ability to inspect Finalize logs related to that file.
- Presents run-centric analysis with parsed run messages, run metrics and ability to download or inspect run logs and the files processed by a run.

The app discovers extraction pipelines, loads their configuration, queries data-model views (annotation state view and file view) and the extraction pipeline run history to present a consolidated monitoring UI.

## Tabs and Components

- **Overview**
  - Live pipeline KPIs.
  - Throughput charts (daily/hourly/weekly).

- **File Explorer**
  - File-centric table with selectable rows showing file metadata, status and annotation timestamps.
  - Select a file to open the **Function Log Viewer** which shows per-function tabs (Prepare / Launch / Finalize / Promote) and fetches logs for the specific function call. Logs are filtered for relevant lines and are downloadable.

- **Run History**
  - Run-centric KPIs and charts derived from parsed run messages.
  - Detailed run list with paginated view; expanders allow viewing function logs and the list of external file IDs processed in that run.

## Key libraries

- Streamlit for UI rendering
- pandas for DataFrame manipulation
- cognite-sdk (cognite-client) to read extraction pipeline configs, data model instances, functions logs and raw rows
- altair for charts

All Python dependencies for the module are listed in `requirements.txt`.

## Installation and Run

1. From the repository root, create/activate your virtual environment and install dependencies. Example with Poetry:

```bash
poetry install
poetry shell
```

2. Change to the module folder and run the Streamlit app:

```bash
cd modules/accelerators/contextualization/cdf_file_annotation/streamlit/file_annotation_dashboard_pipeline_health
streamlit run Pipeline_Health.py
```

## Environment Variables

- The app expects a `.env` file (or environment, if you're running inside CDF) with Cognite authentication and configuration values. You need to create one under the `file_annotation_dashboard_pipeline_health` folder.

## Pipeline Selection and Data Sources

- When the app loads, it lists available extraction pipelines for the project in a dropdown. Selecting a pipeline loads that extraction pipeline's configuration and uses it to determine which raw database, tables and other resources to query. The dashboard does not hardcode database/table names; it reads them from the selected extraction pipeline configuration.
