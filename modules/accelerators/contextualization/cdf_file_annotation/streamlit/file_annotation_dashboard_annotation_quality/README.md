# Annotation Quality Dashboard

A Streamlit app to monitor and manage file annotation quality for a selected pipeline.

## Overview

- The dashboard helps teams inspect annotation coverage percentage, review annotation tags, promote potential annotations to actual annotations, inspect automatic patterns and manage manual patterns.

## Tabs and Components

- **Overall Quality Metrics**
  - Displays global KPIs and trend charts for annotation coverage percentage.
  - KPIs can be broken down by secondary scope, file resource type and tag resource type.

- **Per-File Analysis**
  - Shows a table of files with annotation counts, coverage percentage and tag details.
  - Includes row-level filters and per-file drilldown to view annotation statuses by scope, resource type or file.

- **Pattern Management**
  - Editable table for managing manual patterns and non-editable table for inspecting automatic patterns.
  - Allows users to add, modify and remove patterns.
  - Includes scope, entity type and tag resource type controls.

## Key libraries

- `Streamlit` for UI rendering.
- `pandas` for DataFrame manipulation.
- `cognite-sdk` to read and write raw rows (manual patterns) when configured.

All Python dependencies for the module are listed in `requirements.txt`. Install them in your virtual environment with Poetry or pip.

## Installation and Run

1. From the repository root, create/activate your virtual environment and install dependencies. Example with Poetry:

```powershell
poetry install
poetry shell
```

2. Run the app by changing to this folder and launching Streamlit:

```powershell
cd mpc-cdf-toolkit/modules/cdf_files_annotation_functions/cdf_refining_files_annotation_setup/streamlit/file_annotation_dashboard_annotation_quality
streamlit run Annotation_Quality.py
```

## Environment Variables

- The app expects a `.env` file (or environment, if you're running inside CDF) with Cognite authentication and configuration values. You need to create one under the `file_annotation_dashboard_annotation_quality` folder.

## Pipeline Selection and Data Sources

- When the app loads, it lists available extraction pipelines for the project in a dropdown. Selecting a pipeline loads that extraction pipeline's configuration and uses it to determine which raw database, tables and other resources to query. The dashboard does not hardcode database/table names; it reads them from the selected extraction pipeline configuration.
