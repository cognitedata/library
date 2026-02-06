# 251126 Marimo TSJM Analysis Notebook

Start set for planning the marimo re-implementation of TSJM related steps. Follow the commits and READMEs to understand the changes.

## Overview

Create a marimo notebook (`marimo-tsjm-analysis.py`) with:

- `uvx run` support via inline dependencies (`--sandbox` mode)
- Polars as the data backend for efficient processing
- Altair for interactive visualizations
- Modular structure with markdown chapters for organization

## Files to Create

### 1. Main Notebook: `marimo-tsjm-analysis.py`

Structure with markdown chapters:

**Chapter 1: Setup and Configuration**

- Inline dependencies header (marimo sandbox format):
  ```python
  # /// script
  # requires-python = ">=3.11"
  # dependencies = [
  #     "marimo",
  #     "polars",
  #     "altair",
  #     "python-dotenv",
  #     "cognite-sdk",
  #     "rich",
  # ]
  # ///
  ```

- Configuration UI elements (customer selector, file path input)
- `.env` file loader for Cognite credentials

**Chapter 2: TSJM Export (Cognite API)**

- Port the parallel export logic from [tsjm-exporter.ipynb](streamlit-trafo-concurrency/src/jupyter/tsjm-exporter.ipynb):
  - `process_single_transformation()` function
  - `process_parallel_and_dump()` function with ThreadPoolExecutor
  - CogniteClient initialization with OAuthInteractive
- UI controls: customer dropdown, max_workers slider, jobs_per_transformation input
- Progress tracking using marimo's status elements

**Chapter 3: Data Loading**

- File browser/path input for JSONL selection
- Load JSONL into Polars LazyFrame for efficient processing
- Schema display and basic statistics
- Reusable `jobs_df` and `events_df` dataframes

**Chapter 4: Concurrency Analysis**

Port visualization from [app_cdf3.py](streamlit-trafo-concurrency/src/app_cdf3.py):

- Date range selector using `mo.ui.date_range`
- Calculate event data (start/end events with +1/-1 changes)
- Compute running concurrency via cumsum
- Interactive Altair step chart with click selection
- Top 5 peak concurrency days table (`mo.ui.table`)
- Active jobs detail table on chart click

**Chapter 5: Metrics Aggregation**

- Unpack nested JSON from `tsjm_last_counts` column using Polars `json.path_match`
- Group by day and aggregate `instances.upsertedNoop` metric
- Bar chart visualization of daily aggregates
- Filterable table of results

**Chapter 6: Data Export**

- Export filtered results to CSV/Parquet
- Download buttons for charts and data

### 2. Documentation: `README-marimo-analysis.md`

Contents:

- Purpose and features overview
- Installation and running instructions:
  ```bash
  # Run with uvx (recommended)
  uvx marimo edit --sandbox streamlit-trafo-concurrency/src/jupyter/marimo-tsjm-analysis.py
  
  # Or run as app
  uvx marimo run --sandbox streamlit-trafo-concurrency/src/jupyter/marimo-tsjm-analysis.py
  ```

- VS Code/Cursor development setup with live updates
- Data format requirements (JSONL schema)
- Chapter descriptions
- Environment file template

## Key Implementation Details

### Polars Data Processing

- Use `pl.scan_ndjson()` for lazy loading of large JSONL files
- Schema:
  ```python
  schema = {
      "project": pl.Utf8,
      "ts_external_id": pl.Utf8,
      "tsj_job_id": pl.Int64,
      "tsj_started_time": pl.Datetime,
      "tsj_finished_time": pl.Datetime,
      "tsj_status": pl.Utf8,
      "tsjm_last_counts": pl.Utf8,  # JSON string
  }
  ```


### Altair Concurrency Chart

- Use `alt.Chart().mark_line(interpolate='step-after')` for step visualization
- Add `selection_point()` for click interactivity
- Bind to marimo state for reactive updates

### Metrics JSON Unpacking (Polars)

```python
df.with_columns(
    pl.col("tsjm_last_counts")
    .str.json_path_match("$.['instances.upsertedNoop']")
    .cast(pl.Int64)
    .alias("instances_upserted_noop")
)
```

## Dependencies

```
marimo>=0.9.0
polars>=1.0.0
altair>=5.0.0
python-dotenv>=1.0.0
cognite-sdk>=7.0.0
rich>=13.0.0
```
