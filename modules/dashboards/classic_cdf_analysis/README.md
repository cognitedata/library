# Classic CDF Analysis — Streamlit

A Streamlit app for **classic CDF model analysis** across **assets**, **time series**, **events**, **sequences**, and **files**. Supports **auto** and **custom** analysis modes with a shared dataset section. Designed to run inside CDF via Stlite/Pyodide, or locally with a `.env` file.

## Features

### All Datasets summary

Project-wide resource counts (assets, time series, events, sequences, files, transformations, functions, workflows, raw tables) and unique metadata property key counts displayed on load.

### Datasets (optional)

Load the dataset catalogue, view per-dataset counts, and optionally restrict all analysis to selected datasets.

### Analysis

Select one or more resource types (Assets, Time series, Events, Sequences, Files) and run analysis in one of two modes:

- **Auto mode** — the algorithm selects metadata keys based on a configurable instance-count threshold.
- **Custom mode** — load all available metadata keys per resource type and pick manually.

Results are displayed in-browser and can be downloaded as a text report (filename includes `auto` or `custom` mode label).

#### Instance count threshold (Auto mode)

The **Instance count threshold (%)** controls which metadata keys are included in auto mode. For each resource type the app fetches the total resource count and the per-key instance counts. A metadata key is included if it meets **either** of these conditions:

1. **Top 15** — the key is among the 15 most frequent eligible metadata keys (regardless of threshold), or
2. **Meets threshold** — the key's instance count is ≥ the threshold percentage of the total resource count.

The default is **60 %**. Lowering it includes more keys (longer report, more API calls). Raising it restricts the report to only the most prevalent keys.

Only "sorting-like" keys (containing terms like *type*, *category*, *level*, *class*, etc.) are considered. Keys that look like identifiers (containing *name*, *id*, *uuid*, etc.) or datetime fields are automatically excluded.

Progress messages are printed to the browser console during processing. Open the browser dev tools (F12 → Console) and filter on `ANALYSIS` to follow along.

## Project layout

| File | Purpose |
|------|---------|
| `app.py` | Main application — UI, session state, CDF client setup |
| `analysis.py` | CDF aggregate/list API calls and analysis logic |
| `key_selection.py` | Filter key selection heuristics for analysis |
| `build_cdf_json.py` | Builds the CDF import JSON from the Python source files |
| `Classic-Analysis-Complete-CDF-source.json` | Generated — import this into CDF to deploy |
| `requirements.txt` | Python dependencies for local and CDF deployment |
| `README.md` | This file |

## Running locally

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure CDF credentials

Create a `.env` file in this directory:

```env
COGNITE_PROJECT=your-cdf-project
COGNITE_BASE_URL=https://api.cognitedata.com
```

Then set **one** of the following authentication methods:

**Bearer token** (simplest for local development):

```env
CDF_TOKEN=your-bearer-token
```

**OAuth client credentials:**

```env
COGNITE_CLIENT_ID=your-client-id
COGNITE_CLIENT_SECRET=your-client-secret
COGNITE_TENANT_ID=organizations
```

### 3. Start the app

```bash
python -m streamlit run app.py
```

Open the URL shown in the terminal (typically `http://localhost:8501`).

## Building for CDF

The app runs in CDF as a Stlite (Pyodide/WebAssembly) Streamlit app. The build step bundles all Python source files into a single JSON that CDF can import.

### 1. Build the JSON

From this directory:

```bash
python build_cdf_json.py
```

This produces `Classic-Analysis-Complete-CDF-source.json`.

The build script also applies a small patch to `get_client_and_project()` so that in CDF the pre-injected `CogniteClient` is detected automatically (no credentials needed).

### 2. Deploy to CDF

1. Open **CDF Console** → **Build solutions** → **Streamlit apps** (or the equivalent section in your CDF version).
2. Create a new app or update an existing one.
3. Import `Classic-Analysis-Complete-CDF-source.json` as the app source.
4. Save and publish.

The app will be available to users who have access to the CDF project.

## Configuration reference

| Variable | Required | Description |
|----------|----------|-------------|
| `COGNITE_PROJECT` | Yes | CDF project name |
| `COGNITE_BASE_URL` | No | CDF cluster URL (default: `https://api.cognitedata.com`) |
| `CDF_TOKEN` | * | Bearer-token authentication |
| `COGNITE_TOKEN` | * | Alternative name for bearer token |
| `COGNITE_CLIENT_ID` | * | OAuth2 client ID |
| `COGNITE_CLIENT_SECRET` | * | OAuth2 client secret |
| `COGNITE_TENANT_ID` | No | Azure AD tenant ID (default: `organizations`) |

\* Provide either `CDF_TOKEN` or both `COGNITE_CLIENT_ID` and `COGNITE_CLIENT_SECRET`.

When deployed to CDF, credentials are handled automatically — no configuration needed.
