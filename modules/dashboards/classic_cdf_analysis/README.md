# Classic CDF Analysis

## Overview

A Streamlit app for **classic CDF model analysis** across **assets**, **time series**, **events**, **sequences**, and **files**. Supports **auto** and **custom** analysis modes with a shared dataset section. Designed to run inside CDF via Stlite/Pyodide, or locally with a `.env` file.

---

## Module Components

```
classic_cdf_analysis/
├── data_sets/
│   └── classic_cdf_analysis_apps.DataSet.yaml   # Dataset for Streamlit app
├── streamlit/
│   ├── classic_cdf_analysis_dashboard/
│   │   ├── app.py                               # Main application — UI, session state, CDF client setup
│   │   ├── analysis.py                          # CDF aggregate/list API calls and analysis logic
│   │   ├── key_selection.py                     # Filter key selection heuristics for analysis
│   │   ├── requirements.txt
│   └── classic_cdf_analysis_dashboard.Streamlit.yaml
├── module.toml
└── README.md
```
---

## Deployment

### Prerequisites

- A Cognite Toolkit project with a `cdf.toml` file at the project root.
- Valid authentication to your target CDF environment (API key or OAuth client credentials).

### Step 1: Enable external libraries

Edit your project’s `cdf.toml` and add:
```toml
[alpha_flags]
external-libraries = true

[library.cognite]
url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
checksum = "sha256:795a1d303af6994cff10656057238e7634ebbe1cac1a5962a5c654038a88b078"
```

**Replacing the default library**

New Toolkit projects often have a `[library.toolkit-data]` section pointing at `toolkit-data`. You **cannot** have both. To use this Deployment Pack you must **replace** that section:

| Replace this                         | With this           |
|--------------------------------------|---------------------|
| `[library.toolkit-data]`             | `[library.cognite]` |
| `github.com/cognitedata/toolkit-data/...` | `github.com/cognitedata/library/...` (as in the URL above) |

Delete or comment out the entire `[library.toolkit-data]` block and keep only the `[library.cognite]` block shown above.

**Checksum warning**

When you run `cdf modules add` or `cdf build`, you may see:

```text
WARNING [HIGH]: The provided checksum sha256:... does not match downloaded file hash sha256:...
Please verify the checksum with the source and update cdf.toml if needed.
```

This happens when the library release has been updated and the checksum in this README is outdated. The download still succeeds. To clear the warning: copy the **new** checksum from the warning message and set it in `cdf.toml` under `[library.cognite]`, for example:

```toml
checksum = "sha256:34d65c5ef7cded58878f838385dc3d39ae261bdd3426bfbd8f55b279ba4c40ed"
```

(Use the value from your actual warning.)

### Step 2: Add the module

**First try:** Run from your project root:
```bash
cdf modules add .
```

This lists all available deployment packs without changing your existing modules. In the menu, choose **Dashboards**, then **Classic CDF Analysis**.

**If the module is not in the list:** Use init instead:

```bash
cdf modules init .
```

Then choose **Dashboards** → **Classic CDF Analysis**.

- **`cdf modules add`** — Adds the selected module(s) and does **not** overwrite modules you already have.
- **`cdf modules init`** — Replaces your current module set with a new selection. Commit or back up first if you rely on existing modules.

### Step 3: Verify folder structure

After adding the module, confirm your project contains:

```text
modules/
    └── dashboards/
        └── classic_cdf_analysis/
```

### Step 4: Build and deploy

From the project root:
```bash
cdf build
cdf deploy --dry-run
cdf deploy
```

- **`cdf build`** — Builds the module (e.g. Streamlit app bundle).
- **`cdf deploy --dry-run`** — Shows what would be deployed without applying changes.
- **`cdf deploy`** — Deploys to your CDF project.

### Where to find the app in CDF

After deployment, open your CDF project and go to:

**Industrial Tools** → **Custom Apps** → **Classic CDF Analysis**

Users need access to the CDF project and to the app (permissions depend on your CDF setup).

---

## App usage

### Opening the app

- **In CDF:** Go to **Industrial Tools** → **Custom Apps** → **Classic CDF Analysis**. The app uses the project and credentials of the current user; no extra configuration is needed.
- **Locally:** See [Running the app locally](#running-the-app-locally) below.

---

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



## Running the app locally

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure CDF credentials**

	Create a `.env` file in this directory:

	```env
	COGNITE_PROJECT=your-cdf-project
	COGNITE_BASE_URL=https://api.cognitedata.com
	```

	Then set **one** of the following authentication methods:

    **API key:**

    ```env
    COGNITE_API_KEY=your-api-key
    ```

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

3. **Start the app**

	```bash
	python -m streamlit run app.py
	```

	Open the URL shown in the terminal (typically `http://localhost:8501`).

**Configuration reference**

| Variable                | Required | Description |
|-------------------------|----------|-------------|
| `COGNITE_PROJECT`       | Yes      | CDF project name |
| `COGNITE_BASE_URL`      | No       | CDF cluster URL (default: `https://api.cognitedata.com`) |
| `COGNITE_API_KEY`       | *        | API key authentication |
| `CDF_TOKEN`             | *        | Bearer-token authentication |
| `COGNITE_TOKEN`         | *        | Alternative name for bearer token |
| `COGNITE_CLIENT_ID`     | *        | OAuth2 client ID |
| `COGNITE_CLIENT_SECRET` | *        | OAuth2 client secret |
| `COGNITE_TENANT_ID`     | No       | Azure AD tenant ID (default: `organizations`) |

\* Provide either `COGNITE_API_KEY` or `CDF_TOKEN` or both `COGNITE_CLIENT_ID` and `COGNITE_CLIENT_SECRET`.

When deployed to CDF, credentials are handled automatically — no configuration needed.

---

## Support

- [Cognite Documentation](https://docs.cognite.com)
- Slack: **#topic-deployment-packs**

