# Classic CDF Analysis

## Overview

The **Classic CDF Analysis** module provides a Streamlit app that analyzes **metadata field distribution** across CDF resources: Assets, Time series, Events, Sequences, and Files. Use it to:

- See project-wide and per-dataset resource counts.
- **Run analysis** — Pick one resource type and one metadata (or filter) key; get distinct values, counts, and related metadata keys. Results can be downloaded as `.txt`.
- **Deep analysis** — Select one or more resource types; the app discovers metadata keys, applies a configurable threshold, and produces a full report per resource type with values, counts, and related keys. Reports are downloadable as a single `.txt` file.

The app runs in CDF as a custom Streamlit app (Stlite/Pyodide) or locally with a `.env` file and the Cognite SDK.

---

## Module Components

```
classic_cdf_analysis/
├── data_sets/
│   └── classic_cdf_analysis_apps.DataSet.yaml   # Dataset for Streamlit app
├── streamlit/
│   ├── classic_cdf_analysis_dashboard/
│   │   ├── app.py                               # Streamlit entry point
│   │   ├── analysis.py                          # CDF aggregate/list API and analysis logic
│   │   ├── deep_analysis.py                     # Filter key selection for deep analysis
│   │   ├── build_cdf_json.py                    # Build CDF import JSON (for manual CDF deploy)
│   │   ├── Classic-Analysis-Complete-CDF-source.json
│   │   ├── requirements.txt
│   │   └── README.md
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

### All Datasets (summary)

At the top of the app, **All Datasets** shows project-wide counts for:

- Assets  
- Time series  
- Events  
- Sequences  
- Files  
- Transformations  
- Functions  
- Workflows  
- Raw tables  

These load automatically when you open the app. Use this to get a quick overview of resource volume in the project.

---

### Datasets (optional)

This section lets you **optionally** restrict all analyses below to specific datasets.

1. Click **Load datasets**. The app fetches the dataset list and starts loading resource counts per dataset.
2. A table appears with columns: **Select**, **Dataset**, **Assets**, **Timeseries**, **Events**, **Sequences**, **Files**.
3. Check **Select** for the datasets you want to use. Analyses (Run analysis and Deep analysis) will then use only resources in those datasets.
4. If you leave nothing selected (or clear selection), analyses use **all datasets** in the project.
5. Use **Clear selection** to reset and go back to “all datasets”.

You can enable **Show datasets with no resources (all counts 0)** to include empty datasets in the table. Resource counts load in batches; wait for numbers to appear before relying on them for filtering.

---

### Run analysis (single key)

Run analysis for **one resource type** and **one metadata or filter key**. You get distinct values, counts per value, and related metadata keys.

1. **Resource type** — Choose: Assets, Time series, Events, Sequences, or Files.
2. **Filter key** — Either:
   - Click **Load metadata keys**, then pick a key from the dropdown (keys show instance counts in parentheses), or  
   - Type the key name manually (e.g. for Time series: `"is step"`, `"is string"`, `"unit"`; for Files: `"type"`, `"labels"`, `"author"`, `"source"`).
3. Click **Run analysis**. Results appear below: each value, its count, and related metadata keys.
4. Use **Download .txt** to save the result as a text file.
5. Use **Clear** to remove the result and run another analysis.

If you have selected datasets in **Datasets (optional)**, only those datasets are used. Otherwise all datasets are used.

---

### Deep analysis (multi-resource, many keys)

Deep analysis runs across **one or more resource types**, discovers metadata keys, and produces a **full report per resource type** (values, counts, related keys). It can take a while on large projects.

1. **Resource types** — Check the types you want: Assets, Time series, Events, Sequences, Files (at least one).
2. **Instance count threshold (%)** — Default **60**. A metadata key is included if either:
   - It is among the **top 15** most frequent “sorting-like” keys (e.g. type, category, level, class), or  
   - Its instance count is **≥ this percentage** of the total resource count for that type.  
   Lower values → more keys and longer reports; higher values → fewer, more dominant keys.
3. Click **Run deep analysis**. The app runs aggregate counts, discovers keys, filters by the threshold, and analyzes each key. Progress can be seen in the browser console (F12 → Console, filter by `DEEP`).
4. When finished, a report section appears for each resource type. Use **Download report** to save all sections as one `.txt` file.
5. Use **Clear** to remove the results.

**Which keys are included**

- Only “sorting-like” keys (e.g. type, category, level, class, kind, group) are considered.  
- Identifier-like keys (name, id, external_id, uuid, etc.) and datetime-like keys are excluded.  

Dataset selection from **Datasets (optional)** applies: selected datasets restrict deep analysis to those datasets; no selection means all datasets.

---

### Running the app locally

To run the app on your machine against a CDF project:

1. **Install dependencies**

   ```bash
   pip install streamlit cognite-sdk pandas python-dotenv
   ```

2. **Configure credentials**

   In the app directory (e.g. `streamlit/classic_cdf_analysis_dashboard/`), create a `.env` file:

   ```env
   COGNITE_PROJECT=your-cdf-project
   COGNITE_BASE_URL=https://api.cognitedata.com
   ```

   Then use **one** of these:

   **API key:**

   ```env
   COGNITE_API_KEY=your-api-key
   ```

   **OAuth client credentials:**

   ```env
   COGNITE_CLIENT_ID=your-client-id
   COGNITE_CLIENT_SECRET=your-client-secret
   COGNITE_TENANT_ID=organizations
   ```

3. **Start the app**

   ```bash
   cd streamlit/classic_cdf_analysis_dashboard
   python -m streamlit run app.py
   ```

   Open the URL shown (typically `http://localhost:8501`).

**Configuration reference**

| Variable                 | Required | Description |
|--------------------------|----------|-------------|
| `COGNITE_PROJECT`        | Yes      | CDF project name |
| `COGNITE_BASE_URL`       | No       | CDF cluster URL (default: `https://api.cognitedata.com`) |
| `COGNITE_API_KEY`        | *        | API key authentication |
| `COGNITE_CLIENT_ID`      | *        | OAuth2 client ID |
| `COGNITE_CLIENT_SECRET`  | *        | OAuth2 client secret |
| `COGNITE_TENANT_ID`      | No       | Azure AD tenant ID (default: `organizations`) |

\* Use either `COGNITE_API_KEY` or both `COGNITE_CLIENT_ID` and `COGNITE_CLIENT_SECRET`. When the app runs inside CDF, credentials are provided by the environment; you do not need to set these.

---

## Support

- [Cognite Documentation](https://docs.cognite.com)
- Slack: **#topic-deployment-packs**
