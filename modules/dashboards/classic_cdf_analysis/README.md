# Classic CDF Analysis

## Overview

The **Classic CDF Analysis** module provides a Streamlit app for metadata field distribution across CDF resources: Assets, Time series, Events, Sequences, and Files. It supports single-key analysis and deep analysis (multi-resource, multiple metadata keys) with optional dataset selection.

For app features and configuration, see [streamlit/classic_cdf_analysis_dashboard/README.md](streamlit/classic_cdf_analysis_dashboard/README.md).

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
│   │   ├── deep_analysis.py                      # Filter key selection for deep analysis
│   │   ├── build_cdf_json.py                     # Build CDF import JSON
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

- Cognite Toolkit project with `cdf.toml`
- Valid authentication to your CDF environment

### Step 1: Enable External Libraries

In `cdf.toml`:

```toml
[alpha_flags]
external-libraries = true

[library.cognite]
url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
checksum = "sha256:795a1d303af6994cff10656057238e7634ebbe1cac1a5962a5c654038a88b078"
```

Replace `[library.toolkit-data]` with `[library.cognite]` if needed (see other dashboard READMEs).

### Step 2: Add the Module

**First try:** Run:

```bash
cdf modules add .
```

This works on the first try and shows all available deployment packs. Select **Dashboards** → **Classic CDF Analysis**.

**If you don't see the module:** Use init instead:

```bash
cdf modules init .
```

Then select **Dashboards** → **Classic CDF Analysis**.

> **Note:** `cdf modules add` adds modules without overwriting existing ones. `cdf modules init` overwrites your current modules with a fresh selection—commit or back up first if you use init.

### Step 3: Verify and Deploy

Confirm:

```
modules/dashboards/classic_cdf_analysis/
```

Then:

```bash
cdf build
cdf deploy --dry-run
cdf deploy
```

The app will appear in CDF under **Industrial Tools** → **Custom Apps** as **Classic CDF Analysis**.

---

## Support

- [Cognite Documentation](https://docs.cognite.com)
- Slack **#topic-deployment-packs**
