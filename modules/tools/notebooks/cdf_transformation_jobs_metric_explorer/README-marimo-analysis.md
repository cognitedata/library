# TSJM Explorer Notebook

A web-based notebook for exporting, visualizing and drill-down into CDF Transformation-Job-Metrics (TSJM).

## Glossary

| Term | Meaning |
|------|---------|
| **TSJM** | Transformation-Job-Metrics - the data model used by this notebook |
| **Transformation** | A CDF transformation entity (also abbreviated as "trafo" in code) |
| **Job** | A single execution instance of a transformation (prefix: `tsj_` in data) |
| **Metrics** | Numeric values reported by jobs (e.g., rows created, updated, deleted) |
| **Concurrency** | Number of transformation jobs running simultaneously |

## First Run Checklist

1. [ ] **Install `uv`** - See [installation guide](https://docs.astral.sh/uv/getting-started/installation/)
2. [ ] **Create `.env` file** - Copy from Cognite Toolkit or create manually (see [Environment Configuration](#environment-configuration))
3. [ ] **Verify CDF access** - Ensure you have permissions to read transformations and jobs
4. [ ] **Run the notebook** - `uvx --python 3.13 marimo edit marimo-tsjm-analysis.py`
5. [ ] **Configure connection** - Fill in `.env` path and click "Connect" in Chapter 1
6. [ ] **Export data** - Set limits and click "Start Export" in Chapter 2
7. [ ] **Explore visualizations** - Load exported data in Chapter 3, then explore Chapters 4-5

## Features

- **TSJM Export**: Export transformation job metrics from CDF using parallel processing
- **Concurrency Analysis**: Visualize concurrent transformation jobs over time
- **Metrics Comparison**: Compare up to 10 metrics on color-coded line charts (auto-discovers all metric types)
- **Drill-down Analysis**: Step-by-step flow from daily aggregation → transformation table → daily trend → job details
- **Interactive Charts**: Drag to select range, Ctrl+Shift+scroll to zoom
- **Trend Indicators**: [experimental] Emoji-based trend visualization (📈📉➡️) per metric
- **Interactive UI**: Grafana-style date range selection, reactive tables, searchable metric dropdowns
- **Data Export**: Export analysis results to CSV, Parquet, or JSON
- **Inline Tests**: pytest-compatible tests for validation (`pytest marimo-tsjm-analysis.py`)

## Quick Start

### Prerequisite

1. For accessing CDF and downloading metrics you need to provide a `.env` file.
  - The syntax and options are fully compatible with the `.env` file used for Cognite Toolkit. So you can use an existing one, or create it manually. See chapter "Environment Configuration" for details.

2. To run this solutions in an isolated Python environment with minimum setup complexity, `uv` is recommended. Nothing else is required, but the `.env` file and the `marimo-tsjm-analysis.py` :)

Installation notes for `uv` you can find here:
- <https://docs.astral.sh/uv/getting-started/installation/#standalone-installer>
- Once installed, `uvx` is available too and used in the next step as a shortcut for `uv run`

An optional `pyproject.toml` (read the top comment lines!) is provided for reference, but not required for the following steps.

### Background information how this solution is built

**Info:**
  > The solution is built using Marimo a modern open-source Jupyter replacement. A notebook coming with a lot of [fresh ideas](https://docs.marimo.io/#highlights), see:
  > - [Marimo.io Homepage and docs](https://marimo.io/)
  > - YouTube [Marimo channel](https://www.youtube.com/@marimo-team).

  This notebook is a very promising playground to explore Marimo capabilties, along with Cursor AI development too. The code-learnings with Cursor are documented in `README-marimo-coding-best-practices.md`.

**Info:**
  > The abbreviation `tsjm` used in names and code stands for "Transformations > Jobs > Metrics" which is the source of data to explore. Right now transformation job concurrency and transformation job metrics (over-time and aggregation) can be explored

**Info:**
  > `uv` supports [PEP 723](https://peps.python.org/pep-0723/) inline script metadata to declare its dependencies ([documentation](https://docs.astral.sh/uv/guides/scripts/#declaring-script-dependencies)), and doesn't need the `pyproject.toml` which makes sharing and testing easier. See comment block on top of `marimo-tsjm-analysis.py`.

### Run with `uvx` (recommended)

```bash
# switch into this directory
cd marimo-transformation-jobs-metric-explorer

# Run with uvx (recommended) add --headless if needed

# Run in app-mode (only visuals no code editing possible)
uvx --python 3.13 marimo run --sandbox --no-token marimo-tsjm-analysis.py

# Or in `edit` mode providing developer support.
# To access and edit all code, which is organized in cells. Some cells might be folded ('hidden') and require a `CTRL-h` keyboard shortcut to toggle, this usually helps to hide bigger code blocks, and unfold them on demand.
uvx --python 3.13 marimo edit --sandbox --watch --no-token marimo-tsjm-analysis.py


# run static analysis for early feedback on variable dependencies, empty cells, etc.
uvx --python 3.13 marimo check marimo-tsjm-analysis.py

# run pytest suite, requires the pyproject.toml file to be available with dependecies in sync with the inlined ones. The test cases are only tested in Linux not Windows, ymmv!
uv run pytest marimo-tsjm-analysis.py -v --tb=short
```

- The `--sandbox` flag automatically installs dependencies defined in the script header.
- The `--watch` flag allows in `edit` mode to reload automatically on source-code changes (done for example by VSCode or Cursor), but allows saving changes from Marimo UI editing too. To avoid conflicts, it is recommended to switch off the 'Autosave enabled' toggle in user-settings.
- The `--no-token` allows with in a controlled network to access the notebook w/o a token.

### Run with installed marimo

```bash
# Install marimo first
pip install marimo

# Then run
marimo edit marimo-tsjm-analysis.py
```

### reusing CDF Toolkit authentication approach 

This approach tries to standardize the usage of `.env` file, required for providing parameters to connect to CDF.

For that it follows the Cognite Toolkit:

- same format of `.env` file
- code reuse to instantiate the `CogniteClient`, with support to all "login_flow" types supported
- up to running `cdf auth init/verify` with `uvx` like a standalone command-line tool

## Development with VS Code / Cursor

### Live Updates Setup

marimo automatically watches the source file for changes by default (no extra flags needed).

1. Install the marimo VS Code extension (optional, for syntax highlighting)
2. Run marimo in edit mode:
   ```bash
   uvx marimo edit --sandbox streamlit-trafo-concurrency/src/jupyter/marimo-tsjm-analysis.py
   ```
3. Open the provided URL in your browser (usually `http://localhost:2718`)
4. Edit the `.py` file in VS Code/Cursor - changes sync automatically to the browser

To disable file watching, use `--no-watch`:
```bash
uvx marimo edit --sandbox --no-watch marimo-tsjm-analysis.py
```

### Alternative: Edit in Browser

The marimo editor provides a full IDE experience in the browser with:
- Cell execution controls
- Variable explorer
- Output rendering
- AI code completion (if configured)

## Notebook Structure

### Chapter 1: Setup and Configuration
- `.env` file path configuration (cognite-toolkit format)
- Output folder configuration with validation (supports `~` expansion)
- **CogniteClient initialization** using `cognite-toolkit`'s `EnvironmentVariables`
- Supports all login flows: `interactive`, `client_credentials`, `device_code`, `token`
- **Project selector table** showing all available projects from token inspection
- Default project from `.env` used automatically (can be overridden by selection)

### Chapter 2: TSJM Export (Cognite API)
- **Requires:** Active CDF connection from Chapter 1
- Shows available transformation count from selected project
- Parallel export using `ThreadPoolExecutor`
- Configurable workers (default 4, increase to 8-12 for bulk)
- Transformation limit (default 10 for testing, set to 0 for all)
- Progress tracking with marimo's native progress bar

### Chapter 3: Data Loading
- Load JSONL files with Polars
- Automatic schema detection
- Data overview and statistics

### Chapter 4: Concurrency Analysis
- Interactive date range selection with **zoom & pan sliders**:
  - **Zoom slider** (10-100%): Narrows the visible time window
  - **Pan slider** (0-100%): Scrolls left/right within the selected range
  - **Reset button**: Returns to full view
- Step chart visualization with Altair (wrapped in `mo.ui.altair_chart`)
- **Chart interactions:**
  - **Drag** → Select time range (brush selection)
  - **Click** → Select single data point
- Top 5 peak concurrency days table
- Active jobs detail table (updates on chart selection)

### Chapter 5: Metrics Aggregation
- **Smart metric discovery:** Uses 2nd most recent day (latest might be incomplete) to find all unique metric names
- JSON metric unpacking from `tsjm_last_counts` column
- **Multi-metric comparison:** Select up to 10 metrics to compare on one chart
- **Grafana-style date range selector:**
  - **Relative:** Last N days/weeks/months
  - **Custom:** Specific start/end dates
  - **All Data:** Full date range
- **Daily Aggregation chart:** Color-coded line chart with brush selection
- **Transformations table:** Shows trend per metric with emoji indicators:
  - 📈 increase (>5%) · 📉 decrease (<-5%) · ➡️ stable (±5%)
  - Trend calculated by comparing **first half vs second half** of selected period (summed values)
- **Daily Trend drill-down:** Select a transformation to see its daily trend chart
- **Job Details drill-down:** Brush select on trend chart to see individual job data

### Chapter 6: Data Export
- Export to CSV, Parquet, or JSON
- Download buttons for analysis results

### Chapter 7: Tests (pytest)
- Inline pytest-compatible test functions
- Tests for each chapter's functionality
- Run from command line or within marimo

## Running Tests

The notebook includes pytest-compatible tests that validate core functionality.

### Using local pyproject.toml (recommended)

```bash
cd cdf_transformation_jobs_metric_explorer

# Run static analysis first (fast)
uvx --python 3.13 marimo check marimo-tsjm-analysis.py

# Run all tests
uv run pytest marimo-tsjm-analysis.py -v

# Run specific test
uv run pytest marimo-tsjm-analysis.py::test_concurrency_calculation_logic -v
```

### Using uvx with inline dependencies

If you don't have the local `pyproject.toml`, you can run tests by manually specifying dependencies:

```bash
uvx --with marimo --with "polars>=1.0.0" --with "altair>=5.0.0,<6.0.0" \
    --with "python-dotenv>=1.0.0" --with "cognite-sdk[pandas]>=7.0.0" \
    --with "pyarrow>=18.0.0" \
    pytest streamlit-trafo-concurrency/src/jupyter/marimo-tsjm-analysis.py -v
```

### In-notebook testing

Tests are also executed reactively within marimo during notebook editing when pytest is available.

See [marimo pytest documentation](https://docs.marimo.io/guides/testing/pytest/) for more details.

### Test Variable Naming Convention

In marimo test cells, prefix local variables with `_` (e.g., `_result`, `_df`) to prevent them from being exported to the marimo graph. This avoids "multiple definitions" errors when the same variable name is used in multiple test cells.

> **Note**: The `pyproject.toml` in the jupyter folder must be kept in sync with the inline
> script dependencies in `marimo-tsjm-analysis.py` (lines 1-10). Both define the same
> dependencies for different use cases.

## Environment Configuration (Cognite Toolkit compatible)

Uses **cognite-toolkit** `.env` format (compatible with `cdf auth init`).

Create a `.env` file (default location: `~/.env`)

### interactive creation and verification

```shell
uvx --from cognite-toolkit cdf auth init

WARNING: No .env file found in current or parent directory.
? Choose the provider (Who authenticates you?) (Use arrow keys)
» entra_id: Use Microsoft Entra ID to authenticate
auth0: Use Auth0 to authenticate
other: Use other IDP to authenticate
..
```

and then verify it  

```shell
uvx --from cognite-toolkit cdf auth verify

Installed 49 packages in 171ms
  WARNING: Overriding environment variables with values from .env file...
Checking basic project configuration...
OK
Checking projects that the service principal/application has access to...
# make sure to say no here, if you don't need/understand what it is aboutGroup 'cognite_toolkit_service_principal' does not exist in the CDF project.?
Do you want to create it? No
```

HINT:  

- running this in a folder with a `cdf.toml` requires to align the cdf-tk version, fixing to a matching version like `==0.6.98` in this example:
  - `uvx --from cognite-toolkit==0.6.98 cdf ..`

### Manual setup of `.env`

For details read documentation: [Authentication and authorization for Cognite Toolkit](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/auth)

```env
# Required
CDF_CLUSTER=westeurope-1
CDF_PROJECT=my-project

# Authentication
PROVIDER=entra_id
LOGIN_FLOW=interactive

# For interactive/device_code flow
IDP_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
IDP_CLIENT_ID=yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy

# For client_credentials flow (add these)
# IDP_CLIENT_SECRET=your-client-secret
# IDP_TOKEN_URL=https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token
```

| Variable           | Required        | Description                               |
| ------------------ | --------------- | ----------------------------------------- |
| `CDF_CLUSTER`      | ✅               | CDF cluster (e.g., `api`, `westeurope-1`) |
| `CDF_PROJECT`      | ✅               | Default CDF project                       |
| `PROVIDER`         | ✅               | Auth provider (`entra_id`, `auth0`, etc.) |
| `LOGIN_FLOW`       | ✅               | Auth flow (`interactive`, `client_credentials`, `device_code`, `token`) |
| `IDP_TENANT_ID`    | For entra_id    | Microsoft Entra ID tenant ID              |
| `IDP_CLIENT_ID`    | For interactive | Azure AD app client ID                    |
| `IDP_CLIENT_SECRET`| For client_creds| Service principal secret                  |

See [cognite-toolkit auth documentation](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/auth) for all options.

## JSONL Data Format

Expected schema for TSJM dumps:

```json
{
  "project": "string",
  "ts_no": 0,
  "ts_id": 12345,
  "ts_external_id": "cdf:transformation:name",
  "tsj_no": 0,
  "tsj_job_id": 67890,
  "tsj_created_time": 1700000000000,
  "tsj_started_time": 1700000000000,
  "tsj_finished_time": 1700000100000,
  "tsj_last_seen_time": 1700000100000,
  "tsj_error": null,
  "tsj_status": "Completed",
  "tsjm_last_counts": "{\"requests\":10,\"instances.upsertedNoop\":5}"
}
```

## Dependencies

Defined inline in the script (installed automatically with `--sandbox`):

- `marimo` - Reactive notebook framework
- `polars>=1.0.0` - Fast DataFrame library
- `altair>=5.0.0,<6.0.0` - Declarative visualization (pinned to v5 for marimo compatibility)
- `python-dotenv>=1.0.0` - Environment file loading
- `cognite-toolkit` - CDF toolkit (includes cognite-sdk with auth utilities)
- `pyarrow>=18.0.0` - Required for Polars I/O operations

> **Note**: `cognite-toolkit` is used for authentication, providing the same `.env` format
> as the `cdf` CLI tool. It includes `cognite-sdk` as a dependency.

> **Note**: Altair is pinned to `<6.0.0` because marimo's `mo.ui.altair_chart` wrapper
> currently expects Altair v5. Altair 5.0+ supports Polars DataFrames natively.

Progress bars are provided natively by marimo using `mo.status.progress_bar()`.

**For running tests:**
- `pytest` - Required for command-line test execution (not needed for in-notebook tests)

## Related Files

- `tsjm-exporter.ipynb` - Original Jupyter notebook for TSJM export
- `../app_cdf3.py` - Streamlit app for concurrency visualization

## Troubleshooting

### Import errors with uvx
Ensure you're using `--sandbox` flag to enable inline dependencies.

### OAuth authentication issues
1. Verify `.env` file path is correct and file exists
2. Check required variables: `CDF_CLUSTER`, `CDF_PROJECT`, `PROVIDER`, `LOGIN_FLOW`
3. For `interactive` flow: ensure `IDP_TENANT_ID` and `IDP_CLIENT_ID` are set
4. Click "Connect to CDF" button to trigger authentication
5. For interactive login, a browser window should open

### Large file performance
For files > 1M rows, consider:
- Using date range filters
- Increasing Polars streaming threshold
- Pre-filtering data before loading

