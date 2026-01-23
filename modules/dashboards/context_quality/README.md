# Contextualization Quality Dashboard

## Overview

The **Contextualization Quality Dashboard** module provides a comprehensive solution for measuring, monitoring, and visualizing the **contextualization quality** of your data in **Cognite Data Fusion (CDF)**. It consists of two main components:

1. **Contextualization Quality Metrics Function** (external_id: `context_quality_handler`) - Computes all quality metrics and saves them as a JSON file in CDF
2. **Streamlit Dashboard** (`context_quality_dashboard`) - Visualizes the pre-computed metrics with interactive gauges, charts, and tables

This module helps data engineers and operations teams understand how well their data is contextualized across five key dimensions:

- **Asset Hierarchy Quality** - Structural integrity of the asset tree
- **Equipment-Asset Relationships** - Quality of equipment-to-asset mappings
- **Time Series Contextualization** - How well time series are linked to assets
- **Maintenance Workflow Quality** - Quality of maintenance data from RMDM v1 (notifications, work orders, failure documentation)
- **File Annotation Quality** - Quality of P&ID diagram annotations linking files to assets/equipment

---

## Module Components

```
context_quality/
├── auth/                                    # Authentication configurations
├── data_sets/
│   └── context_quality_dashboard.DataSet.yaml  # Dataset for storing function code and files
├── functions/
│   ├── context_quality_handler/
│   │   ├── handler.py                       # Main Cognite Function orchestration
│   │   └── metrics/                         # Modular metric computation
│   │       ├── common.py                    # Shared utilities and data classes
│   │       ├── asset_hierarchy.py           # Asset hierarchy metrics
│   │       ├── equipment.py                 # Equipment-asset metrics
│   │       ├── timeseries.py                # Time series metrics
│   │       ├── maintenance.py               # Maintenance workflow metrics (RMDM v1)
│   │       ├── file_annotation.py           # File annotation metrics (CDM)
│   │       └── storage.py                   # File storage utilities
│   └── context_quality.Function.yaml        # Function configuration
├── streamlit/
│   ├── context_quality_dashboard/
│   │   ├── main.py                          # Streamlit dashboard entry point
│   │   ├── requirements.txt                 # Python dependencies
│   │   └── dashboards/                      # Modular dashboard components
│   │       ├── common.py                    # Shared UI components & color functions
│   │       ├── sidebar.py                   # Sidebar with metadata
│   │       ├── configuration.py             # Configuration & Run tab (NEW)
│   │       ├── asset_hierarchy.py           # Asset Hierarchy tab
│   │       ├── equipment.py                 # Equipment-Asset tab
│   │       ├── timeseries.py                # Time Series tab
│   │       ├── maintenance.py               # Maintenance Workflow tab (RMDM v1)
│   │       ├── file_annotation.py           # File Annotation tab (CDM)
│   │       └── ai_summary.py                # AI-powered insights generator
│   └── context_quality_dashboard.Streamlit.yaml  # Streamlit app configuration
├── module.toml                              # Module metadata
└── README.md                                # This file
```

---

## Deployment

### Prerequisites

Before you start, ensure you have:

- A Cognite Toolkit project set up locally
- Your project contains the standard `cdf.toml` file
- Valid authentication to your target CDF environment

### Step 1: Enable External Libraries

Edit your project's `cdf.toml` and add:

```toml
[alpha_flags]
external-libraries = true

[library.cognite]
url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
checksum = "sha256:795a1d303af6994cff10656057238e7634ebbe1cac1a5962a5c654038a88b078"
```

This allows the Toolkit to retrieve official library packages.

> **📝 Note: Replacing the Default Library**
>
> By default, a Cognite Toolkit project contains a `[library.toolkit-data]` section pointing to `https://github.com/cognitedata/toolkit-data/...`. This provides core modules like Quickstart, SourceSystem, Common, etc.
>
> **These two library sections cannot coexist.** To use this Deployment Pack, you must **replace** the `toolkit-data` section with `library.cognite`:
>
> | Replace This | With This |
> |--------------|-----------|
> | `[library.toolkit-data]` | `[library.cognite]` |
> | `github.com/cognitedata/toolkit-data/...` | `github.com/cognitedata/library/...` |
>
> The `library.cognite` package includes all Deployment Packs developed by the Value Delivery Accelerator team (RMDM, RCA agents, Context Quality Dashboard, etc.).

> **⚠️ Checksum Warning**
>
> When running `cdf modules add`, you may see a warning like:
>
> ```
> WARNING [HIGH]: The provided checksum sha256:... does not match downloaded file hash sha256:...
> Please verify the checksum with the source and update cdf.toml if needed.
> This may indicate that the package content has changed.
> ```
>
> **This is expected behavior.** The checksum in this documentation may be outdated because it gets updated with every release. The package will still download successfully despite the warning.
>
> **To resolve the warning:** Copy the new checksum value shown in the warning message and update your `cdf.toml` with it. For example, if the warning shows `sha256:da2b33d60c66700f...`, update your config to:
>
> ```toml
> [library.cognite]
> url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
> checksum = "sha256:da2b33d60c66700f..."
> ```

### Step 2 (Optional but Recommended): Enable Usage Tracking

To help improve the Deployment Pack:

```bash
cdf collect opt-in
```

### Step 3: Add the Module

Run:

```bash
cdf modules init .
```

> **⚠️ Disclaimer**: This command will overwrite existing modules. Commit changes before running, or use a fresh directory.

### Step 4: Select the Dashboards Package

From the menu, select:

```
Dashboards: Streamlit dashboards and visualization modules
```

Then select **Contextualization Quality Dashboard**.

### Step 5: Verify Folder Structure

After installation, your project should contain:

```
modules/
    └── dashboards/
        └── context_quality/
```

### Step 6: Deploy to CDF

Build and deploy:

```bash
cdf build
```

```bash
cdf deploy --dry-run
```

```bash
cdf deploy
```

> ⏱️ **Note: Function Deployment Time**
>
> After running `cdf deploy`, the Cognite Function (`context_quality_handler`) may take **2-5 minutes** to fully deploy. The Streamlit dashboard will be available immediately, but the function needs time to initialize before you can run it.
>
> If you see "Function not available" when clicking **Run Function** in the dashboard, wait a few minutes and try again. You can verify deployment status in CDF:
> - Navigate to **Data management** → **Build solutions** → **Functions**
> - Look for `context_quality_handler` and check its status

---

## Getting Started (Post-Deployment)

After deployment, the easiest way to get started is through the **dashboard itself**:

### Step 1: Open the Dashboard

1. Log in to [Cognite Data Fusion](https://fusion.cognite.com)
2. Navigate to **Industrial Tools** → **Custom Apps**
3. Click on **Contextualization Quality**

### Step 2: Configure Data Model Views

The dashboard opens to the **⚙️ Configure & Run** tab. Here you can:

1. **Configure views for each dashboard:**
   - 🌳 **Asset Hierarchy** — Configure the Asset view
   - 🔧 **Equipment-Asset** — Configure the Equipment view
   - ⏱️ **Time Series** — Configure the Time Series view
   - 🛠️ **Maintenance** — Configure Notification, MaintenanceOrder, FailureNotification views
   - 📄 **File Annotation** — Configure the Diagram Annotation view

2. **Use defaults or customize:**
   - Defaults are set for **Cognite Core Data Model (CDM)** and **RMDM v1**
   - Modify space, view external ID, and version for your custom data models
   - Disable dashboards you don't need (e.g., Maintenance if RMDM is not deployed)

### Step 3: Run the Metrics Function

1. Click **▶️ Run Function** to trigger the metrics computation
2. The dashboard will show the function status: **Running** → **Completed** → **Failed**
3. Click **🔄 Check Status** to refresh the status while running
4. Once completed, click **📊 View Dashboard** to see your metrics

> ⚠️ **Function Not Available?**
>
> If you see an error saying the function was not found:
> 1. **Wait 2-5 minutes** — The function may still be deploying
> 2. **Verify deployment** — Go to **Data management** → **Build solutions** → **Functions** and check that `context_quality_handler` exists and has status "Ready"
> 3. **Re-deploy if needed** — Run `cdf deploy` again from your project directory

### Step 4: View Your Metrics

After the function completes, navigate to the other tabs to see your contextualization quality metrics:

- **🌳 Asset Hierarchy** — Hierarchy completion, orphan detection, depth/breadth analysis
- **🔧 Equipment-Asset** — Equipment association rates, metadata completeness
- **⏱️ Time Series** — TS-to-asset contextualization, unit completeness, freshness
- **🛠️ Maintenance** — Notification/work order linkage, failure documentation
- **📄 File Annotation** — P&ID annotation confidence, status distribution

---

## Configuration

### Dashboard-Based Configuration (Recommended)

The easiest way to configure data model views is through the **⚙️ Configure & Run** tab in the dashboard. This allows you to:

- Configure views for each of the 5 dashboards
- Enable/disable specific metrics (Maintenance, File Annotation)
- Adjust processing limits
- Run the function with your custom configuration

No code changes required!

### Per-Dashboard Configuration

Each dashboard has its own configurable views:

| Dashboard | Views to Configure | Default Space |
|-----------|-------------------|---------------|
| 🌳 Asset Hierarchy | Asset View | `cdf_cdm` |
| 🔧 Equipment-Asset | Equipment View | `cdf_cdm` |
| ⏱️ Time Series | Time Series View | `cdf_cdm` |
| 🛠️ Maintenance | Notification, MaintenanceOrder, FailureNotification Views | `rmdm` |
| 📄 File Annotation | CogniteDiagramAnnotation View | `cdf_cdm` |

### Default Configuration

By default, the function queries the **Cognite Core Data Model (CDM)** and **RMDM v1** views. The defaults are defined in `dashboards/configuration.py`:

```python
DEFAULT_CONFIG = {
    # Dashboard 1: Asset Hierarchy
    "asset_view_space": "cdf_cdm",
    "asset_view_external_id": "CogniteAsset",
    "asset_view_version": "v1",
    
    # Dashboard 2: Equipment-Asset
    "equipment_view_space": "cdf_cdm",
    "equipment_view_external_id": "CogniteEquipment",
    "equipment_view_version": "v1",
    
    # Dashboard 3: Time Series
    "ts_view_space": "cdf_cdm",
    "ts_view_external_id": "CogniteTimeSeries",
    "ts_view_version": "v1",
    
    # Dashboard 4: Maintenance Workflow
    "notification_view_space": "rmdm",
    "notification_view_external_id": "Notification",
    "notification_view_version": "v1",
    "maintenance_order_view_space": "rmdm",
    "maintenance_order_view_external_id": "MaintenanceOrder",
    "maintenance_order_view_version": "v1",
    "failure_notification_view_space": "rmdm",
    "failure_notification_view_external_id": "FailureNotification",
    "failure_notification_view_version": "v1",
    
    # Dashboard 5: File Annotation
    "annotation_view_space": "cdf_cdm",
    "annotation_view_external_id": "CogniteDiagramAnnotation",
    "annotation_view_version": "v1",
    
    # Feature Flags
    "enable_maintenance_metrics": True,
    "enable_file_annotation_metrics": True,
    
    # Processing Limits
    "max_assets": 150000,
    "max_equipment": 150000,
    "max_timeseries": 150000,
    "max_notifications": 150000,
    "max_maintenance_orders": 150000,
    "max_annotations": 200000,
}
```

### Alternative: SDK-Based Configuration

You can also pass configuration overrides when calling the function via SDK:

```python
from cognite.client import CogniteClient

client = CogniteClient()

# Call with custom configuration
client.functions.call(
    external_id="context_quality_handler",
    data={
        "asset_view_space": "my_custom_space",
        "asset_view_external_id": "MyAssetView",
        "asset_view_version": "v1",
        "enable_maintenance_metrics": False,  # Disable if RMDM not available
    }
)
```

---

### Project-Specific Field Names

Different projects may use different property names for certain fields. Here are the key fields you may need to customize:

| Field | Default Property Name | Location to Modify |
|-------|----------------------|-------------------|
| Asset Criticality | `criticality` | Line 402 in `handler.py` |
| Equipment Criticality | `criticality` | Line 444 in `handler.py` |
| Asset Type | `type` or `assetClass` | Line 414 in `handler.py` |
| Equipment Type | `equipmentType` | Line 440 in `handler.py` |
| Serial Number | `serialNumber` | Line 442 in `handler.py` |
| Manufacturer | `manufacturer` | Line 443 in `handler.py` |

---

## Alternative: Running the Function via SDK/UI

While the recommended approach is to use the dashboard's **⚙️ Configure & Run** tab, you can also run the function directly:

**Option 1: Using the CDF UI**
1. Navigate to **Data management** → **Build solutions** → **Functions**
2. Find `context_quality_handler`
3. Click **Call** and optionally provide configuration JSON

**Option 2: Using the SDK**

```python
from cognite.client import CogniteClient

client = CogniteClient()

# Run with default configuration
response = client.functions.call(
    external_id="context_quality_handler",
    data={}
)

print(f"Call ID: {response.id}, Status: {response.status}")
```

The function will:
1. Query all data from the configured views
2. Compute all quality metrics
3. Save results to a Cognite File: `contextualization_quality_metrics`

---

## Scheduling the Function

For continuous monitoring, schedule the function to run periodically:

```python
from cognite.client.data_classes import FunctionSchedule

# Schedule the Contextualization Quality Metrics function
client.functions.schedules.create(
    FunctionSchedule(
        name="Contextualization Quality Daily",
        function_external_id="context_quality_handler",
        cron_expression="0 6 * * *",  # Daily at 6 AM
        data={}
    )
)
```

---

## Troubleshooting

### "Function not available" when clicking Run Function

**Cause:** The Cognite Function has not finished deploying yet.

**Solution:**
1. **Wait 2-5 minutes** — Functions take time to deploy after `cdf deploy`
2. **Verify deployment status** in CDF:
   - Go to **Data management** → **Build solutions** → **Functions**
   - Look for `context_quality_handler`
   - Check that status is "Ready" (not "Deploying" or "Failed")
3. **Re-deploy if needed** — Run `cdf deploy` again from your project directory

### Dashboard shows "No metrics data available"

**Cause:** The Cognite Function has not been run yet, or it failed.

**Solution:**
1. Go to the **⚙️ Configure & Run** tab
2. Click **▶️ Run Function**
3. Wait for the function to complete (check status)
4. If it fails, check function logs in CDF

### Metrics show all zeros

**Cause:** The data model views are empty or misconfigured.

**Solution:**
1. Verify your data exists in the configured views
2. Check that the view space, external_id, and version match your data model
3. Use the **⚙️ Configure & Run** tab to update view configuration

### Function times out

**Cause:** Too much data to process within the 10-minute function limit.

**Solution:**
1. Reduce processing limits in the **⚙️ Configure & Run** tab under "Processing Limits (Advanced)"
2. Disable features you don't need:
   - Uncheck "Enable Maintenance Metrics" if RMDM is not needed
   - Uncheck "Enable File Annotation Metrics" if P&ID annotations are not needed

### Maintenance tab shows warning

**Cause:** RMDM v1 is not deployed or has no data.

**Solution:**
1. Deploy RMDM v1 to your project, OR
2. Disable maintenance metrics in the **⚙️ Configure & Run** tab, OR
3. Configure custom view names if your RMDM uses different view names

---

## Metrics Reference

The Contextualization Quality module computes **35+ metrics** across four categories. Below is a detailed explanation of each metric, including formulas and interpretation guidelines.

---

### 1. Asset Hierarchy Metrics

These metrics assess the structural quality and completeness of your asset hierarchy.

#### 1.1 Hierarchy Completion Rate

**What it measures:** The percentage of non-root assets that have a valid parent link.

**Formula:**

```
Hierarchy Completion Rate (%) = (Assets with Parent / Non-Root Assets) × 100

Where:
  Non-Root Assets = Total Assets - Root Assets
```

**Interpretation:**
- 🟢 **≥ 98%**: Excellent - Nearly complete hierarchy
- 🟡 **95-98%**: Warning - Some missing links
- 🔴 **< 95%**: Critical - Hierarchy has gaps

---

#### 1.2 Orphan Count & Rate

**What it measures:** Orphans are assets that have **no parent AND no children** - they are completely disconnected from the hierarchy.

**Formula:**

```
Orphan Rate (%) = (Orphan Count / Total Assets) × 100

Where:
  Orphan = Asset where parent == NULL AND children_count == 0
```

**Interpretation:**
- 🟢 **0 orphans**: Perfect - All assets are connected
- 🟡 **1-5 orphans**: Warning - Minor disconnected assets
- 🔴 **> 5 orphans**: Critical - Significant hierarchy issues

---

#### 1.3 Depth Statistics

**Average Depth:**

```
Average Depth = Σ(Depth of each asset) / Total Assets

Where:
  Depth = Number of levels from root to the asset (root = 0)
```

**Max Depth:** The deepest level in the hierarchy.

**Interpretation:**
- 🟢 **Max Depth ≤ 6**: Good - Reasonable hierarchy depth
- 🟡 **Max Depth 7-8**: Warning - Deep hierarchy
- 🔴 **Max Depth > 8**: Critical - Excessively deep hierarchy

---

#### 1.4 Breadth Statistics

**Average Children per Parent:**

```
Average Children = Σ(Children count per parent) / Number of parents
```

**Standard Deviation of Children:**

```
Std Dev = √(Σ(children_count - avg_children)² / n)
```

A high standard deviation indicates an uneven distribution (some parents have many children, others have few).

---

### 2. Equipment-Asset Relationship Metrics

These metrics measure the quality of equipment-to-asset mappings.

#### 2.1 Equipment Association Rate

**What it measures:** The percentage of equipment items that are linked to an asset.

**Formula:**

```
Equipment Association Rate (%) = (Equipment with Asset Link / Total Equipment) × 100
```

**Interpretation:**
- 🟢 **≥ 90%**: Excellent - Nearly all equipment is linked
- 🟡 **70-90%**: Warning - Some unlinked equipment
- 🔴 **< 70%**: Critical - Many equipment items lack asset links

---

#### 2.2 Asset Equipment Coverage

**What it measures:** The percentage of assets that have at least one equipment linked to them.

**Formula:**

```
Asset Equipment Coverage (%) = (Assets with Equipment / Total Assets) × 100
```

---

#### 2.3 Serial Number Completeness

**What it measures:** The percentage of equipment items that have the `serialNumber` property populated.

**Formula:**

```
Serial Number Completeness (%) = (Equipment with Serial Number / Total Equipment) × 100
```

**Interpretation:**
- 🟢 **≥ 90%**: Excellent - Good equipment traceability
- 🟡 **70-90%**: Warning - Some missing serial numbers
- 🔴 **< 70%**: Critical - Poor equipment documentation

---

#### 2.4 Manufacturer Completeness

**What it measures:** The percentage of equipment items that have the `manufacturer` property populated.

**Formula:**

```
Manufacturer Completeness (%) = (Equipment with Manufacturer / Total Equipment) × 100
```

---

#### 2.5 Type Consistency

**What it measures:** The percentage of equipment where the equipment type is consistent with the linked asset's type. Uses predefined mappings (e.g., a pump equipment should link to a pump asset).

**Formula:**

```
Type Consistency Rate (%) = (Consistent Relationships / Total Equipment) × 100
```

> 📝 **Note:** Type mappings are defined in lines **96-101** of `handler.py`. Modify these mappings to match your project's type definitions:
> ```python
> TYPE_MAPPINGS = {
>     "iso14224_va_di_diaphragm": ["VALVE", "CONTROL_VALVE"],
>     "iso14224_pu_centrifugal_pump": ["PUMP", "PUMPING_EQUIPMENT"],
>     # Add your custom mappings here
> }
> ```

---

#### 2.6 Critical Equipment Contextualization

**What it measures:** The percentage of critical equipment that is linked to an asset.

**Formula:**

```
Critical Equipment Contextualization (%) = (Critical Equipment Linked / Total Critical Equipment) × 100
```

> ⚠️ **Note:** Similar to critical assets, if your project uses a different field name for equipment criticality, modify line **444** in `handler.py`:
> ```python
> criticality=props.get("criticality"),  # Change "criticality" to your field name
> ```

---

### 3. Time Series Contextualization Metrics

These metrics measure how well time series data is linked to assets and the quality of the time series metadata.

#### 3.1 TS to Asset Contextualization Rate 

**What it measures:** The percentage of time series that are linked to at least one asset.

> ⚠️ **Why this is the primary metric:** An orphaned time series (not linked to any asset) cannot be associated with equipment, location, or business context. This makes the data difficult to discover and use. Every time series should be contextualized.

**Formula:**

```
TS to Asset Rate (%) = (Time Series with Asset Link / Total Time Series) × 100
```

**Interpretation:**
- 🟢 **≥ 95%**: Excellent - Nearly all TS are contextualized
- 🟡 **90-95%**: Warning - Some orphaned time series exist
- 🔴 **< 90%**: Critical - Many time series lack asset context

---

#### 3.2 Asset Monitoring Coverage

**What it measures:** The percentage of assets that have at least one time series linked to them.

> **Note:** It is acceptable for some assets (e.g., structural assets, buildings, organizational units) to not have time series. This metric helps identify assets that *could* benefit from monitoring data.

**Formula:**

```
Asset Monitoring Coverage (%) = (Assets with Time Series / Total Assets) × 100
```

**Interpretation:**
- 🟢 **> 80%**: Good - Most assets have monitoring data
- 🟡 **70-80%**: Warning - Some assets lack time series
- 🔴 **< 70%**: Critical - Many assets are unmonitored

---

#### 3.3 Critical Asset Coverage

**What it measures:** The percentage of critical assets that have time series linked. Critical assets are those with `criticality = "critical"` in their properties.

**Formula:**

```
Critical Asset Coverage (%) = (Critical Assets with TS / Total Critical Assets) × 100
```

**Interpretation:**
- 🟢 **100%**: Perfect - All critical assets are monitored
- 🟡 **≥ 95%**: Warning - Nearly all critical assets covered
- 🔴 **< 95%**: Critical - Critical assets are unmonitored

> ⚠️ **Note:** If your project uses a different property name for criticality (e.g., `priority`, `importance`, `criticalityLevel`), you must modify line **402** in `handler.py`:
> ```python
> if props.get("criticality") == "critical":  # Change "criticality" to your field name
> ```

---

#### 3.4 Source Unit Completeness

**What it measures:** The percentage of time series that have the `sourceUnit` property populated. The source unit represents the original unit of measurement from the data source (e.g., "°C", "bar", "rpm").

**Formula:**

```
Source Unit Completeness (%) = (TS with sourceUnit / Total TS) × 100
```

**Interpretation:**
- 🟢 **> 95%**: Excellent - Nearly all TS have unit information
- 🟡 **90-95%**: Good - Most TS have units
- 🔴 **< 90%**: Warning - Many TS lack unit metadata

---

#### 3.5 Target Unit Completeness (Standardized Unit)

**What it measures:** The percentage of time series with a standardized `unit` property. This represents the converted/normalized unit after any unit transformations.

**Formula:**

```
Target Unit Completeness (%) = (TS with unit / Total TS) × 100
```

---

#### 3.6 Unit Mapping Rate

**What it measures:** When both `sourceUnit` and `unit` are present, this metric tracks how many have matching values (indicating no conversion was needed or conversion is complete).

**Formula:**

```
Unit Mapping Rate (%) = (TS where unit == sourceUnit / TS with both units) × 100
```

---

#### 3.7 Data Freshness

**What it measures:** The percentage of time series that have been updated within the last N days (default: 30 days).

**Formula:**

```
Data Freshness (%) = (TS updated within N days / Total TS) × 100
```

**Interpretation:**
- 🟢 **≥ 90%**: Excellent - Data is current
- 🟡 **70-90%**: Warning - Some stale data
- 🔴 **< 70%**: Critical - Significant stale data

---

#### 3.8 Average Time Since Last TS Update

**What it measures:** The average time difference between "now" and the `lastUpdatedTime` of time series.

> ⚠️ **Important:** This metric tracks **metadata updates** (when the time series definition was last modified), NOT when the latest datapoint was ingested. For actual data freshness, see the "Data Freshness" metric which checks if TS have been updated within the last N days.

**Formula:**

```
Avg Time Since Last TS Update (hours) = Σ(Now - lastUpdatedTime) / Count of valid TS
```

---

#### 3.9 Historical Data Completeness

**What it measures:** The percentage of the time span that contains actual data (vs gaps). A gap is defined as a period longer than the threshold (default: 7 days) without any datapoints.

**Formula:**

```
Historical Data Completeness (%) = ((Total Time Span - Total Gap Duration) / Total Time Span) × 100
```

**Example:** If a time series spans 365 days but has a 30-day gap:
```
Completeness = (365 - 30) / 365 × 100 = 91.8%
```

**Interpretation:**
- 🟢 **≥ 95%**: Excellent - Minimal data gaps
- 🟡 **85-95%**: Warning - Some significant gaps
- 🔴 **< 85%**: Critical - Major data gaps exist

---

### 4. Maintenance Workflow Metrics (RMDM v1)

These metrics measure the quality of maintenance data from the **RMDM v1** (Reference Data Model for Maintenance) data model. This includes notifications, work orders, and failure documentation.

#### Prerequisites

> ⚠️ **RMDM v1 Required:** These metrics require RMDM v1 to be deployed and populated in your CDF project. If RMDM v1 is not available, the Maintenance Workflow tab will display a helpful warning message.

**Required Views:**

The following views must exist and contain data in your CDF project:

| View | Space | Description |
|------|-------|-------------|
| `Notification` | `rmdm` | Maintenance notifications/requests |
| `MaintenanceOrder` | `rmdm` | Work orders |
| `FailureNotification` | `rmdm` | Failure analysis records |

**Using a Different RMDM Model?**

If your RMDM data model uses a different space name or view names, you can configure them in:

📁 **`metrics/common.py`** → **Lines 40-48** (DEFAULT_CONFIG section)

```python
# View configurations - RMDM v1 (Maintenance Workflow)
"notification_view_space": "your_custom_space",
"notification_view_external_id": "YourNotificationView",
"notification_view_version": "v1",
"maintenance_order_view_space": "your_custom_space",
"maintenance_order_view_external_id": "YourMaintenanceOrderView",
"maintenance_order_view_version": "v1",
"failure_notification_view_space": "your_custom_space",
"failure_notification_view_external_id": "YourFailureNotificationView",
"failure_notification_view_version": "v1",
```

Alternatively, pass these as function input overrides:

```python
client.functions.call(
    external_id="context_quality_handler",
    data={
        "notification_view_space": "my_rmdm_space",
        "notification_view_external_id": "MyNotificationView",
        # ... other overrides
    }
)
```

---

#### Understanding Metric Direction

Based on domain expert guidance, maintenance metrics have different priorities:

| Metric Type | Direction | Explanation |
|-------------|-----------|-------------|
| **CRITICAL** | High values required | All items should meet this criteria |
| **INFORMATIONAL** | Low values are OK | Not all items need to meet this criteria |

---

#### 4.1 Work Order → Notification Rate (CRITICAL)

**What it measures:** The percentage of work orders that have a linked notification.

**Why it's critical:** All work orders SHOULD originate from a notification. This is the standard maintenance workflow: a notification triggers an investigation, which may result in a work order.

**Formula:**

```
Work Order → Notification Rate (%) = (Work Orders with Notification / Total Work Orders) × 100
```

**Interpretation:**
- 🟢 **≥ 95%**: Excellent - Proper workflow followed
- 🟡 **80-95%**: Warning - Some work orders created without notifications
- 🔴 **< 80%**: Critical - Many work orders bypass the notification process

---

#### 4.2 Notification → Work Order Rate (INFORMATIONAL)

**What it measures:** The percentage of notifications that are linked to a maintenance order.

**Why it's informational:** Many notifications do NOT need a work order. Examples include: informational notifications, minor observations, issues already resolved, or notifications that don't require maintenance action.

**Formula:**

```
Notification → Work Order Rate (%) = (Notifications with Work Order / Total Notifications) × 100
```

**Interpretation:**
- 🔵 **Any value**: Informational - Low values are acceptable
- This metric is shown in blue to indicate it's informational, not critical

---

#### 4.3 Notification → Asset Rate (CRITICAL)

**What it measures:** The percentage of notifications that are linked to an asset.

**Formula:**

```
Notification → Asset Rate (%) = (Notifications with Asset / Total Notifications) × 100
```

**Interpretation:**
- 🟢 **≥ 90%**: Excellent - Notifications have proper asset context
- 🟡 **70-90%**: Warning - Some notifications lack asset context
- 🔴 **< 70%**: Critical - Many notifications cannot be traced to assets

---

#### 4.4 Notification → Equipment Rate (CRITICAL)

**What it measures:** The percentage of notifications that are linked to equipment.

**Formula:**

```
Notification → Equipment Rate (%) = (Notifications with Equipment / Total Notifications) × 100
```

---

#### 4.5 Work Order → Asset Rate (CRITICAL)

**What it measures:** The percentage of work orders that are linked to an asset.

**Formula:**

```
Work Order → Asset Rate (%) = (Work Orders with Asset / Total Work Orders) × 100
```

**Interpretation:**
- 🟢 **≥ 90%**: Excellent - Work orders have proper asset context
- 🟡 **70-90%**: Warning - Some work orders lack asset context
- 🔴 **< 70%**: Critical - Technicians may not know where to go

---

#### 4.6 Work Order → Equipment Rate (CRITICAL)

**What it measures:** The percentage of work orders that are linked to equipment.

**Formula:**

```
Work Order → Equipment Rate (%) = (Work Orders with Equipment / Total Work Orders) × 100
```

---

#### 4.7 Work Order Completion Rate

**What it measures:** The percentage of work orders that have been marked as completed.

**Formula:**

```
Work Order Completion Rate (%) = (Completed Work Orders / Total Work Orders) × 100
```

**Interpretation:**
- 🟢 **≥ 80%**: Good - Most work is being completed
- 🟡 **60-80%**: Warning - Backlog building up
- 🔴 **< 60%**: Critical - Significant work backlog

---

#### 4.8 Failure Mode Documentation Rate

**What it measures:** The percentage of failure notifications that have a failure mode documented.

**Formula:**

```
Failure Mode Rate (%) = (Failure Notifications with Mode / Total Failure Notifications) × 100
```

---

#### 4.9 Failure Mechanism Documentation Rate

**What it measures:** The percentage of failure notifications that have a failure mechanism documented.

**Formula:**

```
Failure Mechanism Rate (%) = (Failure Notifications with Mechanism / Total Failure Notifications) × 100
```

---

#### 4.10 Failure Cause Documentation Rate

**What it measures:** The percentage of failure notifications that have a failure cause documented.

**Formula:**

```
Failure Cause Rate (%) = (Failure Notifications with Cause / Total Failure Notifications) × 100
```

**Interpretation (for all failure documentation rates):**
- 🟢 **≥ 80%**: Excellent - Good failure documentation for reliability analysis
- 🟡 **50-80%**: Warning - Incomplete failure documentation
- 🔴 **< 50%**: Critical - Insufficient data for reliability engineering

---

#### 4.11 Asset Maintenance Coverage (INFORMATIONAL)

**What it measures:** The percentage of assets that have at least one notification or work order associated with them.

**Why it's informational:** Not all assets require maintenance records. Examples include: buildings, land, infrastructure, or assets that have never had issues.

**Formula:**

```
Asset Maintenance Coverage (%) = (Assets with Notifications or Work Orders / Total Assets) × 100
```

**Interpretation:**
- 🔵 **Any value**: Informational - Low values are acceptable

---

#### 4.12 Equipment Maintenance Coverage (INFORMATIONAL)

**What it measures:** The percentage of equipment that has at least one notification or work order associated with it.

**Formula:**

```
Equipment Maintenance Coverage (%) = (Equipment with Notifications or Work Orders / Total Equipment) × 100
```

**Interpretation:**
- 🔵 **Any value**: Informational - Low values are acceptable

---

### 5. File Annotation Metrics (CDM CogniteDiagramAnnotation)

These metrics measure the quality of P&ID diagram annotations that link files (diagrams) to assets and equipment. The data comes from the CDM `CogniteDiagramAnnotation` edge type.

#### Prerequisites

> ⚠️ **P&ID Parsing Required:** These metrics require P&ID files to be processed with the Cognite Diagram Parsing service. If no annotations exist, the File Annotation tab will display a helpful warning message.

**Required Views:**

| View | Space | Description |
|------|-------|-------------|
| `CogniteDiagramAnnotation` | `cdf_cdm` | Diagram annotation edges linking files to entities |

#### Configuration

If your annotations are stored in a different space or view, configure them in `metrics/common.py`:

```python
"annotation_view_space": "cdf_cdm",
"annotation_view_external_id": "CogniteDiagramAnnotation",
"annotation_view_version": "v1",
```

To disable file annotation metrics entirely, set:

```python
"enable_file_annotation_metrics": False,
```

---

#### 5.1 User-Provided Reference Numbers

Unlike other dashboards, the File Annotation dashboard includes **user input boxes** for reference numbers that are not available in the data model. This allows you to calculate completion rates on-the-fly.

| Input Field | Description |
|-------------|-------------|
| **Files in Scope** | Total number of P&ID files/documents that should be annotated |
| **Expected Asset Tags** | Estimated number of asset tags that should be detected across all files |

These values are used to calculate:
- **File Processing Rate** = (Files with Annotations / Files in Scope) × 100
- **Asset Tag Detection Rate** = (Asset Tag Annotations / Expected Tags) × 100

> 💡 **Tip:** If you don't know the exact numbers, leave them at 0. The dashboard will still show all absolute metrics (counts, confidence, status).

---

#### 5.2 Total Annotations

**What it measures:** The total count of `CogniteDiagramAnnotation` edges in the data model.

---

#### 5.3 Files with Annotations

**What it measures:** The number of unique files (source nodes) that have at least one annotation.

---

#### 5.4 Average Confidence Score

**What it measures:** The average confidence value across all annotations.

**Interpretation:**
- 🟢 **≥ 90%**: Excellent - High-quality annotations
- 🟡 **70-90%**: Good - Most annotations are reliable
- 🔴 **< 70%**: Warning - Many low-confidence annotations may need review

---

#### 5.5 Confidence Distribution

**What it measures:** The distribution of annotations by confidence level.

| Level | Range | Description |
|-------|-------|-------------|
| High | ≥ 90% | Highly reliable annotations |
| Medium | 50-90% | Moderate confidence, may benefit from review |
| Low | < 50% | Low confidence, likely needs manual review |

---

#### 5.6 Annotation Status Distribution

**What it measures:** The distribution of annotations by review status.

| Status | Description |
|--------|-------------|
| **Approved** | Annotations validated by a user or automated process |
| **Suggested** | Pending review - generated by the system but not yet confirmed |
| **Rejected** | Incorrect matches that were rejected during review |

**Interpretation:**
- High **Suggested** count indicates a review backlog
- High **Approved** rate (≥80%) indicates mature annotation quality

---

#### 5.7 Annotation Types

**What it measures:** The breakdown of annotations by target entity type.

| Type | Description |
|------|-------------|
| **Asset Tags** | Annotations linking to assets/equipment (e.g., tag detection) |
| **File Links** | Annotations linking to other files (e.g., document cross-references) |
| **Other** | Annotations to other entity types |

---

## AI-Powered Insights

Each dashboard includes an **🤖 AI Insights** section that uses the Cognite AI Chat Completions API to generate contextual summaries of the metrics.

### How It Works

1. Click the **"✨ Generate Insights"** button on any dashboard
2. The AI analyzes all metrics with domain-specific knowledge
3. Receive a 3-5 sentence actionable summary

### What the AI Provides

- **Critical Issue Identification**: Highlights metrics that need immediate attention
- **Positive Acknowledgment**: Confirms what's working well
- **Priority Actions**: Suggests 1-2 specific next steps
- **Context-Aware Interpretation**: Knows which metrics are CRITICAL vs INFORMATIONAL

### Directional Rules

The AI is trained with explicit rules based on domain expert guidance:

| Dashboard | CRITICAL Metrics | INFORMATIONAL Metrics |
|-----------|------------------|----------------------|
| **Asset Hierarchy** | Hierarchy Completion, Orphan Rate | Depth metrics |
| **Equipment-Asset** | Equipment→Asset Association | Asset Equipment Coverage |
| **Time Series** | TS→Asset Contextualization | Asset Monitoring Coverage |
| **Maintenance** | WO→Notification, WO→Asset | Notification→WO, Maintenance Coverage |
| **File Annotation** | Confidence Score, Approved Rate | File Processing Rate (if reference not provided) |

The AI will NOT flag informational metrics as problems, even if the values are low.

### Example Output

> "Your time series contextualization is excellent at 98.5%, with only 15 orphaned time series out of 1,000. However, 3 critical assets are missing monitoring data. Priority: Address the unmonitored critical assets first, as these represent the highest-risk gaps in your monitoring coverage."

---

## Support

For troubleshooting or deployment issues:

- Refer to the [Cognite Documentation](https://docs.cognite.com)
- Contact your **Cognite support team**
- Join the Slack channel **#topic-deployment-packs** for community support and discussions

---


