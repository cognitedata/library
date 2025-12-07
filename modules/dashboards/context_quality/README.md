# Contextualization Quality Dashboard

## Overview

The **Contextualization Quality Dashboard** module provides a comprehensive solution for measuring, monitoring, and visualizing the **contextualization quality** of your data in **Cognite Data Fusion (CDF)**. It consists of two main components:

1. **Cognite Function** (`context_quality_handler`) - Computes all quality metrics and saves them as a JSON file in CDF
2. **Streamlit Dashboard** (`context_quality_dashboard`) - Visualizes the pre-computed metrics with interactive gauges, charts, and tables

This module helps data engineers and operations teams understand how well their data is contextualized across three key dimensions:

- **Time Series Contextualization** - How well time series are linked to assets
- **Asset Hierarchy Quality** - Structural integrity of the asset tree
- **Equipment-Asset Relationships** - Quality of equipment-to-asset mappings

---

## Module Components

```
context_quality/
├── auth/                                    # Authentication configurations
├── data_sets/
│   └── context_quality_dashboard.DataSet.yaml  # Dataset for storing function code and files
├── functions/
│   ├── context_quality_handler/
│   │   └── handler.py                       # Main Cognite Function code
│   └── context_quality.Function.yaml        # Function configuration
├── streamlit/
│   ├── context_quality_dashboard/
│   │   ├── main.py                          # Streamlit dashboard code
│   │   └── requirements.txt                 # Python dependencies
│   └── context_quality_dashboard.Streamlit.yaml  # Streamlit app configuration
├── module.toml                              # Module metadata
└── README.md                                # This file
```

---

## Metrics Reference

The Contextualization Quality module computes **25+ metrics** across three categories. Below is a detailed explanation of each metric, including formulas and interpretation guidelines.

---

### 1. Time Series Contextualization Metrics

These metrics measure how well time series data is linked to assets and the quality of the time series metadata.

#### 1.1 Asset TS Association Rate

**What it measures:** The percentage of assets that have at least one time series linked to them.

**Formula:**

```
Asset TS Association Rate (%) = (Assets with Time Series / Total Assets) × 100
```

**Interpretation:**
- 🟢 **> 80%**: Good - Most assets have monitoring data
- 🟡 **70-80%**: Warning - Some assets lack time series
- 🔴 **< 70%**: Critical - Many assets are unmonitored

---

#### 1.2 Critical Asset Coverage

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

#### 1.3 Source Unit Completeness

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

#### 1.4 Target Unit Completeness (Standardized Unit)

**What it measures:** The percentage of time series with a standardized `unit` property. This represents the converted/normalized unit after any unit transformations.

**Formula:**

```
Target Unit Completeness (%) = (TS with unit / Total TS) × 100
```

---

#### 1.5 Unit Mapping Rate

**What it measures:** When both `sourceUnit` and `unit` are present, this metric tracks how many have matching values (indicating no conversion was needed or conversion is complete).

**Formula:**

```
Unit Mapping Rate (%) = (TS where unit == sourceUnit / TS with both units) × 100
```

---

#### 1.6 Data Freshness

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

#### 1.7 Processing Lag

**What it measures:** The average time difference between "now" and the `lastUpdatedTime` of time series.

**Formula:**

```
Processing Lag (hours) = Σ(Now - lastUpdatedTime) / Count of valid TS
```

---

#### 1.8 Historical Data Completeness

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

### 2. Asset Hierarchy Metrics

These metrics assess the structural quality and completeness of your asset hierarchy.

#### 2.1 Hierarchy Completion Rate

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

#### 2.2 Orphan Count & Rate

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

#### 2.3 Depth Statistics

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

#### 2.4 Breadth Statistics

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

### 3. Equipment-Asset Relationship Metrics

These metrics measure the quality of equipment-to-asset mappings.

#### 3.1 Equipment Association Rate

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

#### 3.2 Asset Equipment Coverage

**What it measures:** The percentage of assets that have at least one equipment linked to them.

**Formula:**

```
Asset Equipment Coverage (%) = (Assets with Equipment / Total Assets) × 100
```

---

#### 3.3 Serial Number Completeness

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

#### 3.4 Manufacturer Completeness

**What it measures:** The percentage of equipment items that have the `manufacturer` property populated.

**Formula:**

```
Manufacturer Completeness (%) = (Equipment with Manufacturer / Total Equipment) × 100
```

---

#### 3.5 Type Consistency

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

#### 3.6 Critical Equipment Contextualization

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

## Configuration

### Default Data Model View

By default, the function queries the **Cognite Core Data Model (CDM)** views. The default configuration is set in lines **68-93** of `handler.py`:

```python
DEFAULT_CONFIG = {
    "chunk_size": 500,
    # View configurations - DEFAULT: Cognite Core Data Model (cdf_cdm)
    "asset_view_space": "cdf_cdm",
    "asset_view_external_id": "CogniteAsset",
    "asset_view_version": "v1",
    "ts_view_space": "cdf_cdm",
    "ts_view_external_id": "CogniteTimeSeries",
    "ts_view_version": "v1",
    "equipment_view_space": "cdf_cdm",
    "equipment_view_external_id": "CogniteEquipment",
    "equipment_view_version": "v1",
    # Processing limits
    "max_timeseries": 150000,
    "max_assets": 150000,
    "max_equipment": 150000,
    # Freshness settings
    "freshness_days": 30,
    # Historical gap analysis
    "enable_historical_gaps": True,
    "gap_sample_rate": 20,
    "gap_threshold_days": 7,
    "gap_lookback": "1000d-ago",
    # ... more config
}
```

### Changing the Data Model View

If your project uses a **custom data model** instead of the CDM, modify lines **71-79** in `handler.py`:

```python
# Example: Using a custom data model view
"asset_view_space": "your_custom_space",
"asset_view_external_id": "YourAssetView",
"asset_view_version": "v1",
"ts_view_space": "your_custom_space",
"ts_view_external_id": "YourTimeSeriesView",
"ts_view_version": "v1",
"equipment_view_space": "your_custom_space",
"equipment_view_external_id": "YourEquipmentView",
"equipment_view_version": "v1",
```

Alternatively, you can pass configuration overrides when calling the function:

```python
client.functions.call(
    external_id="context_quality_handler",
    data={
        "asset_view_space": "my_space",
        "asset_view_external_id": "MyAssetView",
        "asset_view_version": "v1"
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

---

## Post-Deployment: Running the Function

> ⚠️ **IMPORTANT**: The Cognite Function **MUST be executed at least once** before launching the Streamlit dashboard. The dashboard reads pre-computed metrics from a JSON file that the function generates.

### Run the Function

After deployment, trigger the function:

**Option 1: Using the CDF UI**
1. Navigate to **Functions** in the CDF UI
2. Find `context_quality_handler`
3. Click **Run** or **Call**

**Option 2: Using the SDK**

```python
from cognite.client import CogniteClient

client = CogniteClient()

# Run with default configuration
response = client.functions.call(
    external_id="context_quality_handler",
    data={}
)

print(response)
```

**Option 3: Using cdf-tk CLI**

```bash
cdf functions call context_quality_handler
```

### Verify Function Execution

The function will:
1. Query all Time Series, Assets, and Equipment from the configured views
2. Compute all quality metrics
3. Save results to a Cognite File: `contextualization_quality_metrics`

You can verify the file exists:

```python
file = client.files.retrieve(external_id="contextualization_quality_metrics")
print(f"File created: {file.name}, Size: {file.size} bytes")
```

---

## Launching the Dashboard

Once the function has run successfully:

1. Navigate to **Streamlit Apps** in the CDF UI
2. Find **Contextualization Quality** dashboard
3. Click to launch

The dashboard will load the pre-computed metrics and display:
- **Asset Hierarchy Quality** tab
- **Equipment-Asset Quality** tab
- **Time Series Contextualization** tab

---

## Scheduling the Function

For continuous monitoring, schedule the function to run periodically:

```python
from cognite.client.data_classes import FunctionSchedule

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

### Dashboard shows "Could not load metrics file"

**Cause:** The Cognite Function has not been run yet, or it failed.

**Solution:**
1. Run the function manually (see above)
2. Check function logs for errors
3. Verify the file `contextualization_quality_metrics` exists in CDF Files

### Metrics show all zeros

**Cause:** The data model views are empty or misconfigured.

**Solution:**
1. Verify your data exists in the configured views
2. Check that the view space, external_id, and version match your data model
3. Modify lines 71-79 in `handler.py` if using a custom data model

### Function times out

**Cause:** Too much data to process within the 10-minute function limit.

**Solution:**
1. Reduce processing limits in configuration:
   ```python
   {
       "max_timeseries": 50000,
       "max_assets": 50000,
       "max_equipment": 50000
   }
   ```
2. Disable historical gap analysis: `"enable_historical_gaps": False`

---

## Support

For troubleshooting or deployment issues:

- Refer to the [Cognite Documentation](https://docs.cognite.com)
- Contact your **Cognite support team**

---


