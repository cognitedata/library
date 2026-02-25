# Data Prerequisites for RCA with RMDM Agents

This document describes the **RMDM v1** data model views and fields that each RCA agent uses. Ensuring these views are populated with the required data is necessary for the agents to work as intended.

The RCA module depends on the **RMDM v1** data model. Deploy the [`modules/models/rmdm_v1`](../../models/rmdm_v1/) module first and load your maintenance and asset data into the instance space (e.g. `rmdm`) before using the agents. For the full RMDM v1 schema (containers, views, and properties), see the [rmdm_v1 README](../../models/rmdm_v1/README.md) and the view/container YAML files under `modules/models/rmdm_v1/data_models/`.

---

## Summary: Agents and RMDM Views

| Agent | RMDM views used | Purpose |
|-------|-----------------|---------|
| **Cause Map Agent** | Equipment, FailureNotification, FailureMode, EquipmentClass | Find equipment, latest/most common failure notifications, failure mode code, equipment class code; generate cause maps |
| **RCA Agent** | Asset, FailureNotification, Timeseries_ext, File_ext, MaintenanceOrder | Full RCA workflow: assets, failures, time series, documents, images, work orders |
| **Time Series Agent** | Asset, Timeseries_ext | Find assets and time series; related-asset hierarchy uses Asset; TS datapoints use CDF Time Series API |

Some agent tools also use **CDF CDM** views (e.g. `CogniteTimeSeries` in space `cdf_cdm`) for time series by related assets. Those are outside RMDM but required for that functionality.

---

## 1. Cause Map Agent (`cause_map_agent`)

The Cause Map Agent finds equipment, its failure notifications, failure mode, and equipment class, then builds a cause map (e.g. for the RCA function or canvas).

### Supported equipment classes (4)

The cause map templates in this module support **four equipment classes**. The agent looks up the cause map by the equipment class **code** from RMDM. The codes must match the keys in the bundled cause map file (`combined_cause_map.json`):

| Code | Equipment class |
|------|-----------------|
| **HE** | Heat exchanger |
| **VE** | Valve |
| **PU** | Pump |
| **CO** | Compressor |

Only equipment whose `equipmentClass` has one of these four codes will have a cause map template available. For other equipment classes, the RCA function may return an error that the cause map template was not found.

### 1.1 Equipment (view: `Equipment`, space: `rmdm`)

| Field / relation | Required for agent | Notes |
|------------------|--------------------|--------|
| `name` | Yes | Used to match equipment by name (exact match). |
| `asset` | Recommended | Links equipment to an asset (functional location). |
| `equipmentClass` | Yes | Relation to EquipmentClass; agent needs the **code** of the equipment class for the RCA/cause map. |

**Populate:** At least `name` and `equipmentClass` (and ideally `asset`) so the agent can find equipment and resolve the equipment class code.

### 1.2 FailureNotification (view: `FailureNotification`, space: `rmdm`)

FailureNotification implements the Notification view; the agent filters by equipment and uses notification timing/priority.

| Field / relation | Required for agent | Notes |
|------------------|--------------------|--------|
| `equipment` | Yes | Links to Equipment; used to filter failure notifications for the selected equipment. |
| `failureMode` | Yes | Relation to FailureMode; agent uses it to get the failure mode **code** for the cause map. |
| `actualStartTime` (startTime) | Yes | Used to pick the “latest” failure notification (e.g. by start time). |
| `priority` | Recommended | From CogniteNotification (IDM); agent filters for high priority (e.g. 1 or 2) when using “latest high priority” path. |

**Populate:** At least `equipment`, `failureMode`, and start time so the agent can find the latest (or latest high-priority) failure notification and its failure mode.

### 1.3 FailureMode (view: `FailureMode`, space: `rmdm`)

| Field | Required for agent | Notes |
|-------|--------------------|--------|
| `code` | Yes | Used as the failure mode identifier (e.g. 3-letter code) passed to the RCA/cause map logic. |

**Populate:** `code` for each failure mode used in failure notifications.

### 1.4 EquipmentClass (view: `EquipmentClass`, space: `rmdm`)

| Field | Required for agent | Notes |
|-------|--------------------|--------|
| `code` | Yes | Used as the equipment class identifier passed to the RCA/cause map function. |

**Populate:** `code` for each equipment class referenced by Equipment.

---

## 2. RCA Agent (`rca_agent`)

The RCA Agent supports the full RCA workflow: assets, failure notifications, time series, documents, images, and maintenance orders.

### 2.1 Asset (view: `Asset`, space: `rmdm`)

| Field / relation | Required for agent | Notes |
|------------------|--------------------|--------|
| `parent` | Yes | Used to traverse hierarchy (parent, children, siblings) for “Find time series for related assets” and similar. |
| `externalId` / `space` | Yes | Identity and scope for queries. |

**Populate:** Asset hierarchy with `parent` set so that parent/children/siblings and related-asset time series work.

### 2.2 FailureNotification (view: `FailureNotification`, space: `rmdm`)

| Field / relation | Required for agent | Notes |
|------------------|--------------------|--------|
| `equipment` | Recommended | Links to Equipment; used when filtering notifications by equipment. |
| `asset` | Recommended | Links to Asset (from Notification). |
| `failureMode` | Recommended | For failure context and linking to FailureMode. |

**Populate:** Notifications linked to `equipment` and/or `asset` (and optionally `failureMode`) for meaningful RCA context.

### 2.3 Timeseries_ext (view: `Timeseries_ext`, space: `rmdm`)

| Field / relation | Required for agent | Notes |
|------------------|--------------------|--------|
| `assets` | Recommended | Links time series to assets so the agent can find “time series for this asset.” |
| `equipment` | Optional | Links time series to equipment. |
| Metadata (name, description, etc.) | Recommended | For display and search. |

**Populate:** Time series instances with at least `assets` (or `equipment`) so the agent can list and query time series by asset/equipment.

**Note:** The “Find time series for related assets” tool also uses the **CDF CDM** view `CogniteTimeSeries` (space `cdf_cdm`) for querying by asset. RMDM `Timeseries_ext` and CDM time series linkage to the same asset hierarchy should be consistent for best results.

### 2.4 File_ext (view: `File_ext`, space: `rmdm`)

Used by “Find related documents” and “Find images” (runPythonCode tools).

| Field / relation | Required for agent | Notes |
|------------------|--------------------|--------|
| `assets` | Yes | Used to filter files by asset (e.g. documents/images for an asset). |
| `mimeType` | Yes | Used to filter PDFs vs images (e.g. `application/pdf`, `image/jpeg`). |
| `category` | Optional | Used in document tool to filter by file category (e.g. NORSOK categories). |
| `name`, `description` | Recommended | For display and search. |

**Populate:** Files linked to assets via `assets`, with correct `mimeType` (and optional `category`) so document and image tools return relevant results.

### 2.5 MaintenanceOrder (view: `MaintenanceOrder`, space: `rmdm`)

| Field / relation | Required for agent | Notes |
|------------------|--------------------|--------|
| `assets` / `mainAsset` | Yes | Used to filter work orders by asset. |
| `equipment` | Recommended | Links work order to equipment. |
| `type` (or type code) | Yes | Used to distinguish corrective (e.g. PM02) vs preventive (e.g. PM03) orders. |

**Populate:** Maintenance orders with `mainAsset` or `assets` and type/code so the agent can list corrective/preventive maintenance for an asset.

---

## 3. Time Series Agent (`ts_agent`)

The Time Series Agent finds assets and time series, and can find time series for related assets (parent, children, siblings).

### 3.1 Asset (view: `Asset`, space: `rmdm`)

| Field / relation | Required for agent | Notes |
|------------------|--------------------|--------|
| `parent` | Yes | Required for “Find time series for related assets” (parent, children, siblings). |
| `externalId` / `space` | Yes | Identity and scope. |

**Populate:** Asset hierarchy with `parent` set so related-asset time series queries work.

### 3.2 Timeseries_ext (view: `Timeseries_ext`, space: `rmdm`)

| Field / relation | Required for agent | Notes |
|------------------|--------------------|--------|
| `assets` | Yes | Used to list and search time series by asset. |
| `equipment` | Optional | Alternative link to equipment. |
| Metadata (name, description, etc.) | Recommended | For display and search. |

**Populate:** Time series with `assets` (or `equipment`) so the agent can find time series for an asset and for related assets.

**Note:** The “Find time series for related assets” tool queries the **CDF CDM** view `CogniteTimeSeries` (space `cdf_cdm`) with `assets`; the “Query time series data points” tool uses the CDF Time Series API. Ensure RMDM `Timeseries_ext` and CDM time series are populated and linked to the same asset hierarchy for full functionality.

---

## 4. Cross-View Dependencies

- **Asset** is central: Cause Map uses it via Equipment → asset; RCA and TS use it for hierarchy and for linking notifications, files, maintenance orders, and time series.
- **Equipment** links to **Asset** and **EquipmentClass**; Cause Map and RCA depend on these for equipment lookup and class code.
- **FailureNotification** links to **Equipment**, **Asset**, and **FailureMode**; Cause Map and RCA use these for failure context and cause map generation.
- **File_ext** and **MaintenanceOrder** must link to **Asset** (and optionally **Equipment**) so the RCA agent can find documents, images, and work orders per asset.

---

## 5. Minimal Data Checklist

- **RMDM v1** deployed; instance space (e.g. `rmdm`) created and used consistently.
- **Asset:** Hierarchy with `parent` populated; at least the assets you want to analyze.
- **Equipment:** `name`, `asset`, `equipmentClass` populated for equipment used in RCA/cause maps.
- **EquipmentClass:** `code` populated for classes used by Equipment.
- **FailureMode:** `code` populated for modes used in FailureNotification.
- **FailureNotification:** `equipment`, `failureMode`, and start time (and optionally priority) for notifications used in cause map and RCA.
- **Timeseries_ext:** `assets` (or `equipment`) and metadata for time series you want to analyze.
- **File_ext:** `assets` and `mimeType` (and optional `category`) for documents and images used in RCA.
- **MaintenanceOrder:** `mainAsset` or `assets` and type/code for work orders used in RCA.

For full RMDM v1 schema and property details, see the [rmdm_v1 data model](../../models/rmdm_v1/) in the library.
