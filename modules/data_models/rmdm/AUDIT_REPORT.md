# CDF Best Practices Audit

**Project:** rmdm
**Date:** 2026-06-17
**Audited by:** cog-vd-audit (cog-vd-best-practices plugin)

---

## Summary

| Domain | [PASS] Pass | [WARN] Warning | [FAIL] Fail | Status |
|---|---:|---:|---:|---|
| A. Naming Conventions | 12 | 2 | 0 | [YELLOW] |
| B. Data Modeling      | 14 | 2 | 0 | [YELLOW] |
| C. Transformations    | 1 | 0 | 0 | [GREEN] |
| D. Functions          | 1 | 0 | 0 | [GREEN] |
| E. Workflows          | 1 | 0 | 0 | [GREEN] |
| F. DMS Queries        | 1 | 0 | 0 | [GREEN] |
| **Total**             | **30** | **4** | **0** | |

> [GREEN] No failures - [YELLOW] Warnings only - [RED] One or more failures

---

## A. Naming Conventions

**Files scanned:** 28   **Identifiers checked:** 27

### [FAIL] Failures

| File | Resource | Identifier | Issue | Suggested Fix |
|---|---|---|---|---|
| | | | No failures after remediation. | |

### [WARN] Warnings

| File | Resource | Identifier | Note |
|---|---|---|---|
| `data_models/rmdm.space.yaml` | Data model space | `rmdm` | Plugin convention prefers DM spaces like `dm_dom_<domain>`. This module has an established short space identifier; changing it would create a migration and deployment-compatibility decision. |
| `data_models/rmdm_v1.datamodel.yaml` | Data model external ID | `rmdm_v1` | Plugin convention prefers PascalCase data model external IDs with model tier. Existing module convention is snake_case with version suffix; changing this would create a new data model identity. |

### [PASS] Passed

All local container and view external IDs use PascalCase. Local properties use camelCase. No unsafe characters, GUID-like identifiers, generic placeholders, or capitalization-only duplicates were found.

---

## B. Data Modeling

### [FAIL] Failures

| File | Check | Detail | Fix |
|---|---|---|---|
| | | No failures after remediation. | |

### [WARN] Warnings

| File | Check | Detail |
|---|---|---|
| `data_models/*.yaml` | Template variables for space/version | Local files hardcode `rmdm` and `v1`. The plugin recommends Toolkit variables for module reuse, but changing schema spaces or version references should be coordinated with the deployment configuration and any existing consumers. |
| `README.md` | Consumer tracking | The data-modeling guidance recommends a `consumers:` section for known apps and pinned versions before deleting or replacing deployed view versions. None is present. |

### [PASS] Passed

All 13 local views are referenced in `rmdm_v1.datamodel.yaml`; no orphaned local views were found.

All local `usedFor: node` container properties are exposed in the corresponding local view.

All view properties have non-empty descriptions.

All local direct-relation view properties have explicit `source` blocks.

All local direct-relation container properties now have btree indexes.

All local direct-list relation properties now set `maxListSize: 300`, which keeps btree indexing valid for `direct[]`.

No local container exceeds 100 properties or 10 indexes.

No `usedFor: record` containers were found.

The module has a single local view implementing `cdf_cdm:CogniteAsset`: `Asset`.

CDM/IDM property mappings are mapped to CDM/IDM containers rather than duplicated into local containers.

### Remediation Applied

| File | Change |
|---|---|
| `data_models/containers/Equipment.container.yaml` | Added btree index for `equipmentClass`. |
| `data_models/containers/EquipmentClass.container.yaml` | Added `maxListSize: 300` and btree index for `failureModes`. |
| `data_models/containers/EquipmentType.container.yaml` | Added btree index for `class`. |
| `data_models/containers/FailureNotification.container.yaml` | Added btree indexes for `failureMode` and `failureMechanism`; corrected notification spelling in metadata. |
| `data_models/containers/Notification.container.yaml` | Added `maxListSize: 300` and btree indexes for `equipment`, `subunitFailed`, and `componentFailed`; corrected notification spelling in metadata. |
| `data_models/containers/FileExtension.container.yaml` | Corrected metadata typos in descriptions. |
| `data_models/containers/MaintenanceOrder.container.yaml` | Corrected notification spelling in metadata. |
| `data_models/views/FileExtension.view.yaml` | Corrected metadata typos in descriptions. |
| `data_models/views/FailureMode.view.yaml` | Corrected "Applicable" spelling in metadata. |
| `data_models/views/FailureNotification.view.yaml` | Corrected notification spelling in metadata. |
| `data_models/views/MaintenanceOrder.view.yaml` | Corrected notification spelling in metadata. |
| `data_models/views/Notification.view.yaml` | Corrected notification spelling in metadata. |

---

## C. Transformations

> No files found - skipping.

### [PASS] Passed

No transformation YAML or SQL files were present in this module.

---

## D. Functions

> No files found - skipping.

### [PASS] Passed

No CDF Function YAML, handler, or requirements files were present in this module.

---

## E. Workflows

> No files found - skipping.

### [PASS] Passed

No Workflow YAML files were present in this module.

---

## F. DMS Queries

> No files found - skipping.

### [PASS] Passed

No Python files with DMS API calls were present in this module.

---

## Next Steps

1. **[WARNING]** Decide whether this reusable module should adopt Toolkit variables for local space/version references, then update the module together with its deployment configuration.
2. **[WARNING]** Decide whether existing identifiers `rmdm` and `rmdm_v1` are intentionally retained as module conventions or should be migrated to the plugin naming convention in a new model/space rollout.
3. **[WARNING]** Add a `consumers:` section to `README.md` before making future breaking view-version changes.

---

*Share this file for alignment reviews. For deeper guidance trigger the matching specialist skill: `cdf-naming-check`, `cognite-data-modeling`, `cognite-transformation`, `cognite-function`, `cognite-workflow`, or `cognite-dms-queries`.*
