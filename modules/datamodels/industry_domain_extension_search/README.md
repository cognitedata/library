# Industry Domain Extension Search (Solution)

## Overview

Schema-only search (`_SOL`) data product for the industry domain template. Maps enterprise / CDM / IDM containers into Search-oriented views and hosts reverse directs. **No containers, no transformations, no instance writes.**

**Layer:** Solution / search (`IndustrySearch_SOL`). Enterprise companion: [`industry_domain_extension`](../industry_domain_extension/README.md).

**CDM / IDM versions:** CogniteCore `v1`, Process Industry `v1` (mapped, never `implements`).

**consumers:**

- Cognite Search UI
- Apps that navigate reverses on `SearchTag` / `SearchWorkOrder` / `SearchEquipment` / `SearchTimeSeriesData`

> **Do not copy CFIHOS `_search` `implements`.** CFIHOS search views still `implements` CDM/IDM — that is legacy. This module maps containers only (classification minimum + Search UX Describable maps per type).

## Module Components

| Area | Contents |
|------|----------|
| Space | `{{ search_space }}` (views + data model only) |
| Views | SearchTag, SearchEquipment, SearchDocument, SearchTimeSeriesData, SearchWorkOrder, SearchWorkOrderOperation, SearchNotification, SearchMaterial, SearchOperatingLimit |
| Data model | `IndustrySearch_SOL` + `CogniteEquipmentType` / `CogniteFileCategory` |
| Auth | `admin_industry_sol_*`, `consumer_industry_sol_*` |
| Location | Search location filter over `{{ instance_space }}` |

There is **no** search view for `AlarmEvent` — consumers read alarms through the enterprise record view and Records API. For Search + alarms, use dual membership with `dom_consumer` (see enterprise README).

### Naming note — `labels` vs `tags`

`SearchTag` exposes CogniteDescribable `tags` as view property `labels` (same alias as enterprise `Tag`).

### Single instance space

Instances live in the shared `{{ instance_space }}` owned with the enterprise module (documented exception — see enterprise README).

## Deployment

### Prerequisites

- Deploy `industry_domain_extension` first
- Toolkit ≥ 0.9

```bash
cdf modules add
# select dp:models → industry_domain_extension_search
```

## Module Structure

```
industry_domain_extension_search/
  module.toml
  default.config.yaml
  README.md
  auth/
  locations/
  data_modeling/
    search.Space.yaml
    search.DataModel.yaml
    views/          # no containers/
```

## Support

- Cognite Hub and Cognite Docs
- Plan source: `../industry_domain_extension.PLAN.md`
