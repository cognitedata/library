# Industry Domain Extension Search (Solution)

## Overview

Schema-only search (`_SOL`) data product for the industry domain template. Maps enterprise / CDM / IDM containers into Search-oriented views and hosts reverse directs. **No containers, no transformations, no instance writes, no search instance space.**

**Layer:** Solution / search (`IndustrySearch_SOL`). Enterprise companion: [`industry_domain_extension`](../industry_domain_extension/README.md).

**CDM / IDM versions:** CogniteCore `v1`, Process Industry `v1` (mapped, never `implements`).

**consumers:**

- Cognite Search UI
- Apps that navigate reverses on `SearchTag` / `SearchWorkOrder` / `SearchEquipment` / `SearchTimeSeriesData`

> **Do not copy CFIHOS `_search` `implements`.** CFIHOS search views still `implements` CDM/IDM — that is legacy. This module maps containers only (classification minimum + Search UX Describable maps per type).

## Module Components

| Area | Contents |
|------|----------|
| Space | `{{ search_space }}` — schema only (views + data model); **not** an instance space |
| Views | SearchTag, SearchEquipment, SearchDocument, SearchTimeSeriesData, SearchWorkOrder, SearchWorkOrderOperation, SearchNotification, SearchMaterial, SearchOperatingLimit |
| Data model | `IndustrySearch_SOL` + `CogniteEquipmentType` / `CogniteFileCategory` |
| Auth | `consumer_industry_sol_*` (schema admin lives in the enterprise module as `admin_industry_all_*`) |
| Location | Search location filter over `{{ instance_space }}` (`inst_location`) |

There is **no** search view for `AlarmEvent` — read alarms through the enterprise record view and Records API (`producer_industry_dom_*` or admin). Search UI users use `{{ sol_consumer_group_name }}`; map both IdP groups only when a user needs Search and alarms.

Search consumers need **both** `dataModelsAcl` READ on `cdf_cdm` and `dataModelInstancesAcl` READ on `{{ instance_space }}`. `{{ sol_consumer_group_name }}` already grants that pair. Security categories (`sc_*`) do not gate these instances.

`consumer_industry_sol_*` is an add-on to foundation `consumer_all_*`, not a replacement.

### Naming note — `labels` vs `tags`

`SearchTag` exposes CogniteDescribable `tags` as view property `labels` (same alias as enterprise `Tag`).

### No search instance space

Search holds views and the data model only. Instances live in `{{ instance_space }}` (default `inst_location`), owned with the enterprise module. Nothing would belong in a separate search instance space — Search views map enterprise / CDM / IDM containers; once enterprise loads finish, data is visible through `IndustrySearch_SOL` with no search-side writes. When the domain is extended, point this location filter at the same instance spaces the enterprise module uses.

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

## Adapting this template

Search stays schema-only: update or remove `Search*` views when the enterprise containers change, and do not add containers, transformations, or a search instance space. Procedure and the stable-contract table: [SPEC.md](../industry_domain_extension/SPEC.md).

## Support

- Cognite Hub and Cognite Docs
- Plan source: `../industry_domain_extension/industry_domain_extension.PLAN.md`
