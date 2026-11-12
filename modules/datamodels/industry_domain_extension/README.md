# Industry Domain Extension (Enterprise)

## Overview

Industry-neutral enterprise (`_DOM`) data model template for asset-heavy industries (process, manufacturing, utilities, energy, mining, infrastructure). Extends Cognite Core (CDM) and Process Industry (IDM) types with a single `Tag` asset hub, work-management views, label-only custom types (`Material`, `OperatingLimit`), and an `AlarmEvent` record stream for high-volume alarms.

**Layer:** Enterprise / domain (`Industry_DOM`). Companion search module: [`industry_domain_extension_search`](../industry_domain_extension_search/README.md) (schema-only `_SOL`).

**CDM / IDM versions:** CogniteCore `v1`, Process Industry `v1`.

**consumers:**

- Search UI via `IndustrySearch_SOL` (pinned to `{{ dm_search_version }}`)
- InField-shaped work views on `WorkOrder` / `Notification` / `WorkOrderOperation`
- Sample workflow `wf_all_industry_ingestion`

## Module Components

| Area | Contents |
|------|----------|
| Spaces | `{{ enterprise_space }}` (schema), `{{ instance_space }}` (instances + records) |
| Containers | Tag, Equipment, Document, TimeSeriesData, WorkOrder, Notification, Material, OperatingLimit (node); AlarmEvent (record) |
| Views | Matching domain views (`implements` CDM/IDM where applicable) + `AlarmEvent` record view (`streamId`, not in the DataModel) |
| Data model | `Industry_DOM` + `CogniteEquipmentType` / `CogniteFileCategory` |
| Stream | `{{ alarm_stream_external_id }}` (default template `ImmutableTestStream` — **dev only**) |
| Auth | `admin_industry_dom_*`, `consumer_industry_dom_*` |
| Governance | Dataset + enterprise location filter |

### Naming note — `labels` vs `tags`

Because the CogniteAsset view is named `Tag`, CogniteDescribable `tags` is exposed as view property `labels` on `Tag` (and on `SearchTag`). Transformations must write `AS labels`, never `AS tags`.

### Single instance space (documented exception)

This template has no `_SRC` model — transformations write RAW straight into `Industry_DOM` — so one instance space (`inst_industry`) holds all enterprise instances, type nodes and alarm records. A project that adds a `_SRC` model must give it its own instance space. Use per-site instance spaces for site-level access control.

### Alarms + Search dual membership

`sol_consumer` has no `streamRecordsAcl`. Apps that need Search **and** alarms must map the IdP user to both `{{ sol_consumer_group_name }}` and `{{ dom_consumer_group_name }}`.

Sample two-step graph → records query: filter nodes on `Industry_DOM`, then filter `AlarmEvent` records with `in` on the `asset` direct (chunk ≤ 100 ids per request). Immutable streams require a `lastUpdatedTime` range on every filter.

### Stream template warning

`ImmutableTestStream` is for sample/dev only (short retention). The stream template **cannot be changed** after creation — set `BasicArchive` in `config.<env>.yaml` **before** the first non-dev deploy.

## Deployment

### Prerequisites

- Cognite Toolkit project with valid CDF credentials
- Toolkit ≥ 0.9 (record view `streamId` support)

### Add the module

```bash
cdf modules add
# select dp:models → industry_domain_extension (and ideally industry_domain_extension_search)
```

Override variables under `variables.modules` in `config.<env>.yaml` (see `default.config.yaml`). Deploy enterprise before search and before the `*_industry_data_dump` / `cdf_industry_ingestion` companions.

### Validation

```bash
uv run cdf build --config-yaml <org_dir>/config.dev.yaml
uv run cdf deploy --dry-run
python validate_packages.py
```

## Module Structure

```
industry_domain_extension/
  module.toml
  default.config.yaml
  README.md
  SPEC.md                         # full agent adaptation contract
  auth/
  data_sets/
  locations/
  streams/
  data_modeling/
    enterprise.Space.yaml
    instance.Space.yaml
    enterprise.DataModel.yaml
    containers/
    views/
```

## Adapting this template

See [SPEC.md](SPEC.md) for the full agent contract (How to extend, stable vs scaffolding, mapping worksheet, trim/rename recipes). Until sample-data companions land, treat `SOURCE_SCAFFOLD:` properties as the demo surface to replace before first production deploy.

Grep scaffolds:

```bash
rg -n "SOURCE_SCAFFOLD:" modules/datamodels/industry_domain_extension
```

## Support

- Cognite Hub and Cognite Docs (Core data model, Process industry data model, Records and streams)
- Plan source: `industry_domain_extension.PLAN.md` in this folder
