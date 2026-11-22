# Industry Domain Extension (Enterprise)

## Overview

Industry-neutral enterprise (`_DOM`) data model template for asset-heavy industries (process, manufacturing, utilities, energy, mining, infrastructure). Extends Cognite Core (CDM) and Process Industry (IDM) types with a single `Tag` asset hub, work-management views, label-only custom types (`Material`, `OperatingLimit`), and an `AlarmEvent` record stream for high-volume alarms.

**Layer:** Enterprise / domain (`Industry_DOM`). Companion search module: [`industry_domain_extension_search`](../industry_domain_extension_search/README.md) (schema-only `_SOL`).

**CDM / IDM versions:** CogniteCore `v1`, Process Industry `v1`.

**consumers:**

- `IndustrySearch_SOL` (`{{ dm_search_version }}`, default `v1`) — maps the enterprise **containers** only, so it is not pinned to an enterprise view version; container changes still affect it
- InField-shaped work views on `WorkOrder` / `Notification` / `WorkOrderOperation` (`{{ dm_enterprise_version }}`, default `v1`)
- Sample dump transformations (write through the enterprise views at `{{ dm_enterprise_version }}`) and workflows `wf_sap_all_to_industry`, `wf_pi_all_to_industry`, `wf_sharepoint_all_to_industry` (run SAP first)
- Alarm Function `fn_pi_all_ingest_alarm_event` — writes records through `AlarmEvent/{{ dm_enterprise_version }}`

## Module Components

| Area | Contents |
|------|----------|
| Spaces | `{{ enterprise_space }}` (schema); one instance space `{{ instance_space }}` (default `inst_location`); Search schema lives in companion module |
| Containers | Tag, Equipment, Document, TimeSeriesData, WorkOrder, WorkOrderOperation, Notification, Material, OperatingLimit (node); AlarmEvent (record). Every node view has its own container that `requires` its CDM/IDM parent |
| Views | Matching domain views (`implements` CDM/IDM where applicable) + `AlarmEvent` record view (`streamId`, not in the DataModel) |
| Data model | `Industry_DOM` + `CogniteAssetType` / `CogniteEquipmentType` / `CogniteFileCategory` |
| Stream | `{{ alarm_stream_external_id }}` (default template `BasicArchive`; override to `ImmutableTestStream` only in `config.dev.yaml`) |
| Auth | `admin_industry_all_*` (schema on enterprise + search), `producer_industry_dom_*`, pipeline `producer_industry_pp_*` / `producer_industry_wf_*` |
| Governance | Dataset + enterprise location filter |

### Naming note — `labels` vs `tags`

Because the CogniteAsset view is named `Tag`, CogniteDescribable `tags` is exposed as view property `labels` on `Tag` (and on `SearchTag`). Transformations must write `AS labels`, never `AS tags`.

### Label-only custom types — local describable fields

`Material` and `OperatingLimit` do **not** `implements` or `requires` `CogniteDescribable` (avoids the CDM Describable join on those nodes). They keep the same property *names* on their own containers where needed:

- `Material`: local `name`
- `OperatingLimit`: local `name`, `description`, and `aliases` (list of text, max 1000)

Do not also map those fields from `cdf_cdm:CogniteDescribable` on these views — that would reintroduce the join and dual-source the same concepts.

### One instance space (no Source schema layer)

This template has no `_SRC` model — transformations write RAW straight into `Industry_DOM`. Nodes, edges and alarm records all live in **one** instance space, `{{ instance_space }}` (default `inst_location`). Every direct, including links from PI or SharePoint data to a Tag, uses that same space.

There is **no search instance space**: Search is schema-only (views + data model in `{{ search_space }}`).

When the model is installed and extended, change or add instance spaces to match how the derived domain is used (a site, a plant, or an access boundary). Do not split spaces by source system. Update location filters, group scopes, transformation `instanceSpace`, and every `node_reference` together. Spaces are permanent — decide this before the first production deploy. The full step is in [SPEC.md](SPEC.md).

### Alarms vs Search access

`sol_consumer` has no `streamRecordsAcl`. Alarm access is via `{{ dom_producer_group_name }}` (or admin); Search UI via `{{ sol_consumer_group_name }}` (defined in [`industry_domain_extension_search`](../industry_domain_extension_search/default.config.yaml)). Map both IdP groups only when a user needs both products.

Sample two-step graph → records query: filter nodes on `Industry_DOM`, then filter `AlarmEvent` records with `in` on the `asset` direct (chunk ≤ 100 ids per request). Immutable streams require a `lastUpdatedTime` range on every filter.

### Access notes

These groups are **additive** to foundation personas (`admin_all_*`, `producer_all_*`, `consumer_all_*`). Do not delete the foundation groups when installing this pack, and do not treat `admin_industry_all_*` as a substitute for `admin_all_*`.

CDM time series and files need **both** `dataModelsAcl` READ on `cdf_cdm` and `dataModelInstancesAcl` READ on `{{ instance_space }}`. `admin`, `dom_producer`, and `sol_consumer` already grant that pair.

Security categories (`sc_*`) apply to classic time series and files only. They do not gate data-modeling instances. Tag and Search access stays space-scoped.

Out of the box this pack does **not** add a `_SRC` schema layer, per-source human admin groups, or extraction-pipeline ACLs on `producer_industry_pp_*` / `producer_industry_wf_*`. Those are delivery adaptations, not template fixes.

### Stable contract vs scaffolding

| Kind | Keep |
|------|------|
| CONTRACT | `implements`, typed `source`, DataModel membership, Search as schema only |
| SOURCE_SCAFFOLD | Demo properties (`site`, `documentNumber`, `piSourceTag`, …) — rename or delete before the first production deploy |
| RECORD | `AlarmEvent` `usedFor: record`, no constraints or indexes, record view with `streamId` |

### Known gaps in the sample

- **Asset type covers only classified tags.** `tr_sap_all_to_industry_asset_type` creates one `CogniteAssetType` node per SAP tag class (`code` = `classId`, `name` = `className`), and the Tag load writes `type` for the 47 sample tags that have a class name; the other tags have no type. The class lives only on the type node; `Tag` has no class text property.
- **OPC UA properties are scaffold only.** `opcUaNodeId` / `opcUaBrowseName` on `TimeSeriesData` are not written by the sample; trim them if no OPC UA source exists.

### How to extend

1. Inventory `SOURCE_SCAFFOLD:` properties and the demo RAW modules.
2. Keep `inst_location` unless the derived domain needs more instance spaces. Add or rename spaces for a site, plant, or access boundary — not per source system.
3. Map each real source field to a view property (`keep` / `replace` / `drop`) and confirm the table before editing YAML.
4. Remove unused types before the first production deploy.
5. Point transformations at real RAW. Drop the `*_industry_data_dump` modules once real extractors write the same nodes.
6. Add missing fields on the existing enterprise containers only while still pre-production. After the first production deploy, add a new extension container.
7. Keep Search views in step with every container you keep, rename, or remove.

Full procedure, touch map, and rename recipes: [SPEC.md](SPEC.md).

### Stream template warning

Module default is `BasicArchive`. `ImmutableTestStream` is for sample/dev only (short retention) and is set in `config.dev.yaml`. The stream template **cannot be changed** after creation — do not create the stream with `ImmutableTestStream` if you need production retention.

### Querying — cursor reads and indexes

Always scope with `space = {{ instance_space }}`. Prefer `/sync` over unbounded `/list` or `/query` for large volumes. Do not filter or cursor on `lastUpdatedTime` / `createdTime` for DMS instance reads — use `/sync` for change tracking.

**Read all of one view (backfill + live changes)**

1. Filter with `hasData` on the view (and `space`).
2. Use `/sync` with `mode: twoPhase` so the backfill can use an index.
3. Set `backfillSort` to match a **cursorable** btree (property order and direction must match the index). Live sync does not support custom sort.
4. Page with the returned cursor (limit ~500–1000). Sync cursors expire after ~3 days.

If you only filter by a single space (no `hasData`), `mode: onePhase` is enough — built-in cursorable indexes on `space` / `externalId` cover that path.

**Filter vs cursor**

| Index kind | Use for |
|------------|---------|
| `cursorable: true` | Full-type sync backfill (`backfillSort`), `/list` or `/query` sort + cursor at scale |
| `cursorable: false` (btree) | Equality / range filters only — not for ordered pagination |
| CDM `CogniteSchedulable` times | Cursor on WorkOrder / Notification / WorkOrderOperation when you prefer CDM timestamps |
| CDM `asset` / `assets` | On Equipment / TimeSeriesData / File — stored in `cdf_cdm`; cannot add enterprise indexes there |

**Per-view guidance**

| View | Cursor for “all of type” (`backfillSort`) | Filter with enterprise btree |
|------|-------------------------------------------|------------------------------|
| **Tag** | `site` (cursorable) | `site`, `area`, `system` |
| **Equipment** | No enterprise cursorable index — use `space` / `externalId`. `asset` lives on `cdf_cdm:CogniteEquipment` (immutable). Optional weak fallback: filter/sort on `modelNumber` (not cursorable). | `modelNumber` |
| **Document** | `documentNumber` (cursorable) | `documentNumber`, `site`, `area`, `system` |
| **TimeSeriesData** | No enterprise cursorable on asset — `assets` is a CDM list on `CogniteTimeSeries`. Use `space` / `externalId`, or filter on `piSourceTag` / `timeSeriesSource` / `opcUaNodeId` (not cursorable). | `piSourceTag`, `timeSeriesSource`, `opcUaNodeId` |
| **WorkOrder** | `workOrderNumber` (cursorable), or CDM Schedulable `startTime` / `scheduledStartTime` | `workOrderNumber` |
| **WorkOrderOperation** | CDM Schedulable times or `space` / `externalId` | `workCenter` |
| **Notification** | Prefer CDM Schedulable times or `space` / `externalId` | `failureModeCode`, `detectionMethodCode` |
| **Material** | `equipment` or `asset` (both cursorable); pick the relation you always populate | `equipment`, `asset` |
| **OperatingLimit** | `timeSeries` (cursorable) | `timeSeries` |
| **AlarmEvent** | Records — no container indexes. Query the stream with a `lastUpdatedTime` range; chunk `in` filters on `asset` (≤ 100 ids). | — |

Example: sync all Tags with `hasData`, backfill via the cursorable `site` index (`POST /models/instances/sync`):

```yaml
with:
  tags:
    nodes:
      filter:
        and:
          - equals: { property: [node, space], value: "{{ instance_space }}" }
          - hasData:
              - type: view
                space: "{{ enterprise_space }}"
                externalId: Tag
                version: "{{ dm_enterprise_version }}"
    limit: 1000
    mode: twoPhase
    backfillSort:
      - property: [ "{{ enterprise_space }}", Tag, site ]
        direction: ascending
        nullsFirst: false
select:
  tags:
    sources:
      - source:
          type: view
          space: "{{ enterprise_space }}"
          externalId: Tag
          version: "{{ dm_enterprise_version }}"
        properties: [name, site, area, system]
```

Follow `nextCursor` until a page returns fewer than `limit` rows (backfill done), then keep following for live creates/deletes. Same pattern for other views: swap the view id and set `backfillSort` to that view’s cursorable property from the table above.

For `/query` pagination, the `sort` clause must match the cursorable index (e.g. Document: sort by `documentNumber` ascending, nulls last). Mismatched sort triggers `sortNotBackedByIndex`.

## Deployment

### Prerequisites

- Cognite Toolkit project with valid CDF credentials
- Toolkit ≥ 0.9 (record view `streamId` support)

### Add the module

```bash
cdf modules add
# select dp:models → industry_domain_extension (and ideally industry_domain_extension_search)
```

Override variables under `variables.modules` in `config.<env>.yaml` (see `default.config.yaml`). Deploy enterprise before search and before the sample dumps (see [Sample data](#sample-data)).

### Validation

```bash
uv run cdf build --config-yaml <org_dir>/config.dev.yaml
uv run cdf deploy --dry-run
python validate_packages.py
```

## Sample data

This module defines schema only. Sample RAW, transformations and load workflows live in the companion dumps
(or install the whole pack with `dp:industry_quickstart`). All three dumps share one RAW database, `db_industry_sample`
(`raw_database_sap` / `raw_database_pi` / `raw_database_sharepoint` all default to it). That name is sample-only
and does not follow the `raw_{data_type}_{location}_{source}` convention — use convention names for real RAW.

| Module | Workflow | Role |
|--------|----------|------|
| [`cdf_sap_industry_data_dump`](../../sourcesystem/cdf_sap_industry_data_dump/README.md) | `wf_sap_all_to_industry` | Tag, Equipment, Material, work management |
| [`cdf_pi_industry_data_dump`](../../sourcesystem/cdf_pi_industry_data_dump/README.md) | `wf_pi_all_to_industry` | Time series, operating limits, alarm records |
| [`cdf_sharepoint_industry_data_dump`](../../sourcesystem/cdf_sharepoint_industry_data_dump/README.md) | `wf_sharepoint_all_to_industry` | Documents, diagram annotations, equipment–file links |

### Deploy order

1. `industry_domain_extension` (this module — schema, stream, `pp`/`wf` groups)
2. `industry_domain_extension_search`
3. `cdf_sap_industry_data_dump`, then `cdf_pi_industry_data_dump` and `cdf_sharepoint_industry_data_dump`

### Env vars

Set these in `.env` (mapped into `default.config.yaml` / `config.<env>.yaml`):

```bash
IDP_CLIENT_ID=<service principal client id>           # dump workflow triggers + transformations
IDP_CLIENT_SECRET=<service principal client secret>
INGESTION_GROUP_SOURCE_ID=<IdP group for producer_industry_pp_*>
OPERATOR_GROUP_SOURCE_ID=<IdP group for producer_industry_wf_*>
ADMIN_GROUP_SOURCE_ID=<IdP group for admin_industry_all_*>
DOM_PRODUCER_GROUP_SOURCE_ID=<IdP group for producer_industry_dom_*>
```

Map the SP used by `IDP_CLIENT_ID` into the ingestion (`pp`) group. Search UI also needs
`SOL_CONSUMER_GROUP_SOURCE_ID` on the search module.

### Upload sample RAW

Requires Cognite Toolkit ≥ 0.9. Enable the data plugin in `cdf.toml` once:

```toml
[plugins]
data = true
```

Then upload each dump's `upload_data/` (prepend your organization directory if `modules/` sits under one):

```bash
cdf data upload dir modules/sourcesystem/cdf_sap_industry_data_dump/upload_data --cdf-project <project>
cdf data upload dir modules/sourcesystem/cdf_pi_industry_data_dump/upload_data --cdf-project <project>
cdf data upload dir modules/sourcesystem/cdf_sharepoint_industry_data_dump/upload_data --cdf-project <project>
```

### Run order

Schedules never fire by default (`0 0 29 2 *`). After deploy and upload, start the workflows from the CDF UI.
**Cross-module order is mandatory and not enforced by workflow `dependsOn`:**

1. `wf_sap_all_to_industry` **first** (Tags / Equipment must exist)
2. `wf_pi_all_to_industry` and `wf_sharepoint_all_to_industry` (either order)

## Module Structure

```
industry_domain_extension/
  module.toml
  default.config.yaml
  README.md
  SPEC.md                         # full agent adaptation contract
  auth/                           # admin, dom_producer, ingestion (pp), operator (wf)
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

Follow *How to extend* above, then fill the mapping worksheet in [SPEC.md](SPEC.md). `SOURCE_SCAFFOLD:` properties are the demo surface to replace before the first production deploy. After that, container properties cannot be removed.

Grep scaffolds:

```bash
rg -n "SOURCE_SCAFFOLD:" modules/datamodels/industry_domain_extension
```

## Support

- Cognite Hub and Cognite Docs (Core data model, Process industry data model, Records and streams)
- Adaptation contract: [SPEC.md](SPEC.md) in this folder
