# CDF SAP Module (Industry) — Tag, equipment, material and work management sample data

## Overview

Synthetic SAP sample data and the transformations that load it into the industry-neutral
`Industry_DOM` enterprise data model (`modules/datamodels/industry_domain_extension`).

The sample rows are copied from the CFIHOS SAP data dump (`cdf_sap_data_dump`) so both setups
describe the same plant, plus one new `material` table. Only the tables the generic model needs are
included — CFIHOS class tables (pump, valve, compressor, …), `functional_location`,
`maintenance_integrity` and `failure_mode` are intentionally not copied. All RAW rows live in
`raw_asset_all_sap` and every transformation id contains `industry`, so this module can be
deployed next to `cdf_sap_data_dump` without overwriting anything.

Loads are full loads with `conflictMode: upsert` and `ignoreNullFields: false`. This is a
sample-data choice; for incremental loads on a real source, follow the `cdf-transformation` skill
(`is_new()`).

## Module Components

- `raw/sap.Database.yaml` — the RAW database `raw_asset_all_sap`.
- `raw/*.Table.yaml` — RAW tables `tag`, `equipment`, `instrument_equipment`, `material`,
  `work_order`, `work_order_operation`, `notification`.
- `upload_data/` — a `Manifest.yaml` and `RawRows.csv` per table. `instrument_equipment` is not read by
  a transformation in this module; it is kept for the operating-limit load in `cdf_pi_industry_data_dump`.
- `transformations/` — eight transformations:

| Transformation | RAW table | Target view |
|----------------|-----------|-------------|
| `tr_sap_all_to_industry_tag` | `tag` | `Tag` (`type` → asset type when the tag has a class) |
| `tr_sap_all_to_industry_asset_type` | `tag` | `CogniteAssetType` (via `Industry_DOM`; `code` = `classId`, `name` = `className`) |
| `tr_sap_all_to_industry_equipment_type` | `equipment` | `CogniteEquipmentType` (via `Industry_DOM`) |
| `tr_sap_all_to_industry_equipment` | `equipment` | `Equipment` |
| `tr_sap_all_to_industry_material` | `material` | `Material` |
| `tr_sap_all_to_industry_work_order` | `work_order` | `WorkOrder` |
| `tr_sap_all_to_industry_work_order_operation` | `work_order_operation` | `WorkOrderOperation` |
| `tr_sap_all_to_industry_notification` | `notification` | `Notification` |

Direct relations use `node_reference('inst_location', key)` and are `NULL` when the source value is empty,
so the loads do not depend on each other and can run in parallel. `Tag.labels` is written as `AS labels`
(the view alias of `CogniteDescribable.tags`) and always contains `DetectInDiagrams`.

## Deployment

### Prerequisites

- Cognite Toolkit ≥ 0.9 (sample-data upload uses `cdf data upload dir --cdf-project`).
- The `industry_domain_extension` data model module deployed with the same identifiers:
  space `dm_dom_industry`, instance space `inst_location`, data model `Industry_DOM` `v1`,
  and data set `ds_industry_all`.
- An ingestion service principal with write access to the instance space, RAW and the data set.

### Adding to an existing Toolkit project

Add `sourcesystem/cdf_sap_industry_data_dump` to your package's module list. Defaults live in
`default.config.yaml`; override under `variables.modules.sourcesystem.cdf_sap_industry_data_dump`
in `config.<env>.yaml` only when needed. Keep the shared identifiers (`dm_dom_industry`,
`inst_location`, `raw_asset_all_sap`, `ds_industry_all`) in sync with the other `*_industry_*`
modules.

### Starting from scratch

```bash
cdf build -c config.dev.yaml
cdf deploy
```

### Upload sample RAW

Enable the data plugin in `cdf.toml` once:

```toml
[plugins]
data = true
```

Then upload this module's sample rows (prepend your organization directory if `modules/` sits under one):

```bash
cdf data upload dir modules/sourcesystem/cdf_sap_industry_data_dump/upload_data --cdf-project <project>
```

Then run this module's workflow `wf_sap_all_to_industry` (schedule never fires by default — start it
from the CDF UI). **Required first** in the pack run order (before PI and SharePoint); cross-module
order is not enforced by `dependsOn`. Expected row counts: 1154 tags (47 with an asset type), 17 asset
types, 10 equipment, 10 equipment types, 10 materials, 11 work orders, 17 operations and 10 notifications.

## Module Structure

```
cdf_sap_industry_data_dump/
├── module.toml
├── default.config.yaml
├── README.md
├── raw/
│   ├── sap.Database.yaml
│   └── <tag|equipment|instrument_equipment|material|work_order|work_order_operation|notification>.Table.yaml
├── upload_data/
│   └── <Manifest.yaml + RawRows.csv for each table above>
├── transformations/
│   └── <8 tr_sap_all_to_industry_*.Transformation.{yaml,sql} pairs>
└── workflows/
    └── wf_sap_all_to_industry.Workflow{,Version,Trigger}.yaml
```


## Support

For issues, questions, or contributions, open an issue or pull request in the
[cognitedata/library](https://github.com/cognitedata/library) repository.
