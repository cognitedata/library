# CDF SAP Module (Industry) — Tag, equipment, material and work management sample data

## Overview

Synthetic SAP sample data and the transformations that load it into the industry-neutral
`Industry_DOM` enterprise data model (`modules/datamodels/industry_domain_extension`).

The sample rows are copied from the CFIHOS SAP data dump (`cdf_sap_data_dump`) so both setups
describe the same plant, plus one new `material` table. Only the tables the generic model needs are
included — CFIHOS class tables (pump, valve, compressor, …), `functional_location`,
`maintenance_integrity` and `failure_mode` are intentionally not copied. All RAW rows live in a
separate database (`{{ raw_database_sap }}`) and every transformation id contains `industry`, so this
module can be deployed next to `cdf_sap_data_dump` without overwriting anything.

Loads are full loads with `conflictMode: upsert` and `ignoreNullFields: false`. This is a
sample-data choice; for incremental loads on a real source, follow the `cdf-transformation` skill
(`is_new()`).

## Module Components

- `raw/sap.Database.yaml` — the RAW database `{{ raw_database_sap }}`.
- `raw/*.Table.yaml` — RAW tables `tag`, `equipment`, `instrument_equipment`, `material`,
  `work_order`, `work_order_operation`, `notification`.
- `upload_data/` — a `Manifest.yaml` and `RawRows.csv` per table. `instrument_equipment` is not read by
  a transformation in this module; it is kept for the operating-limit load in `cdf_pi_industry_data_dump`.
- `transformations/` — seven transformations:

| Transformation | RAW table | Target view |
|----------------|-----------|-------------|
| `tr_sap_all_to_industry_tag` | `tag` | `Tag` |
| `tr_sap_all_to_industry_equipment_type` | `equipment` | `cdf_cdm:CogniteEquipmentType` |
| `tr_sap_all_to_industry_equipment` | `equipment` | `Equipment` |
| `tr_sap_all_to_industry_material` | `material` | `Material` |
| `tr_sap_all_to_industry_work_order` | `work_order` | `WorkOrder` |
| `tr_sap_all_to_industry_work_order_operation` | `work_order_operation` | `WorkOrderOperation` |
| `tr_sap_all_to_industry_notification` | `notification` | `Notification` |

Direct relations use `node_reference('{{ instance_space }}', key)` and are `NULL` when the source value is empty,
so the loads do not depend on each other and can run in parallel. `Tag.labels` is written as `AS labels`
(the view alias of `CogniteDescribable.tags`) and always contains `DetectInDiagrams`.

## Deployment

### Prerequisites

- Cognite Toolkit.
- The `industry_domain_extension` data model module deployed with the same `enterprise_space`,
  `instance_space`, `enterprise_data_model_external_id` and `dm_enterprise_version`.
- The data set `{{ dataset_external_id }}` and an ingestion service principal with write access to the
  instance space, RAW and the data set.

### Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `enterprise_space` | `dm_dom_industry` | Space of the enterprise views |
| `instance_space` | `inst_industry` | Space the nodes are written to |
| `enterprise_data_model_external_id` | `Industry_DOM` | Target data model |
| `dm_enterprise_version` | `v1` | Data model and view version |
| `dataset_external_id` | `ds_industry_all` | Data set for the transformations |
| `raw_database_sap` | `raw_sample_all_sap` | RAW database |
| `ingestion_client_id` / `ingestion_client_secret` | `${IDP_CLIENT_ID}` / `${IDP_CLIENT_SECRET}` | Transformation credentials |

### Adding to an existing Toolkit project

Add `sourcesystem/cdf_sap_industry_data_dump` to your package's module list and configure the variables
under `variables.modules.cdf_sap_industry_data_dump` in your `config.<env>.yaml`.

### Starting from scratch

```bash
cdf build --env <your-env>
cdf deploy --env <your-env>
```

Then run the transformations (or the `cdf_industry_ingestion` workflow, when deployed). Expected row
counts: 1154 tags, 10 equipment, 10 equipment types, 10 materials, 11 work orders, 17 operations and
10 notifications.

## Module Structure

```
cdf_sap_industry_data_dump/
├── module.toml
├── default.config.yaml
├── raw/
│   ├── sap.Database.yaml
│   └── <tag|equipment|instrument_equipment|material|work_order|work_order_operation|notification>.Table.yaml
├── upload_data/
│   └── <Manifest.yaml + RawRows.csv for each table above>
└── transformations/
    └── <7 tr_sap_all_to_industry_*.Transformation.{yaml,sql} pairs>
```

## Support

For issues, questions, or contributions, open an issue or pull request in the
[cognitedata/library](https://github.com/cognitedata/library) repository.
