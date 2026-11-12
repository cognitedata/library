# CDF SharePoint Module (Industry) — document synthetic data

## Overview

This module provides synthetic SharePoint document data for the industry-neutral enterprise model `Industry_DOM`
(`datamodels/industry_domain_extension`). It is the Industry counterpart of `cdf_sharepoint_data_dump`: the sample
files and P&ID diagram annotations are copied from the CFIHOS sample so the two setups can be deployed side by side,
but everything is renamed (`raw_sample_all_sharepoint`, `tr_sharepoint_all_to_industry_*`) and written to the Industry
model.

The module loads document metadata into the `Document` view, creates the `CogniteFileCategory` nodes the documents
refer to, writes the diagram annotations as `CogniteDiagramAnnotation` edges and links documents to `Equipment`.

## Module Components

- `raw/` — database `raw_database_sharepoint` with tables `file` and `diagram_annotation`.
- `upload_data/` — manifests and CSV rows for both tables, plus `file_content.Manifest.yaml` and the 11 sample PDFs in
  `data/` that upload the file content.
- `transformations/`
  - `tr_sharepoint_all_to_industry_file_category` — distinct `documentTypeCode` → `CogniteFileCategory`.
  - `tr_sharepoint_all_to_industry_document` — `file` ⋈ `diagram_annotation` → `Document`. Sets `isUploaded` and
    `uploadedTime` (the issue date) so the file syncer and Search treat the sample PDFs as uploaded. `assets` is the
    distinct union of the file's own `asset_externalId` and every tag it is annotated with; this transformation is the
    single owner of `Document.assets`. `currentRevision` from the sample is carried into `revision` as is.
  - `tr_sharepoint_all_to_industry_diagram_annotation` — `diagram_annotation` → `CogniteDiagramAnnotation` edges. The
    edge, start and end node spaces all come from `instance_space`; the space columns in the CSV are ignored.
  - `tr_sharepoint_all_to_industry_equipment_document_connection` — **overlay** on `Equipment` nodes created by the
    SAP module: writes `Equipment.files` for documents annotated with the equipment's tag, with
    `ignoreNullFields: true`.

Document nodes use the external id `file_<file name>`, matching the file content manifest and the annotation edges.
Transformations are full loads with `upsert`; this is a sample-data choice. For real sources use incremental loads
(see the `cdf-transformation` skill).

## Deployment

### Prerequisites

- Cognite Toolkit.
- The `datamodels/industry_domain_extension` module deployed, so the model and the data set `dataset_external_id`
  exist.
- `sourcesystem/cdf_sap_industry_data_dump` deployed first: the equipment link reads its `equipment` table from
  `raw_database_sap`, and the `Tag` and `Equipment` nodes must exist before the annotation edges and the overlay run.
- Load order: `tr_sharepoint_all_to_industry_document` before `tr_sharepoint_all_to_industry_diagram_annotation`
  (edges need existing start and end nodes), and the equipment overlay last.

### Adding to an existing Toolkit project

Add `sourcesystem/cdf_sharepoint_industry_data_dump` to your module list and set the variables under
`variables.modules.cdf_sharepoint_industry_data_dump` in `config.<env>.yaml`. The variables are shared with the other
`*_industry_*` modules and must have the same values everywhere. `ingestion_client_id` and `ingestion_client_secret`
come from `${IDP_CLIENT_ID}` and `${IDP_CLIENT_SECRET}`.

### Starting from scratch

```bash
cdf build --env <your-env>
cdf deploy --env <your-env>
```

Run the transformations through the `common/cdf_industry_ingestion` workflow.

## Module Structure

```
cdf_sharepoint_industry_data_dump/
├── module.toml
├── default.config.yaml
├── README.md
├── raw/
│   ├── sharepoint.Database.yaml
│   ├── file.Table.yaml
│   └── diagram_annotation.Table.yaml
├── upload_data/
│   ├── file.Manifest.yaml
│   ├── file.RawRows.csv
│   ├── diagram_annotation.Manifest.yaml
│   ├── diagram_annotation.RawRows.csv
│   ├── file_content.Manifest.yaml
│   └── data/                      # 11 sample PDFs
└── transformations/
    ├── tr_sharepoint_all_to_industry_file_category.Transformation.{yaml,sql}
    ├── tr_sharepoint_all_to_industry_document.Transformation.{yaml,sql}
    ├── tr_sharepoint_all_to_industry_diagram_annotation.Transformation.{yaml,sql}
    └── tr_sharepoint_all_to_industry_equipment_document_connection.Transformation.{yaml,sql}
```

## Support

For questions or issues, open an issue in the
[Cognite library repository](https://github.com/cognitedata/library) or reach out via Cognite Hub.
