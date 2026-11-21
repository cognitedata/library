# CDF SharePoint Module (Industry) — document synthetic data

## Overview

This module provides synthetic SharePoint document data for the industry-neutral enterprise model `Industry_DOM`
(`datamodels/industry_domain_extension`). It is the Industry counterpart of `cdf_sharepoint_data_dump`: the sample
files and P&ID diagram annotations are copied from the CFIHOS sample so the two setups can be deployed side by side,
but everything is renamed (`db_industry_sample`, `tr_sharepoint_all_to_industry_*`) and written to the Industry
model.

The module loads document metadata into the `Document` view, creates the `CogniteFileCategory` nodes the documents
refer to, writes the diagram annotations as `CogniteDiagramAnnotation` edges and links documents to `Equipment`.

## Module Components

- `raw/` — database `db_industry_sample` with tables `file` and `diagram_annotation`.
- `upload_data/` — manifests and CSV rows for both tables, plus `file_content.Manifest.yaml` and the 11 sample PDFs in
  `data/` that upload the file content.
- `transformations/`
  - `tr_sharepoint_all_to_industry_file_category` — distinct `documentTypeCode` → `CogniteFileCategory`.
  - `tr_sharepoint_all_to_industry_document` — `file` ⋈ `diagram_annotation` → `Document`. Does not set
    `isUploaded` / `uploadedTime` (system-managed on `CogniteFile`); those are set when sample PDF content is uploaded
    via `file_content.Manifest.yaml`. `assets` is the distinct union of the file's own `asset_externalId` and every
    tag it is annotated with; this transformation is the single owner of `Document.assets`. Sample `currentRevision`
    values are revision tokens (e.g. `A`).
  - `tr_sharepoint_all_to_industry_diagram_annotation` — `diagram_annotation` → `CogniteDiagramAnnotation` edges.
    The edge, the start node (document) and the end node (Tag) all use `inst_location`.
  - `tr_sharepoint_all_to_industry_equipment_document_connection` — **overlay** on existing `Equipment` nodes
    (`cdf_nodes` join so stubs are never created): writes `Equipment.files` for documents annotated with the
    equipment's tag, with `ignoreNullFields: true`. Requires the SAP dump workflow first.

Document nodes use the external id `file_<file name>`, matching the file content manifest and the annotation edges.
Transformations are full loads with `upsert`; this is a sample-data choice. For real sources use incremental loads
(see the `cdf-transformation` skill).

## Deployment

### Prerequisites

- Cognite Toolkit ≥ 0.9 (sample-data upload uses `cdf data upload dir --cdf-project`).
- The `datamodels/industry_domain_extension` module deployed, so the model and the data set `ds_industry_all`
  exist.
- `sourcesystem/cdf_sap_industry_data_dump` deployed first: the equipment link reads its `equipment` table from
  `db_industry_sample`, and the `Tag` and `Equipment` nodes must exist before the annotation edges and the overlay run.
- Load order: `tr_sharepoint_all_to_industry_document` before `tr_sharepoint_all_to_industry_diagram_annotation`
  (edges need existing start and end nodes), and the equipment overlay last.

### Adding to an existing Toolkit project

Add `sourcesystem/cdf_sharepoint_industry_data_dump` to your module list. Defaults live in `default.config.yaml`;
override under `variables.modules.sourcesystem.cdf_sharepoint_industry_data_dump` in `config.<env>.yaml` only when
needed. Keep the shared identifiers (`dm_dom_industry`, `inst_location`, `db_industry_sample`, `ds_industry_all`)
in sync with the other `*_industry_*` modules.

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

Then upload this module's sample rows and PDF content (prepend your organization directory if `modules/` sits under one):

```bash
cdf data upload dir modules/sourcesystem/cdf_sharepoint_industry_data_dump/upload_data --cdf-project <project>
```

Run this module's workflow `wf_sharepoint_all_to_industry` (schedule never fires by default — start
it from the CDF UI). **Required:** run `wf_sap_all_to_industry` first so Tag and Equipment nodes
exist for annotation edges and the equipment–document overlay. Cross-module order is not enforced
by `dependsOn`; the overlay skips missing Equipment via `cdf_nodes`, but Tag links still need SAP.

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
├── transformations/
│   ├── tr_sharepoint_all_to_industry_file_category.Transformation.{yaml,sql}
│   ├── tr_sharepoint_all_to_industry_document.Transformation.{yaml,sql}
│   ├── tr_sharepoint_all_to_industry_diagram_annotation.Transformation.{yaml,sql}
│   └── tr_sharepoint_all_to_industry_equipment_document_connection.Transformation.{yaml,sql}
└── workflows/
    └── wf_sharepoint_all_to_industry.Workflow{,Version,Trigger}.yaml
```


## Support

For questions or issues, open an issue in the
[Cognite library repository](https://github.com/cognitedata/library) or reach out via Cognite Hub.
