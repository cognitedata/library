# CDF SharePoint Module (CFIHOS) — Document synthetic data

## Overview

This module provides CFIHOS-shaped synthetic document data and the transformations that populate
the `Files` view and CFIHOS diagram-annotation edges of the CFIHOS Oil & Gas domain model
(`cfihos_oil_and_gas_extension`). It is the CFIHOS counterpart of `cdf_sharepoint`: it ships
CFIHOS-shaped file metadata and P&ID diagram annotation records instead of the SharePoint-style
sample data.

Use this module when deploying `dp:quickstart` against the `cfihos_oil_and_gas_extension` data
model. It is not a replacement for `cdf_sharepoint` in other deployment packs — `cdf_sharepoint`
remains the correct choice for `dp:foundation`.

> **Diagram-annotation cleanup:** the `diagram_annotation.*` RAW data, its 3 transformation
> pairs, and the standalone `wf_diagram_annotation.*` workflow work on their own — this module
> doesn't require `contextualization/cdf_file_annotation` to produce annotation edges. But
> `dp:quickstart` installs both, and running both together would write competing
> `CogniteDiagramAnnotation` edges. `common/cdf_project_foundation`'s setup wizard
> (`setup_project.py`) detects this and automatically removes the diagram-annotation files
> from your project (and the matching tasks from `common/cdf_ingestion`'s workflow) the first
> time it runs — nothing is removed from the library module itself, and a project using only
> this module keeps the synthetic pipeline unchanged. `tr_file_all_to_file` (populates `Files`)
> is never removed by either path.

## Module Components

- `raw/file.Table.yaml`, `diagram_annotation.Table.yaml` — RAW table definitions.
- `upload_data/RAW/` — matching manifests, synthetic CSV rows, and a `resources/` folder used by
  the file manifest.
- `transformations/` — 4 transformations: `Files` population, `CogniteDiagramAnnotation`
  population (written to the base `cdf_cdm`/`CogniteCore` space), and the diagram-annotation
  connections that link annotated files to `Equipment` (owned by `cdf_sap_assets_new`) and `Tag`
  nodes.

## Deployment

### Prerequisites

- Cognite Toolkit (minimum version as required by `dp:quickstart`).
- The `cfihos_oil_and_gas_extension` data model module deployed to the same project/space.
- `cdf_sap_assets_new` deployed first (or in the same run) so the `Equipment` and `Tag` nodes
  referenced by the diagram-annotation connection transformations exist.

### Adding to an existing Toolkit project

Add `sourcesystem/cdf_sharepoint_new` to your package's module list and configure the variables
under `variables.modules.cdf_sharepoint_new` in your `config.<env>.yaml`.

### Starting from scratch

```bash
cdf-tk build --env <your-env>
cdf-tk deploy --env <your-env>
```

## Module Structure

```
cdf_sharepoint_new/
├── module.toml
├── default.config.yaml
├── raw/
│   ├── file.Table.yaml
│   └── diagram_annotation.Table.yaml
├── upload_data/
│   └── RAW/
│       ├── file.Manifest.yaml
│       ├── file.RawRows.csv
│       ├── diagram_annotation.Manifest.yaml
│       ├── diagram_annotation.RawRows.csv
│       └── resources/
└── transformations/
    └── <4 tr_*.Transformation.{yaml,sql} pairs>
```

## Support

For questions or issues, open an issue in the
[Cognite library repository](https://github.com/cognitedata/library) or reach out via Cognite
Hub.
