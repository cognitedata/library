# CDF SharePoint Module (CFIHOS) — Document synthetic data

## Overview

This module provides CFIHOS-shaped synthetic document data and the transformations that populate
the `Files` view and CFIHOS diagram-annotation edges of the CFIHOS Oil & Gas domain model
(`cfihos_oil_and_gas_extension`). It ships CFIHOS-shaped file metadata and P&ID diagram annotation 
records instead of the SharePoint-style sample data.

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

- Cognite Toolkit.
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
