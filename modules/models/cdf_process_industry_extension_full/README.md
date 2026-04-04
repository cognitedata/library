# CDF Process Industry Extension Full Module

This module provides a comprehensive extension of the Cognite Process Industry data model, adding organization-specific views and containers for a broad set of enterprise entities.

## Overview

The `cdf_process_industry_extension_full` module includes:

- A schema space for the model definitions
- Organization-prefixed containers and views
- A combined enterprise data model (`{{ organization }}ProcessIndustries`)
- Coverage for core process industry entities and 3D/360 entities

Compared to the previous `cdf_process_industry_extension` module, this module follows the same extension pattern but includes a significantly larger entity surface.

## Module Structure

```text
cdf_process_industry_extension_full/
|- data_models/
|  |- containers/
|  |- views/
|  |- enterprise.datamodel.yaml
|  `- schema.space.yaml
|- default.config.yaml
|- module.toml
`- README.md
```

## Configuration

The module uses these variables from `default.config.yaml`:

```yaml
organization: ORG
schemaSpace: sp_enterprise_process_industry
datamodelVersion: v1.0
```

In your environment config:

```yaml
variables:
  modules:
    cdf_process_industry_extension_full:
      organization: YOUR_ORG
      schemaSpace: sp_enterprise_process_industry
      datamodelVersion: v1.0
```

## Key Notes

- The data model external ID is `{{ organization }}ProcessIndustries`.
- Views are defined in your schema space (`{{ schemaSpace }}`) and versioned by `{{ datamodelVersion }}`.
- This module is intended as a full enterprise extension model for process industry use cases.

## Deployment

Deploy with toolkit as part of your selected package/environment:

```bash
cdf deploy --env <environment>
```

Then verify that the data model exists in CDF Data Modeling.
