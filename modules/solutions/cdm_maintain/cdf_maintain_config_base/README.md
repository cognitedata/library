# Maintain Config Base (APM_Config data model)

Library path: `modules/solutions/cdm_maintain/cdf_maintain_config_base/` · Module ID: `dp:cdm_maintain:cdf_maintain_config_base` · Deployment pack: `dp:cdm_maintain`

## Overview

Maintain reads its configuration from a node in the `APM_Config` space. This module deploys that space and the
`APM_Config` container, view, and data model, so that [`cdf_maintain_location`](../cdf_maintain_location/README.md)
can write the configuration node.

The names are fixed: Maintain looks for `APM_Config` exactly, so the module has no variables. InField uses the same
space. In a project that already runs InField, review the dry run before deploying, and do not run `cdf clean` on this
module.

## Module Components

- `APM_Config` space
- `APM_Config` container and view (version `1`)
- `APM_Config` data model (version `1`)

## Deployment

Deployed as part of the pack. See the [pack README](../README.md#deployment) for prerequisites and steps.

## Module Structure

```
cdf_maintain_config_base/
├── module.toml
├── default.config.yaml                # No variables
└── data_modeling/
    ├── APM_Config.Space.yaml
    ├── APM_Config.DataModel.yaml
    ├── containers/APM_Config.Container.yaml
    └── views/APM_Config.View.yaml
```

## Support

See [Support](../README.md#support) in the pack README.
