# Maintain Location (location filter and APM config node)

Library path: `modules/solutions/cdm_maintain/cdf_maintain_location/` · Module ID: `dp:cdm_maintain:cdf_maintain_location` · Deployment pack: `dp:cdm_maintain`

## Overview

This module connects Maintain to your data for one location. It deploys a `LocationFilter` over the Maintain and
MaintainSource data models, and the APM config node that tells Maintain which spaces to read, how to show each field,
which features are on, and which root asset defines the location.

## Module Components

- `locations/root.LocationFilter.yaml`: a `LocationFilter` with externalId `{{location}}`, covering the `Maintain` and
  `MaintainSource` data models and their spaces.
- `data_modeling/nodes/apm_config_node.Node.yaml`: the `default-maintain-config` node in `APM_Config`, with:
  - `fieldConfiguration`: labels, icons, and Gantt and sort settings for activity fields.
  - `featureConfiguration`: custom layouts, canvas, and 2D grid zones.
  - `rootLocationConfigurations`: the location's display name, its root asset `{{location}}_root`, the data filters,
    and the app and source data instance spaces.

The node externalId is fixed, so deploying the module again for a second location overwrites the first. To add
locations, add entries to `rootLocationConfigurations` in the node instead. See
[Configure locations](https://docs.cognite.com/cdf/maintain/guides/config#configure-locations).

## Variables

| Variable | Default | Description |
|---|---|---|
| `location` | *(required)* | `LocationFilter` externalId and root asset prefix (`<location>_root`). Lowercase, no spaces |
| `location_name` | *(required)* | Location name shown in Maintain |
| `appDataSpace` | `maintain_solution_model` | Must match `schemaSpace` in `cdf_maintain_solution_model` |
| `sourceDataSpace` | `maintain_source_data` | Must match `schemaSpace` in `cdf_maintain_source_data_model` |

## Deployment

Deployed as part of the pack. See the [pack README](../README.md#deployment) for prerequisites and steps. The root
asset `<location>_root` must exist in the source data space, either from your ingestion or from
[`cdf_sample_data`](../cdf_sample_data/README.md).

## Module Structure

```
cdf_maintain_location/
├── module.toml
├── default.config.yaml
├── data_modeling/
│   └── nodes/apm_config_node.Node.yaml
└── locations/
    └── root.LocationFilter.yaml
```

## Support

See [Support](../README.md#support) in the pack README.
