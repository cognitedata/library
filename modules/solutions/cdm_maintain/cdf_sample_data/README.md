# Maintain Sample Data (sample assets, activities, and demo file)

Library path: `modules/solutions/cdm_maintain/cdf_sample_data/` · Module ID: `dp:cdm_maintain:cdf_sample_data` · Deployment pack: `dp:cdm_maintain`

## Overview

This optional module loads a small data set so you can check that Maintain works before you connect a real source
system. It writes to the same spaces as real data, so leave it out of production deployments.

## Module Components

- `assets.Node.yaml`: the root asset `{{location}}_root` and one child equipment asset, `abc_12345`.
- `activities.Node.yaml`: two maintenance activities on `abc_12345`, one preventive and one corrective, linked to the
  demo file and a grid zone.
- `gridZoneAnnotations.Node.yaml`: four grid zones on the demo file, one per quadrant, in the app data space.
- `upload_data/`: `DEMO-FILE.pdf` and its manifest. Upload it with `cdf data upload`; it becomes a `CogniteFile`
  linked to the root asset and `abc_12345`.

## Variables

| Variable | Default | Description |
|---|---|---|
| `instanceSpace` | `maintain_source_data` | Space for the assets, activities, and file. Must match `sourceDataSpace` in `cdf_maintain_location` |
| `appDataSpace` | `maintain_solution_model` | Space for the grid zones. Must match `appDataSpace` in `cdf_maintain_location` |
| `location` | *(required)* | Must match `location` in `cdf_maintain_location` |
| `location_name` | *(required)* | Must match `location_name` in `cdf_maintain_location` |

## Deployment

Not selected by default. Add it with the pack, then deploy and upload the file:

```bash
cdf deploy --env=dev
```

```bash
cdf data upload --env=dev
```

`cdf data upload` needs the `data` plugin enabled in `cdf.toml`. `cdf build` warns that `upload_data` is a
non-resource directory. That is expected. See the [pack README](../README.md#deployment) for the full steps.

## Module Structure

```
cdf_sample_data/
├── module.toml
├── default.config.yaml
├── data_modeling/
│   └── nodes/
│       ├── assets.Node.yaml
│       ├── activities.Node.yaml
│       └── gridZoneAnnotations.Node.yaml
└── upload_data/
    ├── DEMO-FILE.Manifest.yaml
    └── files/DEMO-FILE.pdf
```

## Support

See [Support](../README.md#support) in the pack README.
