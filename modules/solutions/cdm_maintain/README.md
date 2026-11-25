# CDM Maintain (Maintain on Cognite Data Model)

Library path: `modules/solutions/cdm_maintain/` · Deployment pack: `dp:cdm_maintain`

## Overview

[Cognite Maintain](https://docs.cognite.com/cdf/maintain) helps maintenance planners plan shutdowns, turnarounds, and
campaigns with all the related data in one place. They can bundle work into opportunity scopes so equipment is isolated
less often and less production is lost, track progress against the plan, and analyze maintenance trends over time.

This deployment pack shortens the path to that value. One Toolkit deployment sets up everything Maintain needs in a CDF
project, so your team can spend its time loading work orders and planning work, not building data models.

**Why use it:**

- **Faster time to value.** Deploy the data models, the `APM_Config` configuration node, and a location in one run,
  instead of building spaces, containers, views, and a long config node by hand.
- **Fewer failed rollouts.** A wrong space reference or a missing config field can make Maintain show no data, with no
  error. The pack comes pre-wired and tested, so you avoid those mistakes.
- **See it working before you integrate.** Optional sample data shows Maintain working end to end, so stakeholders can
  see the result before you connect a source system such as SAP.
- **Built on CDM and IDM.** Maintenance orders, operations, and assets use the standard CDM and IDM views, so the data
  also works with other Cognite applications.
- **Repeatable.** The same configuration deploys to every environment and every project.

**Who it is for:** delivery engineers, data modelers, and CDF admins setting up Maintain on a new project, or moving
from a custom APM data model to the CDM-aligned Maintain model.

**Do not use it** if you already run a custom Maintain data model in production and cannot migrate spaces. The pack
deploys its own space structure.

## Module Components

| Module | What it deploys | Selected by default |
|---|---|---|
| [`cdf_maintain_config_base`](cdf_maintain_config_base/README.md) | The `APM_Config` space, container, view, and data model that Maintain reads its configuration from | Yes |
| [`cdf_maintain_solution_model`](cdf_maintain_solution_model/README.md) | The `Maintain` data model: `Plan`, `MaintainLayout`, `MaintainCanvas`, `MaintainWorkOrder`, `ManualMappings3D`, `FileGridZoneAnnotation`, `APM_Comment`, `APM_Optimisation`, plus global nodes | Yes |
| [`cdf_maintain_source_data_model`](cdf_maintain_source_data_model/README.md) | The `MaintainSource` data model: the `SourceMaintenanceOrder` container and view, plus the CDM/IDM views Maintain reads | Yes |
| [`cdf_maintain_location`](cdf_maintain_location/README.md) | A `LocationFilter` and the `default-maintain-config` APM config node that connects the location to the source and solution spaces | Yes |
| [`cdf_sample_data`](cdf_sample_data/README.md) | A root asset, one equipment asset, two maintenance activities, grid zone annotations, and a demo PDF | No |

## Deployment

### Prerequisites

- Cognite Toolkit 0.7.0 or later, which reads this library by default. On older versions, see the
  [Toolkit documentation](https://docs.cognite.com/cdf/deploy/cdf_toolkit/) to enable external libraries.
- A Toolkit project with a `cdf.toml`, authenticated to your CDF project (`cdf auth init`, then `cdf auth verify`).
- Access to create spaces, containers, views, data models, nodes, and location filters. For the sample data, also
  access to create files.
- For the sample PDF only: the `data` plugin enabled in `cdf.toml`.

  ```toml
  [plugins]
  data = true
  ```

### Add to an existing Toolkit project

```bash
cdf modules add dp:cdm_maintain
```

The modules are placed under `modules/solutions/cdm_maintain/`, and their variables are added to your
`config.<env>.yaml`.

### Start from scratch

```bash
cdf modules init .
```

> **Warning:** `cdf modules init` overwrites existing modules in the current directory. Run it in a new directory, or
> commit your changes first.

Select **CDM Maintain** from the list, or run `cdf modules add dp:cdm_maintain` afterwards.

### Configure

Set the variables in `config.<env>.yaml`. `location` and `location_name` are required. The space names have defaults,
but the cross-module values must match each other, as noted in the comments.

```yaml
variables:
  modules:
    cdf_maintain_solution_model:
      schemaSpace: maintain_solution_model
      sourceDataSpace: maintain_source_data   # = cdf_maintain_source_data_model.schemaSpace
    cdf_maintain_source_data_model:
      schemaSpace: maintain_source_data
    cdf_maintain_location:
      location: <your_location_id>            # required, lowercase, no spaces, e.g. oslo_refinery
      location_name: <Your Location Name>     # required, shown in Maintain
      appDataSpace: maintain_solution_model   # = cdf_maintain_solution_model.schemaSpace
      sourceDataSpace: maintain_source_data   # = cdf_maintain_source_data_model.schemaSpace
    cdf_sample_data:                          # only if you deploy the sample data
      instanceSpace: maintain_source_data     # = cdf_maintain_location.sourceDataSpace
      appDataSpace: maintain_solution_model   # = cdf_maintain_location.appDataSpace
      location: <your_location_id>            # = cdf_maintain_location.location
      location_name: <Your Location Name>     # = cdf_maintain_location.location_name
```

`location` is the `LocationFilter` externalId and the prefix of the root asset, `<location>_root`.

### Build and deploy

```bash
cdf build --env=dev
```

```bash
cdf deploy --dry-run --env=dev
```

```bash
cdf deploy --env=dev
```

If you deployed `cdf_sample_data`, upload the demo PDF:

```bash
cdf data upload --env=dev
```

`cdf build` warns that `cdf_sample_data` has a non-resource directory, `upload_data`. That is expected: the folder holds
the manifest for `cdf data upload`, not for `cdf deploy`.

### Verify

1. In **Data management → Data models**, the spaces `APM_Config`, `maintain_solution_model`, and
   `maintain_source_data` (or your values) exist.
2. In **Data management → Locations**, a location with the externalId from `location` exists.
3. In the `APM_Config` view, there is one node, `default-maintain-config`, with your spaces and location.
4. If you deployed the sample data, the source data space holds the `CogniteAsset` nodes `<location>_root` and
   `abc_12345`.
5. Open Maintain and check that the location appears in the location selector.

## Known limitations

- **One location per deployment.** The APM config node has a fixed externalId, `default-maintain-config`, so deploying
  the module again for a second location overwrites the first. To add locations, add entries to
  `rootLocationConfigurations` in the node instead. See
  [Configure locations](https://docs.cognite.com/cdf/maintain/guides/config#configure-locations).
- **Root asset naming.** The location's root asset must have the externalId `<location>_root` in the source data
  space.
- **Shared `APM_Config` space.** InField also uses `APM_Config`. In a project that already runs InField, review the
  dry run before deploying, and do not run `cdf clean` on `cdf_maintain_config_base`.
- **Instances share the schema spaces.** By default, data instances are written to the same spaces as the data models.
- **Sample data uses the real spaces.** `cdf_sample_data` writes to the same spaces as your real data. Leave it out of
  production deployments.
- **Fixed identifiers.** Maintain expects the view and container externalIds, the `v1` versions, and the `APM_Config`
  names as shipped. Do not rename them.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `UnresolvedVariableWarning` during `cdf build` | A variable is missing from `config.<env>.yaml` | Add it under the module key shown in [Configure](#configure) |
| Location does not appear in Maintain | `location` differs between modules, or `<location>_root` does not exist | Use the same `location` in `cdf_maintain_location` and `cdf_sample_data`, and check the root asset exists in the source data space |
| Maintain shows no data | Instances are in a different space from the one the location points to | Make `cdf_sample_data.instanceSpace` (or your ingestion target) match `cdf_maintain_location.sourceDataSpace` |
| Asset tab in the activity inspector is empty | The activity's `mainAsset` is empty, or points to an asset outside the source data space | See below |

### Asset tab in the activity inspector is empty

The Asset tab opens the asset referenced by the activity's `mainAsset` property (from `CogniteMaintenanceOrder`).
Check that:

- `mainAsset` is populated on the activity. Populating only `assets` is not enough.
- The referenced node has data in the `CogniteAsset` view (`cdf_cdm`).
- The asset lives in the instance space configured as `sourceDataInstanceSpace` in `rootLocationConfigurations`
  (defaults to `sourceDataSpace`, `maintain_source_data`). Assets ingested into a different instance space are not
  found.

## Module Structure

```
cdm_maintain/
├── README.md                          # This file
├── cdf_maintain_config_base/          # APM_Config space, container, view, data model
├── cdf_maintain_solution_model/       # Maintain data model and global nodes
├── cdf_maintain_source_data_model/    # MaintainSource data model
├── cdf_maintain_location/             # LocationFilter and APM config node
└── cdf_sample_data/                   # Optional sample assets, activities, grid zones, demo PDF
```

Each module folder has its own `README.md` with its files and variables.

## Support

- Maintain questions: Slack **#topic-maintain**
- Deployment pack questions: Slack **#topic-deployment-packs**
- [Cognite Toolkit documentation](https://docs.cognite.com/cdf/deploy/cdf_toolkit/)
