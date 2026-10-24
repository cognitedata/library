# CDM Infield

Library path: `modules/solutions/cdf_infield/` · Deployment pack: `dp:infield`

This deployment pack configures **Infield on CDM** for a site or plant location: instance spaces, application config, Fusion location filter, and IdP-backed access groups.

## Package layout

```
cdf_infield/
├── README.md
└── cdf_infield_location/          # Per-location Infield setup (copy to add more sites)
    ├── module.toml
    ├── default.config.yaml
    ├── README.md
    ├── auth/                      # IdP-linked CDF groups
    ├── cdf_applications/          # InFieldCDMLocationConfig
    ├── data_modeling/             # Instance spaces
    └── locations/                 # Fusion LocationFilter (optional)
```

Naming follows the same pattern as [`cdm_maintain`](../cdm_maintain/): solution folder `cdf_infield`, module folder `cdf_infield_<purpose>` (here `cdf_infield_location`), and resource files prefixed with the module slug (for example `infield_location.LocationFilter.yaml`).

## Multiple locations

Copy `cdf_infield_location` once per site (for example `cdf_infield_location_oslo`), give each copy a unique `id` in `module.toml`, and register the path in `modules/packages.toml`. See [cdf_infield_location/README.md](./cdf_infield_location/README.md) for IdP variables and deploy steps.

## Deploy

```bash
cdf modules add dp:infield
cdf deploy
```
