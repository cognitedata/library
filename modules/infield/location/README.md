# Infield Location

Library path: `modules/infield/location/` · Deployment pack: `dp:infield`

This module configures an Infield location backed by CDM/IDM extension views. It is based on the Springfield example and deploys:

- Location-specific instance spaces for assets, maintenance, time series, files, 3D, app data, and app configuration.
- An `InFieldCDMLocationConfig` instance, which is the location config for the InField application.
- A matching `LocationFilter`, so the same data can be displayed in an equivalent location in Fusion.
- Infield read-only, normal user, template admin, and checklist admin groups.

## Configuration

### Environment variables (IdP groups)

`default.config.yaml` reads the four Infield group **source IDs** from the environment using Cognite Toolkit `${VAR}` substitution (same pattern as `cdf_ingestion`, SharePoint, and other modules in this library).

Add the following to your **project** `.env` file (next to `cdf.toml`), or export them in your shell or CI job, **before** running `cdf deploy`:

| Variable | Purpose |
| -------- | ------- |
| `INFIELD_LOCATION_READ_ONLY_USERS_SOURCE_ID` | Source ID for the read-only Infield users group |
| `INFIELD_LOCATION_NORMAL_USERS_SOURCE_ID` | Source ID for the normal Infield users group |
| `INFIELD_LOCATION_TEMPLATE_ADMIN_USERS_SOURCE_ID` | Source ID for the template admin group |
| `INFIELD_LOCATION_CHECKLIST_ADMIN_USERS_SOURCE_ID` | Source ID for the checklist admin group |

Example `.env` entries (replace with your identity provider’s source IDs for each group):

```bash
INFIELD_LOCATION_READ_ONLY_USERS_SOURCE_ID=
INFIELD_LOCATION_NORMAL_USERS_SOURCE_ID=
INFIELD_LOCATION_TEMPLATE_ADMIN_USERS_SOURCE_ID=
INFIELD_LOCATION_CHECKLIST_ADMIN_USERS_SOURCE_ID=
```

You can instead set the same module variable keys under `variables.modules` in your project’s `config.<env>.yaml` (use the module key your toolkit project uses for this module, often `location`):

```yaml
variables:
  modules:
    location:
      infield_location_read_only_users_source_id: "<idp-source-id>"
      infield_location_normal_users_source_id: "<idp-source-id>"
      infield_location_template_admin_users_source_id: "<idp-source-id>"
      infield_location_checklist_admin_users_source_id: "<idp-source-id>"
```

The configured extension data model must expose views that represent assets, maintenance orders, operations, notifications, and files. By default the module expects `ExtendedAsset`, `ExtendedMaintenanceOrder`, `ExtendedOperation`, `ExtendedNotification`, and `ExtendedFile` in `{{idm_extension_space}}`.

## Multiple Locations

To configure more than one Infield location, copy this `location` module folder once per location and give each copy a unique folder name, for example `location_oslo` and `location_phoenix`. Then give each copy its own IdP group IDs via a separate `.env` (or `config.<env>.yaml` overrides), or point each copy’s `default.config.yaml` at distinct environment variable names.

If you add those copies back to the library package, also add each copied module path to `modules/packages.toml` and make the `id` in each copied `module.toml` unique.

## Deploy

```bash
cdf modules add dp:infield
cdf deploy
```
