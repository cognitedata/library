# Infield Location

Library path: `modules/solutions/cdf_infield/cdf_infield_location/` · Module ID: `dp:infield:cdf_infield_location` · Deployment pack: `dp:infield`

This module configures an Infield location backed by CDM/IDM extension views. It is based on the Springfield example and deploys:

- Location-specific instance spaces for assets, maintenance, time series, files, 3D, app data, and app configuration.
- An `InFieldCDMLocationConfig` instance (`cdf_applications/infield_location.InFieldCDMLocationConfig.yaml`).
- A matching `LocationFilter` (`locations/infield_location.LocationFilter.yaml`) so the same data can be displayed in an equivalent location in Fusion.
- Infield read-only, normal user, template admin, and checklist admin groups (`auth/infield_location_*_role.group.yaml`).

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

You can instead set the same module variable keys under `variables.modules` in your project’s `config.<env>.yaml` (use the module key your toolkit project uses for this module, often `cdf_infield_location`):

```yaml
variables:
  modules:
    cdf_infield_location:
      infield_location_read_only_users_source_id: "<idp-source-id>"
      infield_location_normal_users_source_id: "<idp-source-id>"
      infield_location_template_admin_users_source_id: "<idp-source-id>"
      infield_location_checklist_admin_users_source_id: "<idp-source-id>"
```

The configured extension data model must expose views that represent assets, maintenance orders, operations, notifications, and files. By default the module expects `ExtendedAsset`, `ExtendedMaintenanceOrder`, `ExtendedOperation`, `ExtendedNotification`, and `ExtendedFile` in `{{idm_extension_space}}`.

## Multiple locations

To configure more than one Infield location, copy this `cdf_infield_location` folder once per location and give each copy a unique folder name, for example `cdf_infield_location_oslo` and `cdf_infield_location_phoenix`. Then give each copy its own IdP group IDs via a separate `.env` (or `config.<env>.yaml` overrides), or point each copy’s `default.config.yaml` at distinct environment variable names.

If you add those copies back to the library package, also add each copied module path to `modules/packages.toml` and make the `id` in each copied `module.toml` unique (for example `dp:infield:cdf_infield_location_oslo`).

## Deploy

### Prerequisites

- **Cognite Toolkit 0.7.210 or above** (`cdf --version` to check).
- A CDF project with valid authentication configured for your target environment.
- A `cdf.toml` in your Toolkit project directory.

### Choose your setup path

### 1. Existing Toolkit project

If you already have a Toolkit project, ensure your `cdf.toml` uses the official library URL:

```toml
[library.cognite]
url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
```

In the same `cdf.toml`, ensure deployment packs are enabled:

```toml
[alpha_flags]
deployment-pack = true
```

Then add this module:

```bash
cdf modules add -d cdf_infield_location
```

Build and deploy:

```bash
cdf build
cdf deploy --dry-run
cdf deploy
```

### 2. Starting from scratch

In an empty directory:

```bash
cdf modules init .
```

In the interactive selector:

1. Choose **Infield**.
2. Use **Space** to select **cdf_infield_location**.
3. Press **Enter**.

Then run:

```bash
cdf build
cdf deploy --dry-run
cdf deploy
```
