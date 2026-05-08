# Infield Location

This module configures an Infield location backed by CDM/IDM extension views. It is based on the Oslo location extension example and deploys:

- Location-specific instance spaces for assets, maintenance, time series, files, 3D, app data, and app configuration.
- An `InFieldCDMLocationConfig` instance, which is the location config for the InField application.
- A matching `LocationFilter`, so the same data can be displayed in an equivalent location in Fusion.
- Infield read-only, normal user, template admin, and checklist admin groups.

## Configuration

Update the module variables before deploying:

```yaml
variables:
  modules:
    location:
      location: oslo_refinery
      location_name: Oslo Refinery
      location_description: Oslo Refinery demo
      idm_extension_space: customer_idm_extention
      idm_extension_data_model_external_id: ExtensionDataModel
      idm_extension_data_model_version: v1
      infield_location_read_only_users_source_id: <idp-group-source-id>
      infield_location_normal_users_source_id: <idp-group-source-id>
      infield_location_template_admin_users_source_id: <idp-group-source-id>
      infield_location_checklist_admin_users_source_id: <idp-group-source-id>
```

The configured extension data model must expose views that represent assets, maintenance orders, operations, notifications, and files. By default the module expects `ExtendedAsset`, `ExtendedMaintenanceOrder`, `ExtendedOperation`, `ExtendedNotification`, and `ExtendedFile` in `{{idm_extension_space}}`.

## Multiple Locations

To configure more than one Infield location, copy this `location` module folder once per location and give each copy a unique folder name, for example `location_oslo` and `location_phoenix`. Then update each copy's `default.config.yaml` or environment variables with the location-specific values.

If you add those copies back to the library package, also add each copied module path to `modules/packages.toml` and make the `id` in each copied `module.toml` unique.

## Deploy

```bash
cdf modules add dp:infield
cdf deploy
```
